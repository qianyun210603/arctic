import logging
from datetime import datetime as dt

import bson
import pandas as pd
import pymongo

from .bson_store import BSONStore
from .._util import indent
from ..decorators import mongo_retry
from ..exceptions import NoDataFoundException

logger = logging.getLogger(__name__)

METADATA_STORE_TYPE = 'MetadataStore'


class MetadataStore(BSONStore):
    """
    Metadata Store. This stores metadata with timestamps to allow temporal queries.

    Entries are stored in the following format:
        'symbol': symbol name
        'metadata': metadata to be persisted
        'start_time': when entry becomes effective
        'end_time': (Optional) when entry expires. If not set, it is still in effect

    For each symbol end_time of a entry should match start_time of the next one except for the current entry.
    """

    @classmethod
    def initialize_library(cls, arctic_lib, hashed=True, **kwargs):
        MetadataStore(arctic_lib)._ensure_index()
        BSONStore.initialize_library(arctic_lib, hashed, **kwargs)

    @mongo_retry
    def _ensure_index(self):
        self.create_index([('symbol', pymongo.ASCENDING), ('start_time', pymongo.DESCENDING)],
                          unique=True, background=True)

    def __init__(self, arctic_lib):
        self._arctic_lib = arctic_lib
        self._reset()

    def _reset(self):
        self._collection = self._arctic_lib.get_top_level_collection().metadata

    def __getstate__(self):
        return {'arctic_lib': self._arctic_lib}

    def __setstate__(self, state):
        return MetadataStore.__init__(self, state['arctic_lib'])

    def __str__(self):
        return """<%s at %s>\n%s""" % (self.__class__.__name__, hex(id(self)), indent(str(self._arctic_lib), 4))

    def __repr__(self):
        return str(self)

    @mongo_retry
    def list_symbols(self, regex=None, as_of=None, **kwargs):
        """
         Return the symbols in this library.

         Parameters
         ----------
         as_of : `datetime.datetime`
            filter symbols valid at given time
         regex : `str`
             filter symbols by the passed in regular expression
         kwargs :
             kwarg keys are used as fields to query for symbols with metadata matching
             the kwargs query

         Returns
         -------
         String list of symbols in the library
        """

        # Skip aggregation pipeline
        if not (regex or as_of or kwargs):
            return self.distinct('symbol')

        # Index-based query part
        index_query = {}
        if as_of is not None:
            index_query['start_time'] = {'$lte': as_of}

        if regex or as_of:
            # make sure that symbol is present in query even if only as_of is specified to avoid document scans
            # see 'Pipeline Operators and Indexes' at
            # https://docs.mongodb.com/manual/core/aggregation-pipeline/#aggregation-pipeline-operators-and-performance
            index_query['symbol'] = {'$regex': regex or '^'}

        # Document query part
        data_query = {}
        if kwargs:
            for k, v in kwargs.items():
                data_query['metadata.' + k] = v

        # Sort using index, relying on https://docs.mongodb.com/manual/core/aggregation-pipeline-optimization/
        pipeline = [{'$sort': {'symbol': pymongo.ASCENDING,
                               'start_time': pymongo.DESCENDING}}]

        # Index-based filter on symbol and start_time
        if index_query:
            pipeline.append({'$match': index_query})
        # Group by 'symbol' and get the latest known data
        pipeline.append({'$group': {'_id': '$symbol',
                                    'metadata': {'$first': '$metadata'}}})
        # Match the data fields
        if data_query:
            pipeline.append({'$match': data_query})
        # Return only 'symbol' field value
        pipeline.append({'$project': {'_id': 0, 'symbol': '$_id'}})

        return sorted(r['symbol'] for r in self.aggregate(pipeline))

    @mongo_retry
    def has_symbol(self, symbol):
        return self.find_one({'symbol': symbol}) is not None

    @mongo_retry
    def read_history(self, symbol):
        """
        Return all metadata saved for `symbol`

        Parameters
        ----------
        symbol : `str`
            symbol name for the item

        Returns
        -------
        pandas.DateFrame containing timestamps and metadata entries
        """
        find = self.find({'symbol': symbol}, sort=[('start_time', pymongo.ASCENDING)])
        times = []
        entries = []
        for item in find:
            times.append(item['start_time'])
            entries.append(item['metadata'])
        return pd.DataFrame({symbol: entries}, times)

    @mongo_retry
    def read(self, symbol, as_of=None):
        """
        Return current metadata saved for `symbol`

        Parameters
        ----------
        symbol : `str`
            symbol name for the item
        as_of : `datetime.datetime`
            return entry valid at given time

        Returns
        -------
        metadata
        """
        if as_of is not None:
            res = self.find_one({'symbol': symbol, 'start_time': {'$lte': as_of}},
                                sort=[('start_time', pymongo.DESCENDING)])
        else:
            res = self.find_one({'symbol': symbol}, sort=[('start_time', pymongo.DESCENDING)])
        return res['metadata'] if res is not None else None

    @mongo_retry
    def get_last_update_time(self, symbol, as_of=None):
        """
        Return last update time of current metadata saved for `symbol`

        Parameters
        ----------
        symbol : `str`
            symbol name for the item
        as_of : `datetime.datetime`
            return entry valid at given time

        Returns
        -------
        `datetime.datetime`
        """
        if as_of is not None:
            res = self.find_one({'symbol': symbol, 'start_time': {'$lte': as_of}},
                                sort=[('start_time', pymongo.DESCENDING)])
        else:
            res = self.find_one({'symbol': symbol}, sort=[('start_time', pymongo.DESCENDING)])
        return res.get('last_update', res['start_time']) if res is not None else None

    def write_history(self, collection, last_update=None):
        """
        Manually overwrite entire metadata history for symbols in `collection`

        Parameters
        ----------
        collection : `list of pandas.DataFrame`
            with symbol names as headers and timestamps as indices
            (the same format as output of read_history)
            Example:
                [pandas.DataFrame({'symbol': [{}]}, [datetime.datetime.utcnow()])]
        """
        if last_update is None:
            last_update = dt.utcnow()
        documents = []
        for dataframe in collection:
            if len(dataframe.columns) != 1:
                raise ValueError('More than one symbol found in a DataFrame')
            symbol = dataframe.columns[0]
            times = dataframe.index
            entries = dataframe[symbol].values
            if self.has_symbol(symbol):
                self.purge(symbol)
            doc = {'symbol': symbol, 'metadata': entries[0], 'start_time': times[0], 'last_update': last_update}
            for metadata, start_time in zip(entries[1:], times[1:]):
                if metadata == doc['metadata']:
                    continue
                doc['end_time'] = start_time
                documents.append(doc)
                doc = {'symbol': symbol, 'metadata': metadata, 'start_time': start_time, 'last_update': last_update}
            documents.append(doc)

        self.insert_many(documents)

    def append(self, symbol, metadata, start_time=None, last_update=None):
        """
        Update metadata entry for `symbol`

        Parameters
        ----------
        symbol : `str`
            symbol name for the item
        metadata : `dict`
            to be persisted
        start_time : `datetime.datetime`
            when metadata becomes effective
            Default: datetime.datetime.utcnow()
        """
        if start_time is None:
            start_time = dt.utcnow()
        if last_update is None:
            last_update = dt.utcnow()
        old_metadata = self.find_one({'symbol': symbol}, sort=[('start_time', pymongo.DESCENDING)])
        if old_metadata is not None:
            if old_metadata['metadata'] == metadata:
                old_metadata['last_update'] = last_update
                self.find_one_and_update({'symbol': symbol, 'start_time': old_metadata['start_time']},
                                         {'$set': {'last_update': last_update}},
                                         sort=[('start_time', pymongo.DESCENDING)])
                return old_metadata

            if old_metadata['start_time'] >= start_time:
                raise ValueError('start_time={} is earlier than the last metadata @{}'.format(start_time,
                                                                                              old_metadata['start_time']))
        elif metadata is None:
            return

        self.find_one_and_update({'symbol': symbol}, {'$set': {'end_time': start_time}},
                                 sort=[('start_time', pymongo.DESCENDING)])
        document = {'_id': bson.ObjectId(), 'symbol': symbol, 'metadata': metadata, 'start_time': start_time, 'last_update': last_update}
        mongo_retry(self.insert_one)(document)

        logger.debug('Finished writing metadata for %s', symbol)
        return document

    def prepend(self, symbol, metadata, start_time=None, last_update=None):
        """
        Prepend a metadata entry for `symbol`

        Parameters
        ----------
        symbol : `str`
            symbol name for the item
        metadata : `dict`
            to be persisted
        start_time : `datetime.datetime`
            when metadata becomes effective
            Default: datetime.datetime.min
        """
        if metadata is None:
            return
        if start_time is None:
            start_time = dt.min
        if last_update is None:
            last_update = dt.utcnow()
        old_metadata = self.find_one({'symbol': symbol}, sort=[('start_time', pymongo.ASCENDING)])
        if old_metadata is not None:
            if old_metadata['start_time'] <= start_time:
                raise ValueError('start_time={} is later than the first metadata @{}'.format(start_time,
                                                                                             old_metadata['start_time']))
            if old_metadata['metadata'] == metadata:
                self.find_one_and_update({'symbol': symbol}, {'$set': {'start_time': start_time, "last_update": last_update}},
                                         sort=[('start_time', pymongo.ASCENDING)])
                old_metadata['start_time'] = start_time
                old_metadata['last_update'] = last_update
                return old_metadata
            end_time = old_metadata.get('start_time')
        else:
            end_time = None

        document = {'_id': bson.ObjectId(), 'symbol': symbol, 'metadata': metadata, 'start_time': start_time, "last_update": last_update}
        if end_time is not None:
            document['end_time'] = end_time
        mongo_retry(self.insert_one)(document)

        logger.debug('Finished writing metadata for %s', symbol)
        return document

    def pop(self, symbol):
        """
        Delete current metadata of `symbol`

        Parameters
        ----------
        symbol : `str`
            symbol name to delete

        Returns
        -------
        Deleted metadata
        """
        last_metadata = self.find_one({'symbol': symbol}, sort=[('start_time', pymongo.DESCENDING)])
        if last_metadata is None:
            raise NoDataFoundException('No metadata found for symbol {}'.format(symbol))

        self.find_one_and_delete({'symbol': symbol}, sort=[('start_time', pymongo.DESCENDING)])
        mongo_retry(self.find_one_and_update)({'symbol': symbol}, {'$unset': {'end_time': ''}},
                                              sort=[('start_time', pymongo.DESCENDING)])

        return last_metadata

    @mongo_retry
    def purge(self, symbol):
        """
        Delete all metadata of `symbol`

        Parameters
        ----------
        symbol : `str`
            symbol name to delete
        """
        logger.warning("Deleting entire metadata history for %r from %r" % (symbol, self._arctic_lib.get_name()))
        self.delete_many({'symbol': symbol})

    def amend(self, symbol, new_metadata, start_time=None, last_update=None):
        if last_update is None:
            last_update = dt.utcnow()
        set_dict = {"metadata": new_metadata, 'last_update': last_update}
        if start_time is not None:
            return self.find_one_and_update({'symbol': symbol, 'start_time': start_time}, {"$set": set_dict})
        else:
            return self.find_one_and_update({'symbol': symbol}, {"$set": set_dict}, sort=[('start_time', pymongo.DESCENDING)])

    def rename(self, old_symbol, new_symbol):
        """
        Rename a symbol in the metadata store

        Parameters
        ----------
        old_symbol : `str`
            symbol name to rename
        new_symbol : `str`
            new symbol name
        """
        self.update_many({'symbol': old_symbol}, {'$set': {'symbol': new_symbol}})
