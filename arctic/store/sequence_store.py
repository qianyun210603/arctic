import logging
from datetime import datetime as dt

import bson
import pymongo

from .bson_store import BSONStore
from .._util import indent
from ..decorators import mongo_retry

logger = logging.getLogger(__name__)

SEQUENCE_STORE_TYPE = "SequenceStore"


class SequenceStore(BSONStore):
    """
    Sequence Store. This stores metadata with timestamps to allow temporal queries.

    Entries are stored in the following format:
        'symbol': symbol name
        'metadata': metadata to be persisted

    """

    @classmethod
    def initialize_library(cls, arctic_lib, hashed=True, **kwargs):
        SequenceStore(arctic_lib)._ensure_index()
        BSONStore.initialize_library(arctic_lib, hashed, **kwargs)

    @mongo_retry
    def _ensure_index(self):
        self.create_index([("symbol", pymongo.ASCENDING)], unique=True, background=True)

    def _reset(self):
        self._collection = self._arctic_lib.get_top_level_collection().metadata

    def __getstate__(self):
        return {"arctic_lib": self._arctic_lib}

    def __setstate__(self, state):
        return SequenceStore.__init__(self, state["arctic_lib"])

    def __str__(self):
        return """<%s at %s>\n%s""" % (self.__class__.__name__, hex(id(self)), indent(str(self._arctic_lib), 4))

    def __repr__(self):
        return str(self)

    @mongo_retry
    def list_symbols(self, regex=None, **kwargs):
        """
        Return the symbols in this library.

        Parameters
        ----------
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
        if not (regex or kwargs):
            return self.distinct("symbol")

        # Index-based query part
        index_query = {}

        if regex:
            # make sure that symbol is present in query even if only as_of is specified to avoid document scans
            # see 'Pipeline Operators and Indexes' at
            # https://docs.mongodb.com/manual/core/aggregation-pipeline/#aggregation-pipeline-operators-and-performance
            index_query["symbol"] = {"$regex": regex or "^"}

        # Document query part
        data_query = {}
        if kwargs:
            for k, v in kwargs.items():
                data_query["metadata." + k] = v

        # Sort using index, relying on https://docs.mongodb.com/manual/core/aggregation-pipeline-optimization/
        pipeline = [{"$sort": {"symbol": pymongo.ASCENDING}}]

        # Index-based filter on symbol and start_time
        if index_query:
            pipeline.append({"$match": index_query})
        # Group by 'symbol' and get the latest known data
        pipeline.append({"$group": {"_id": "$symbol", "metadata": {"$first": "$metadata"}}})
        # Match the data fields
        if data_query:
            pipeline.append({"$match": data_query})
        # Return only 'symbol' field value
        pipeline.append({"$project": {"_id": 0, "symbol": "$_id"}})

        return sorted(r["symbol"] for r in self.aggregate(pipeline))

    @mongo_retry
    def has_symbol(self, symbol):
        return self.find_one({"symbol": symbol}) is not None

    @mongo_retry
    def read(self, symbol):
        """
        Return current metadata saved for `symbol`

        Parameters
        ----------
        symbol : `str`
            symbol name for the item

        Returns
        -------
        metadata
        """
        res = self.find_one({"symbol": symbol})
        return res["metadata"] if res is not None else None

    @mongo_retry
    def get_last_update_time(self, symbol):
        """
        Return last update time of current metadata saved for `symbol`

        Parameters
        ----------
        symbol : `str`
            symbol name for the item

        Returns
        -------
        `datetime.datetime`
        """
        res = self.find_one({"symbol": symbol})
        return res.get("last_update") if res is not None else None

    def write(self, symbol, metadata, last_update=None, overwrite=True):
        """
        Update metadata entry for `symbol`

        Parameters
        ----------
        symbol : `str`
            symbol name for the item
        metadata : `dict`
            to be persisted
        last_update : `datetime.datetime`
            last update time
        overwrite: `bool`
            whether to overwrite existing metadata
        """
        if last_update is None:
            last_update = dt.utcnow()
        old_metadata = self.find_one({"symbol": symbol})
        if old_metadata is not None:
            if old_metadata["metadata"] == metadata:
                old_metadata["last_update"] = last_update
                self.find_one_and_update({"symbol": symbol}, {"$set": {"last_update": last_update}})
                return old_metadata
            elif overwrite:
                return self.find_one_and_update(
                    {"symbol": symbol}, {"$set": {"metadata": metadata, "last_update": last_update}}
                )
            else:
                raise ValueError("conflict data")
        elif metadata is None:
            return None

        document = {"_id": bson.ObjectId(), "symbol": symbol, "metadata": metadata, "last_update": last_update}
        mongo_retry(self.insert_one)(document)

        logger.debug("Finished writing metadata for %s", symbol)
        return document

    def append(self, symbol, new_element, last_update=None):
        if last_update is None:
            last_update = dt.utcnow()
        if isinstance(new_element, list):
            return self.find_one_and_update(
                {"symbol": symbol},
                {"$push": {"metadata": {"$each": new_element}}, "$set": {"last_update": last_update}},
            )
        else:
            return self.find_one_and_update(
                {"symbol": symbol}, {"$push": {"metadata": new_element}, "$set": {"last_update": last_update}}
            )

    #
    # def prepend(self, symbol, metadata, last_update=None):
    #     """
    #     Prepend a metadata entry for `symbol`
    #
    #     Parameters
    #     ----------
    #     symbol : `str`
    #         symbol name for the item
    #     metadata : `dict`
    #         to be persisted
    #     start_time : `datetime.datetime`
    #         when metadata becomes effective
    #         Default: datetime.datetime.min
    #     """
    #     if metadata is None:
    #         return
    #     if start_time is None:
    #         start_time = dt.min
    #     if last_update is None:
    #         last_update = dt.utcnow()
    #     old_metadata = self.find_one({'symbol': symbol}, sort=[('start_time', pymongo.ASCENDING)])
    #     if old_metadata is not None:
    #         if old_metadata['start_time'] <= start_time:
    #             raise ValueError('start_time={} is later than the first metadata @{}'.format(start_time,
    #                                                                                          old_metadata['start_time']))
    #         if old_metadata['metadata'] == metadata:
    #             self.find_one_and_update({'symbol': symbol}, {'$set': {'start_time': start_time, "last_update": last_update}},
    #                                      sort=[('start_time', pymongo.ASCENDING)])
    #             old_metadata['start_time'] = start_time
    #             old_metadata['last_update'] = last_update
    #             return old_metadata
    #         end_time = old_metadata.get('start_time')
    #     else:
    #         end_time = None
    #
    #     document = {'_id': bson.ObjectId(), 'symbol': symbol, 'metadata': metadata, 'start_time': start_time, "last_update": last_update}
    #     if end_time is not None:
    #         document['end_time'] = end_time
    #     mongo_retry(self.insert_one)(document)
    #
    #     logger.debug('Finished writing metadata for %s', symbol)
    #     return document

    # def pop(self, symbol):
    #     """
    #     Delete current metadata of `symbol`
    #
    #     Parameters
    #     ----------
    #     symbol : `str`
    #         symbol name to delete
    #
    #     Returns
    #     -------
    #     Deleted metadata
    #     """
    #     last_metadata = self.find_one({'symbol': symbol})
    #     if last_metadata is None:
    #         raise NoDataFoundException('No metadata found for symbol {}'.format(symbol))
    #
    #     self.find_one_and_delete({'symbol': symbol})
    #     mongo_retry(self.find_one_and_update)({'symbol': symbol})
    #
    #     return last_metadata

    @mongo_retry
    def delete(self, symbol):
        """
        Delete all metadata of `symbol`

        Parameters
        ----------
        symbol : `str`
            symbol name to delete
        """
        self.delete_one({"symbol": symbol})

    @mongo_retry
    def rename(self, from_symbol, to_symbol):
        """

        Args:
            from_symbol:
            to_symbol:

        Returns:

        """
        self.update_many({"symbol": from_symbol}, {"$set": {"symbol": to_symbol}})
