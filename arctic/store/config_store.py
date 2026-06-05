import logging
from datetime import UTC
from datetime import datetime as dt

import pymongo

from .bson_store import BSONStore
from .._util import indent
from ..decorators import mongo_retry

logger = logging.getLogger(__name__)

CONFIG_STORE_TYPE = 'ConfigStore'


class ConfigStore(BSONStore):
    """
    Config Store. Stores per-symbol, per-datetime key-value configuration dicts.

    Entries are stored in the following format:
        'symbol': symbol name
        'datetime': datetime of this config snapshot
        'config': dict of key-value configuration pairs
        'last_update': when this entry was last written
    """

    @classmethod
    def initialize_library(cls, arctic_lib, hashed=True, **kwargs):
        ConfigStore(arctic_lib)._ensure_index()
        BSONStore.initialize_library(arctic_lib, hashed, **kwargs)

    @mongo_retry
    def _ensure_index(self):
        self.create_index(
            [('symbol', pymongo.ASCENDING), ('datetime', pymongo.ASCENDING)],
            unique=True, background=True,
        )

    def __init__(self, arctic_lib):
        self._arctic_lib = arctic_lib
        self._reset()

    def _reset(self):
        self._collection = self._arctic_lib.get_top_level_collection().configs

    def __getstate__(self):
        return {'arctic_lib': self._arctic_lib}

    def __setstate__(self, state):
        return ConfigStore.__init__(self, state['arctic_lib'])

    def __str__(self):
        return f"""<{self.__class__.__name__} at {hex(id(self))}>\n{indent(str(self._arctic_lib), 4)}"""

    def __repr__(self):
        return str(self)

    @mongo_retry
    def read(self, symbol, datetime):
        """
        Return config dict for the exact (symbol, datetime) lookup.

        Parameters
        ----------
        symbol : `str`
            symbol name for the item
        datetime : `datetime.datetime`
            exact datetime of the config snapshot

        Returns
        -------
        config dict or None if not found
        """
        res = self.find_one({'symbol': symbol, 'datetime': datetime})
        return res['config'] if res is not None else None

    @mongo_retry
    def read_range(self, symbol, date_range):
        """
        Return all configs for `symbol` within a datetime range.

        Parameters
        ----------
        symbol : `str`
            symbol name for the item
        date_range : `tuple` of (start, end) datetime.datetime
            both inclusive

        Returns
        -------
        list of (datetime, config) tuples, sorted by datetime ascending
        """
        start, end = date_range
        cursor = self.find(
            {'symbol': symbol, 'datetime': {'$gte': start, '$lte': end}},
            sort=[('datetime', pymongo.ASCENDING)],
        )
        return [(doc['datetime'], doc['config']) for doc in cursor]

    @mongo_retry
    def read_as_of(self, symbol, as_of):
        """
        Return config with the most recent datetime <= as_of.

        Parameters
        ----------
        symbol : `str`
            symbol name for the item
        as_of : `datetime.datetime`
            point in time to query as-of

        Returns
        -------
        config dict or None if no entry exists before as_of
        """
        res = self.find_one(
            {'symbol': symbol, 'datetime': {'$lte': as_of}},
            sort=[('datetime', pymongo.DESCENDING)],
        )
        return res['config'] if res is not None else None

    def write(self, symbol, datetime, config):
        """
        Write config for (symbol, datetime). Overwrites if already exists.

        Parameters
        ----------
        symbol : `str`
            symbol name for the item
        datetime : `datetime.datetime`
            datetime of this config snapshot
        config : `dict`
            key-value configuration pairs to persist
        """
        last_update = dt.now(tz=UTC)
        self.find_one_and_update(
            {'symbol': symbol, 'datetime': datetime},
            {'$set': {
                'symbol': symbol,
                'datetime': datetime,
                'config': config,
                'last_update': last_update,
            }},
            upsert=True,
        )

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
        if not (regex or kwargs):
            return self.distinct('symbol')

        index_query = {}
        if regex:
            index_query['symbol'] = {'$regex': regex}

        data_query = {}
        if kwargs:
            for k, v in kwargs.items():
                data_query['config.' + k] = v

        pipeline = [{'$sort': {'symbol': pymongo.ASCENDING,
                               'datetime': pymongo.DESCENDING}}]

        if index_query:
            pipeline.append({'$match': index_query})
        pipeline.append({'$group': {'_id': '$symbol',
                                    'config': {'$first': '$config'}}})
        if data_query:
            pipeline.append({'$match': data_query})
        pipeline.append({'$project': {'_id': 0, 'symbol': '$_id'}})

        return sorted(r['symbol'] for r in self.aggregate(pipeline))

    @mongo_retry
    def has_symbol(self, symbol):
        return self.find_one({'symbol': symbol}) is not None

    @mongo_retry
    def delete(self, symbol, datetime=None):
        """
        Delete config entry/entries for a symbol.

        Parameters
        ----------
        symbol : `str`
            symbol name for the item
        datetime : `datetime.datetime` or None
            if given, delete only that (symbol, datetime) entry.
            if None, delete all entries for the symbol.
        """
        if datetime is not None:
            self.delete_one({'symbol': symbol, 'datetime': datetime})
        else:
            self.delete_many({'symbol': symbol})

    def rename(self, from_symbol, to_symbol):
        """
        Rename a symbol across all entries.

        Parameters
        ----------
        from_symbol : `str`
            existing symbol name
        to_symbol : `str`
            new symbol name
        """
        self.update_many({'symbol': from_symbol}, {'$set': {'symbol': to_symbol}})
