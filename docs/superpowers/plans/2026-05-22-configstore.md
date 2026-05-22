# ConfigStore Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `ConfigStore` that stores per-symbol, per-datetime key-value configuration dicts in MongoDB, following the existing `BSONStore`/`MetadataStore` pattern.

**Architecture:** A new `ConfigStore(BSONStore)` class in `arctic/store/config_store.py`. Each `(symbol, datetime)` pair is a single MongoDB document in a `{library}.configs` subcollection with a compound unique index. Three read methods (exact, range, as-of), one upsert write method, and standard management methods. Registered in `arctic.py` via `CONFIG_STORE_TYPE` constant.

**Tech Stack:** Python 3, pymongo, bson, MongoDB

---

## File Structure

| File | Action | Purpose |
|------|--------|---------|
| `arctic/store/config_store.py` | Create | ConfigStore class |
| `arctic/arctic.py` | Modify | Register CONFIG_STORE_TYPE + LIBRARY_TYPES entry |
| `tests/unit/store/test_config_store.py` | Create | Unit tests with mocked mongo |
| `tests/integration/store/test_config_store.py` | Create | Integration tests with real mongo |
| `arctic/fixtures/arctic.py` | Modify | Add `config_store_lib` fixture |

---

### Task 1: Create the ConfigStore class

**Files:**
- Create: `arctic/store/config_store.py`

- [ ] **Step 1: Write the ConfigStore class**

```python
import logging
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
        last_update = dt.utcnow()
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
```

- [ ] **Step 2: Commit**

```bash
git add arctic/store/config_store.py
git commit -m "add ConfigStore class for symbol-datetime key-value config storage"
```

---

### Task 2: Register ConfigStore in arctic.py and add test fixture

**Files:**
- Modify: `arctic/arctic.py`
- Modify: `arctic/fixtures/arctic.py`

- [ ] **Step 1: Add import and registration in arctic.py**

In `arctic/arctic.py`, add the import for `config_store` and register it. Find the existing store imports (line 18) and add one more. Find the `LIBRARY_TYPES` dict (lines 45-53) and add an entry.

Import (after the existing `from .store import ...` on line 18):
```python
from .store import version_store, bson_store, metadata_store, sequence_store, config_store
```

Constants (add after line 44):
```python
CONFIG_STORE = config_store.CONFIG_STORE_TYPE
```

LIBRARY_TYPES dict (add inside the dict):
```python
    config_store.CONFIG_STORE_TYPE: config_store.ConfigStore,
```

`__all__` list (add after line 31, inside the list):
```python
    "CONFIG_STORE",
```

- [ ] **Step 2: Add test fixture in arctic/fixtures/arctic.py**

Add import at top:
```python
from ..store.config_store import CONFIG_STORE_TYPE
```

Add fixture after `ms_lib` fixture (around line 329):
```python
@pytest.fixture(scope="function")
def config_store_lib(arctic, library_name):
    arctic.initialize_library(library_name, CONFIG_STORE_TYPE)
    return arctic.get_library(library_name)
```

- [ ] **Step 3: Commit**

```bash
git add arctic/arctic.py arctic/fixtures/arctic.py
git commit -m "register ConfigStore in arctic.py and add test fixture"
```

---

### Task 3: Write unit tests (mocked MongoDB)

**Files:**
- Create: `tests/unit/store/test_config_store.py`

- [ ] **Step 1: Write the unit tests**

```python
from datetime import datetime as dt
from unittest.mock import create_autospec, call

from arctic.store.config_store import ConfigStore

# Test data
SYMBOL = 'AAPL'
DT1 = dt(2026, 1, 15, 10, 0)
DT2 = dt(2026, 2, 15, 10, 0)
DT3 = dt(2026, 3, 15, 10, 0)
CONFIG1 = {'strike': 100, 'expiry': '2026-06-15'}
CONFIG2 = {'strike': 110, 'expiry': '2026-07-15'}


def test_ensure_index():
    cs = create_autospec(ConfigStore)
    ConfigStore._ensure_index(cs)
    assert cs.create_index.call_args_list == [
        call([('symbol', 1), ('datetime', 1)],
             unique=True, background=True),
    ]


def test_read_exact_hit():
    cs = create_autospec(ConfigStore)
    cs.find_one.return_value = {'symbol': SYMBOL, 'datetime': DT1, 'config': CONFIG1}
    result = ConfigStore.read(cs, SYMBOL, DT1)
    cs.find_one.assert_called_once_with({'symbol': SYMBOL, 'datetime': DT1})
    assert result == CONFIG1


def test_read_exact_miss():
    cs = create_autospec(ConfigStore)
    cs.find_one.return_value = None
    result = ConfigStore.read(cs, SYMBOL, DT1)
    assert result is None


def test_read_range():
    cs = create_autospec(ConfigStore)
    cs.find.return_value = [
        {'symbol': SYMBOL, 'datetime': DT1, 'config': CONFIG1},
        {'symbol': SYMBOL, 'datetime': DT2, 'config': CONFIG2},
    ]
    result = ConfigStore.read_range(cs, SYMBOL, (DT1, DT2))
    cs.find.assert_called_once_with(
        {'symbol': SYMBOL, 'datetime': {'$gte': DT1, '$lte': DT2}},
        sort=[('datetime', 1)],
    )
    assert result == [(DT1, CONFIG1), (DT2, CONFIG2)]


def test_read_range_empty():
    cs = create_autospec(ConfigStore)
    cs.find.return_value = []
    result = ConfigStore.read_range(cs, SYMBOL, (DT1, DT2))
    assert result == []


def test_read_as_of_hit():
    cs = create_autospec(ConfigStore)
    cs.find_one.return_value = {'symbol': SYMBOL, 'datetime': DT1, 'config': CONFIG1}
    result = ConfigStore.read_as_of(cs, SYMBOL, DT2)
    cs.find_one.assert_called_once_with(
        {'symbol': SYMBOL, 'datetime': {'$lte': DT2}},
        sort=[('datetime', -1)],
    )
    assert result == CONFIG1


def test_read_as_of_miss():
    cs = create_autospec(ConfigStore)
    cs.find_one.return_value = None
    result = ConfigStore.read_as_of(cs, SYMBOL, DT1)
    assert result is None


def test_list_symbols_simple():
    cs = create_autospec(ConfigStore)
    cs.distinct.return_value = ['AAPL', 'MSFT']
    result = ConfigStore.list_symbols(cs)
    cs.distinct.assert_called_once_with('symbol')
    assert result == ['AAPL', 'MSFT']


def test_list_symbols_regex():
    cs = create_autospec(ConfigStore)
    cs.aggregate.return_value = []
    expected_pipeline = [
        {'$sort': {'symbol': 1, 'datetime': -1}},
        {'$match': {'symbol': {'$regex': 'AAPL.*'}}},
        {'$group': {'_id': '$symbol', 'config': {'$first': '$config'}}},
        {'$project': {'_id': 0, 'symbol': '$_id'}},
    ]
    ConfigStore.list_symbols(cs, regex='AAPL.*')
    cs.aggregate.assert_called_once_with(expected_pipeline)


def test_list_symbols_kwargs():
    cs = create_autospec(ConfigStore)
    cs.aggregate.return_value = []
    expected_pipeline = [
        {'$sort': {'symbol': 1, 'datetime': -1}},
        {'$group': {'_id': '$symbol', 'config': {'$first': '$config'}}},
        {'$match': {'config.foo': 'bar'}},
        {'$project': {'_id': 0, 'symbol': '$_id'}},
    ]
    ConfigStore.list_symbols(cs, foo='bar')
    cs.aggregate.assert_called_once_with(expected_pipeline)


def test_has_symbol_true():
    cs = create_autospec(ConfigStore)
    cs.find_one.return_value = {'symbol': SYMBOL}
    result = ConfigStore.has_symbol(cs, SYMBOL)
    cs.find_one.assert_called_once_with({'symbol': SYMBOL})
    assert result is True


def test_has_symbol_false():
    cs = create_autospec(ConfigStore)
    cs.find_one.return_value = None
    result = ConfigStore.has_symbol(cs, SYMBOL)
    assert result is False


def test_delete_single():
    cs = create_autospec(ConfigStore)
    ConfigStore.delete(cs, SYMBOL, datetime=DT1)
    cs.delete_one.assert_called_once_with({'symbol': SYMBOL, 'datetime': DT1})


def test_delete_all():
    cs = create_autospec(ConfigStore)
    ConfigStore.delete(cs, SYMBOL)
    cs.delete_many.assert_called_once_with({'symbol': SYMBOL})


def test_rename():
    cs = create_autospec(ConfigStore)
    ConfigStore.rename(cs, 'OLD', 'NEW')
    cs.update_many.assert_called_once_with({'symbol': 'OLD'}, {'$set': {'symbol': 'NEW'}})
```

- [ ] **Step 2: Run unit tests to verify they pass**

```bash
python -m pytest tests/unit/store/test_config_store.py -v
```

- [ ] **Step 3: Commit**

```bash
git add tests/unit/store/test_config_store.py
git commit -m "add unit tests for ConfigStore"
```

---

### Task 4: Write integration tests (real MongoDB)

**Files:**
- Create: `tests/integration/store/test_config_store.py`

- [ ] **Step 1: Write the integration tests**

```python
import pickle
from datetime import datetime as dt

import pytest

SYMBOL1 = 'symbol1'
SYMBOL2 = 'symbol2'
DT1 = dt(2025, 1, 15, 10, 0)
DT2 = dt(2025, 2, 15, 10, 0)
DT3 = dt(2025, 3, 15, 10, 0)
CONFIG1 = {'strike': 100, 'expiry': '2025-06-15'}
CONFIG2 = {'strike': 110, 'expiry': '2025-07-15'}
CONFIG3 = {'strike': 120, 'expiry': '2025-09-15'}


def test_pickle(config_store_lib):
    buff = pickle.dumps(config_store_lib)
    restored = pickle.loads(buff)
    assert config_store_lib._arctic_lib.get_name() == restored._arctic_lib.get_name()
    assert "arctic_test.TEST" in str(config_store_lib)
    assert str(config_store_lib) == repr(config_store_lib)


def test_has_symbol(config_store_lib):
    assert not config_store_lib.has_symbol(SYMBOL1)
    config_store_lib.write(SYMBOL1, DT1, CONFIG1)
    assert config_store_lib.has_symbol(SYMBOL1)


def test_read_write_exact(config_store_lib):
    assert config_store_lib.read(SYMBOL1, DT1) is None
    config_store_lib.write(SYMBOL1, DT1, CONFIG1)
    assert config_store_lib.read(SYMBOL1, DT1) == CONFIG1


def test_overwrite(config_store_lib):
    config_store_lib.write(SYMBOL1, DT1, CONFIG1)
    config_store_lib.write(SYMBOL1, DT1, CONFIG2)
    assert config_store_lib.read(SYMBOL1, DT1) == CONFIG2


def test_read_range(config_store_lib):
    config_store_lib.write(SYMBOL1, DT1, CONFIG1)
    config_store_lib.write(SYMBOL1, DT2, CONFIG2)
    config_store_lib.write(SYMBOL1, DT3, CONFIG3)

    results = config_store_lib.read_range(SYMBOL1, (DT1, DT2))
    assert results == [(DT1, CONFIG1), (DT2, CONFIG2)]


def test_read_range_empty(config_store_lib):
    results = config_store_lib.read_range(SYMBOL1, (DT1, DT2))
    assert results == []


def test_read_as_of(config_store_lib):
    config_store_lib.write(SYMBOL1, DT1, CONFIG1)
    config_store_lib.write(SYMBOL1, DT3, CONFIG3)

    # Exact match
    assert config_store_lib.read_as_of(SYMBOL1, DT1) == CONFIG1

    # Between entries - gets the one at or before
    assert config_store_lib.read_as_of(SYMBOL1, DT2) == CONFIG1

    # After last entry
    assert config_store_lib.read_as_of(SYMBOL1, dt(2026, 1, 1)) == CONFIG3

    # Before first entry
    assert config_store_lib.read_as_of(SYMBOL1, dt(2020, 1, 1)) is None


def test_read_as_of_miss(config_store_lib):
    assert config_store_lib.read_as_of('nonexistent', DT1) is None


def test_list_symbols(config_store_lib):
    config_store_lib.write(SYMBOL1, DT1, CONFIG1)
    config_store_lib.write(SYMBOL2, DT1, CONFIG2)
    symbols = config_store_lib.list_symbols()
    assert SYMBOL1 in symbols
    assert SYMBOL2 in symbols


def test_list_symbols_regex(config_store_lib):
    config_store_lib.write(SYMBOL1, DT1, CONFIG1)
    config_store_lib.write(SYMBOL2, DT1, CONFIG1)
    symbols = config_store_lib.list_symbols(regex='symbol1')
    assert symbols == ['symbol1']


def test_delete_single(config_store_lib):
    config_store_lib.write(SYMBOL1, DT1, CONFIG1)
    config_store_lib.write(SYMBOL1, DT2, CONFIG2)
    config_store_lib.delete(SYMBOL1, datetime=DT1)
    assert config_store_lib.read(SYMBOL1, DT1) is None
    assert config_store_lib.read(SYMBOL1, DT2) == CONFIG2
    assert config_store_lib.has_symbol(SYMBOL1)


def test_delete_all(config_store_lib):
    config_store_lib.write(SYMBOL1, DT1, CONFIG1)
    config_store_lib.write(SYMBOL1, DT2, CONFIG2)
    config_store_lib.delete(SYMBOL1)
    assert not config_store_lib.has_symbol(SYMBOL1)


def test_rename(config_store_lib):
    config_store_lib.write(SYMBOL1, DT1, CONFIG1)
    config_store_lib.write(SYMBOL1, DT2, CONFIG2)
    config_store_lib.rename(SYMBOL1, SYMBOL2)
    assert config_store_lib.read(SYMBOL2, DT1) == CONFIG1
    assert config_store_lib.read(SYMBOL2, DT2) == CONFIG2
    assert config_store_lib.read(SYMBOL1, DT1) is None


def test_write_different_symbols_same_datetime(config_store_lib):
    config_store_lib.write(SYMBOL1, DT1, CONFIG1)
    config_store_lib.write(SYMBOL2, DT1, CONFIG2)
    assert config_store_lib.read(SYMBOL1, DT1) == CONFIG1
    assert config_store_lib.read(SYMBOL2, DT1) == CONFIG2
```

- [ ] **Step 2: Run integration tests to verify they pass**

```bash
python -m pytest tests/integration/store/test_config_store.py -v
```

- [ ] **Step 3: Commit**

```bash
git add tests/integration/store/test_config_store.py
git commit -m "add integration tests for ConfigStore"
```

---

### Task 5: Final verification

- [ ] **Step 1: Run the full test suite**

```bash
python -m pytest tests/unit/store/test_config_store.py tests/integration/store/test_config_store.py -v
```

- [ ] **Step 2: Verify no regressions**

```bash
python -m pytest tests/unit/store/ tests/integration/store/ -v --timeout=60
```

- [ ] **Step 3: Verify library initialization works end to end**

```python
# Manual smoke test (not committed):
# arctic.initialize_library('test.config_store', arctic.CONFIG_STORE)
# lib = arctic['test.config_store']
# lib.write('AAPL', datetime.utcnow(), {'key': 'value'})
# assert lib.read('AAPL', ...) == {'key': 'value'}
```
