# ConfigStore Design

## Overview

A new store for storing per-symbol, per-datetime key-value configuration data. The data hierarchy is `symbol -> datetime -> key-value config dict`. Each `(symbol, datetime)` pair is an independent point-in-time snapshot with a dict of configuration key-value pairs.

## Requirements

- **Data model**: `(symbol, datetime)` → config dict, point-in-time snapshots
- **Exact lookup**: query by exact `(symbol, datetime)` to get one config dict
- **Datetime range**: query by `symbol + (start, end)` to get all configs in that range
- **As-of query**: query by `symbol + as_of` to get the config with most recent `datetime <= as_of`
- **Write behavior**: overwrite when `(symbol, datetime)` already exists
- **Config retrieval**: always read back the full config dict (no filtering by config internals)

## Data Model

Each `(symbol, datetime)` pair maps to a single MongoDB document in a `{library}.configs` subcollection:

```
{
    _id: ObjectId,
    symbol: "AAPL",
    datetime: ISODate("2026-05-22T10:00:00Z"),
    config: {strike: 100, expiry: "2026-06-15", ...},
    last_update: ISODate("2026-05-22T10:00:05Z")
}
```

**Index**: Compound unique index on `(symbol, datetime)` covering all query patterns.

**Subcollection**: `configs` (following `MetadataStore`'s `.metadata` pattern).

## Architecture

`ConfigStore` extends `BSONStore`, following the same patterns as `MetadataStore` and `SequenceStore`:

- Inherits MongoDB operations from `BSONStore` (find, find_one, update_one, delete_one, delete_many, aggregate, etc.)
- Uses `@mongo_retry` decorator
- Registers via `CONFIG_STORE_TYPE` constant + `LIBRARY_TYPES` dict in `arctic.py`
- Constructor takes `arctic_lib` (ArcticLibraryBinding)
- Implements `_reset()`, `__getstate__`/`__setstate__` for pickling

## Interfaces

### Read

**`read(symbol, datetime)`** — Exact `(symbol, datetime)` lookup. Returns the config dict or None.
- MongoDB: `find_one({'symbol': symbol, 'datetime': datetime})`

**`read_range(symbol, date_range)`** — Returns all configs for `symbol` within `(start, end)` inclusive, sorted by datetime ASC. Returns `[(datetime, config), ...]`.
- MongoDB: `find({'symbol': symbol, 'datetime': {'$gte': start, '$lte': end}}, sort=[('datetime', ASCENDING)])`

**`read_as_of(symbol, as_of)`** — Returns the config with the most recent `datetime <= as_of`. Returns the config dict or None.
- MongoDB: `find_one({'symbol': symbol, 'datetime': {'$lte': as_of}}, sort=[('datetime', DESCENDING)])`

### Write

**`write(symbol, datetime, config)`** — Overwrite if `(symbol, datetime)` exists, insert otherwise. Sets `last_update` to now.
- MongoDB: `update_one({'symbol': symbol, 'datetime': datetime}, {'$set': {...}}, upsert=True)`

### Management

**`list_symbols(regex=None)`** — List distinct symbols, optional regex filter.
- Without regex: `distinct('symbol')`
- With regex/extra filtering: aggregation pipeline (like `MetadataStore.list_symbols`)

**`has_symbol(symbol)`** — Check if symbol has any entries.
- MongoDB: `find_one({'symbol': symbol})`

**`delete(symbol, datetime=None)`** — Delete one entry (if `datetime` given) or all entries for `symbol`.
- One entry: `delete_one({'symbol': symbol, 'datetime': datetime})`
- All entries: `delete_many({'symbol': symbol})`

**`rename(from_symbol, to_symbol)`** — Rename a symbol across all entries.
- MongoDB: `update_many({'symbol': from_symbol}, {'$set': {'symbol': to_symbol}})`

### Class Methods

**`initialize_library(cls, arctic_lib, **kwargs)`** — Create the compound unique index on `(symbol, datetime)`, then delegate to `BSONStore.initialize_library`.

## Registration

In `arctic.py`:
```python
from .store import config_store
CONFIG_STORE = config_store.CONFIG_STORE_TYPE
LIBRARY_TYPES[config_store.CONFIG_STORE_TYPE] = config_store.ConfigStore
```

## File Placement

New file: `arctic/store/config_store.py`

## Error Handling

- `read`/`read_as_of` return `None` on miss (no exception)
- `read_range` returns empty list on miss
- `delete` silently succeeds if nothing matches
- `write` is an upsert (never fails on duplicate)

No new exception types needed.

## Testing

Tests follow the same pattern as `MetadataStore` / `SequenceStore` tests:
- Integration tests with a real MongoDB via pytest
- Test exact read (hit and miss)
- Test range read (with/without results, boundary conditions)
- Test as-of read (exact match, between entries, before first entry, after last entry)
- Test write (insert new, overwrite existing)
- Test list_symbols with and without regex
- Test delete (single entry, all entries for symbol)
- Test rename
- Test has_symbol
