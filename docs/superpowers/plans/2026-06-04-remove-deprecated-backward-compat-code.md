# Remove Deprecated and Python<3.11 / Pandas<2.0 Backward Compatibility Code

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Strip all deprecated code paths and backward-compatibility shims for Python < 3.11 and pandas < 2.0.0 that are dead weight given the current `requires-python = ">=3.11"` and `pandas>=2.0.0` requirements.

**Architecture:** This is a cleanup-only change — no new functionality. Each task targets a self-contained file or related group of files. The changes remove dead branches, delete dead classes, simplify version-detection logic to unconditional modern-API calls, and drop tests that are permanently skipped. No behavioral change for the supported runtime.

**Tech Stack:** Python 3.11+, pandas 2.0+, pymongo 4.0+

---

## Summary of Findings

### Deprecated Code (dead on supported versions):
| # | File | Issue |
|---|------|-------|
| D1 | `arctic/_util.py` | pymongo version detection + dead branch for deprecated `collection.count()` |
| D2 | `arctic/store/_pandas_ndarray_store.py` | `PandasPanelStore` class (Panel removed in pandas 1.0) |
| D3 | `arctic/__init__.py` | Public export of `PandasPanelStore` |
| D4 | `arctic/tickstore/tickstore.py:351` | `.to_pydatetime(warn=False)` — `warn` param deprecated |

### Backward Compatibility Code (dead on Python>=3.11, pandas>=2.0):
| # | File | Issue |
|---|------|-------|
| B1 | `arctic/serialization/numpy_arrays.py:15-17` | `pd.__version__` check for pandas < 0.23 monkey-patch |
| B2 | `arctic/tickstore/tickstore.py:341-345` | `pd.__version__` branch for pandas 0.x/1.0 `_arrays_to_mgr` signature |
| B3 | `arctic/store/_version_store_utils.py:136-139` | `pickle_compat_load` alias (was for `pandas.compat.pickle_compat`) |
| B4 | `arctic/store/_pickle_store.py:93-96` | Pickle protocol capped at 4 for Python < 3.8 compat |
| B5 | `arctic/_util.py:15,22-28` | pymongo version detection (same as D1) |
| B6 | `tests/unit/store/test_pickle_store.py:89-104` | `xfail` test for Python 2 lz4 data |
| B7 | `tests/integration/store/test_pandas_store.py:988-1009` | Permanently skipped py2 test `test_forced_encodings_with_df_mixed_types` |
| B8 | `tests/integration/store/test_pandas_store.py:1054-1080` | Permanently skipped py2 test `test_forced_encodings_with_df` |
| B9 | `arctic/_config.py:92` | `FORCE_BYTES_TO_UNICODE` flag (Python 2 migration aid) |
| B10 | `arctic/serialization/numpy_records.py:353-364` | `force_bytes_to_unicode` conversion block |
| B11 | `arctic/store/_pandas_ndarray_store.py:204` | Passes `force_bytes_to_unicode` to deserializer |

### Items Explicitly NOT in Scope:
- **Legacy forward-pointers system** (`FwPointersCfg.DISABLED`, `_config.py:41-55`, `_version_store_utils.py:103-129`): This is the current default mode, not dead code. Changing it requires a migration plan for existing data. Separate plan needed.
- **`multi_index.py:67-69`**: The `hasattr(idx, 'lexsort_depth')` fallback is forward-looking compat for pandas 3.x, not backward compat. Keep it.
- **`hasattr(datetime, "timezone")`** in tests: Harmless defensive check.
- **pandas internal API usage** (`_arrays_to_mgr`, `max_len_string_array`): These work on pandas 2.x and are runtime dependencies, not compat shims.
- **`FORCE_BYTES_TO_UNICODE` env-var-based flag**: While it is a Python 2 migration aid, removing it requires care because users may have data written under Python 2. The flag itself is kept but noted as legacy.

---

### Task 1: Remove pymongo version detection and deprecated `count()` branch in `_util.py`

**Files:**
- Modify: `arctic/_util.py`

- [ ] **Step 1: Simplify `mongo_count()` to use only the modern API**

Replace lines 14-99 (the `_use_new_count_api` global, `_detect_new_count_api()`, and the branch in `mongo_count()`):

```python
# Remove lines 14-15:
# _use_new_count_api = None

# Remove lines 22-27:
# def _detect_new_count_api():
#     try:
#         mongo_v = [int(v) for v in pymongo.version.split('.')]
#         return mongo_v[0] >= 3 and mongo_v[1] >= 7
#     except Exception:
#         return False

# In mongo_count() (lines 82-99), replace the entire function body with:
def mongo_count(collection, filter=None, **kwargs):
    """
    use with care as filters on un-indexed fields will generate COLLSCAN.
    """
    filter = {} if filter is None else filter
    if filter == {}:
        # fast. uses collection metadata
        return collection.estimated_document_count(**kwargs)
    else:
        # transactions supported, but slow for non-indexed filters
        return collection.count_documents(filter=filter, **kwargs)
```

- [ ] **Step 2: Run existing tests to verify no regression**

Run: `pytest tests/unit/store/ -v --timeout=60 2>&1 | tail -20`
Expected: All tests pass

- [ ] **Step 3: Commit**

```bash
git add arctic/_util.py
git commit -m "refactor: remove pymongo version detection and deprecated count() fallback"
```

---

### Task 2: Remove `pd.__version__` monkey-patch in `numpy_arrays.py`

**Files:**
- Modify: `arctic/serialization/numpy_arrays.py`

- [ ] **Step 1: Remove the conditional monkey-patch**

Remove lines 15-17:
```python
# Remove:
if int(pd.__version__.split('.')[1]) > 22:
    from functools import partial
    pd.concat = partial(pd.concat, sort=False)
```

This was a workaround for a pandas < 0.23 behavioral change. On pandas 2.x, `sort=False` is already the default for `pd.concat`.

- [ ] **Step 2: Run serializer tests**

Run: `pytest tests/unit/serialization/ -v --timeout=60 2>&1 | tail -20`
Expected: All tests pass

- [ ] **Step 3: Commit**

```bash
git add arctic/serialization/numpy_arrays.py
git commit -m "refactor: remove pandas <0.23 concat sort monkey-patch"
```

---

### Task 3: Remove pandas version branch in `tickstore.py`

**Files:**
- Modify: `arctic/tickstore/tickstore.py`

- [ ] **Step 1: Remove dead branch and simplify `_arrays_to_mgr` call**

Replace lines 341-345:
```python
# Replace:
if pd.__version__.startswith("0.") or pd.__version__.startswith("1.0"):
    mgr = _arrays_to_mgr(arrays, columns, index, columns, dtype=None)
else:
    # 4th argument removed + new argument typ is mandatory
    mgr = _arrays_to_mgr(arrays, columns, index, dtype=None, typ="array")

# With:
mgr = _arrays_to_mgr(arrays, columns, index, dtype=None, typ="array")
```

- [ ] **Step 2: Fix deprecated `warn` parameter on line 351**

Find the `.to_pydatetime(warn=False)` call around lines 49-50 in `arctic/chunkstore/date_chunker.py` and in tickstore.py. Let's fix them:

In `arctic/tickstore/tickstore.py`, search for `.to_pydatetime(warn=False)` and remove the `warn=False` argument:
```python
# Replace any:
# .to_pydatetime(warn=False)
# With:
# .to_pydatetime()
```

In `arctic/chunkstore/date_chunker.py` lines 49-50:
```python
# Replace:
start = period_obj_reduced[count].start_time.to_pydatetime(warn=False)
end = period_obj_reduced[count].end_time.to_pydatetime(warn=False)
# With:
start = period_obj_reduced[count].start_time.to_pydatetime()
end = period_obj_reduced[count].end_time.to_pydatetime()
```

- [ ] **Step 3: Run tickstore tests**

Run: `pytest tests/ -k "tick" -v --timeout=60 2>&1 | tail -20`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add arctic/tickstore/tickstore.py arctic/chunkstore/date_chunker.py
git commit -m "refactor: remove pandas 0.x/1.0 version branch and deprecated to_pydatetime(warn=)"
```

---

### Task 4: Remove `PandasPanelStore` class and its export

**Files:**
- Modify: `arctic/store/_pandas_ndarray_store.py`
- Modify: `arctic/__init__.py`

- [ ] **Step 1: Remove `PandasPanelStore` class**

In `arctic/store/_pandas_ndarray_store.py`, delete lines 211-248 (the entire `PandasPanelStore` class):
```python
# Remove this entire class:
class PandasPanelStore(PandasDataFrameStore):
    TYPE = 'pandaspan'

    @staticmethod
    def can_write_type(data):
        # late import, Panel is removed in pandas 1
        from pandas import Panel
        return isinstance(data, Panel)

    def can_write(self, version, symbol, data):
        if self.can_write_type(data):
            frame = data.to_frame(filter_observations=False)
            if NP_OBJECT_DTYPE in frame.dtypes.values or (hasattr(data, 'index') and data.index.dtype is NP_OBJECT_DTYPE):
                return self.SERIALIZER.can_convert_to_records_without_objects(frame, symbol)
            return True
        return False

    def write(self, arctic_lib, version, symbol, item, previous_version):
        if np.product(item.shape) == 0:
            raise ValueError('Cannot insert a zero size panel into mongo.')
        if not np.all(len(i.names) == 1 for i in item.axes):
            raise ValueError('Cannot insert panels with multiindexes')
        item = item.to_frame(filter_observations=False)
        if len(set(item.dtypes)) == 1:
            item = DataFrame(item.stack())
        elif item.columns.dtype != np.dtype('object'):
            raise ValueError('Cannot support non-object dtypes for columns')
        super(PandasPanelStore, self).write(arctic_lib, version, symbol, item, previous_version)

    def read(self, arctic_lib, version, symbol, **kwargs):
        item = super(PandasPanelStore, self).read(arctic_lib, version, symbol, **kwargs)
        if len(item.index.names) == 3:
            return item.iloc[:, 0].unstack().to_panel()
        return item.to_panel()
```

- [ ] **Step 2: Remove `PandasPanelStore` from `__init__.py`**

In `arctic/__init__.py`, change line 8:
```python
# Replace:
from .store._pandas_ndarray_store import PandasDataFrameStore, PandasSeriesStore, PandasPanelStore
# With:
from .store._pandas_ndarray_store import PandasDataFrameStore, PandasSeriesStore
```

Remove `"PandasPanelStore"` from the `__all__` list or relevant export on line 53.

- [ ] **Step 3: Run tests to verify no references break**

Run: `pytest tests/ -v --timeout=120 2>&1 | tail -30`
Expected: All tests pass (no test references PandasPanelStore)

- [ ] **Step 4: Commit**

```bash
git add arctic/store/_pandas_ndarray_store.py arctic/__init__.py
git commit -m "refactor: remove PandasPanelStore (Panel removed since pandas 1.0)"
```

---

### Task 5: Remove pickle protocol cap and `pickle_compat_load` alias

**Files:**
- Modify: `arctic/store/_pickle_store.py`
- Modify: `arctic/store/_version_store_utils.py`

- [ ] **Step 1: Remove pickle protocol cap in `_pickle_store.py`**

Replace lines 93-97:
```python
# Replace:
# Python 3.8 onwards uses protocol 5 which cannot be unpickled in Python versions below that, so limiting
# it to use a maximum of protocol 4 in Python which is understood by 3.4 onwards and is still fairly efficient.
# pickle version 4 is introduced with  python 3.4 and default with 3.8 onward
pickle_protocol = min(pickle.HIGHEST_PROTOCOL, 4)
pickled = pickle.dumps(item, protocol=pickle_protocol)

# With:
pickled = pickle.dumps(item, protocol=pickle.HIGHEST_PROTOCOL)
```

- [ ] **Step 2: Inline `pickle_compat_load` usage in `_pickle_store.py`**

In `_pickle_store.py` line 10, change the import:
```python
# Replace:
from ._version_store_utils import checksum, pickle_compat_load, version_base_or_id
# With:
from ._version_store_utils import checksum, version_base_or_id
```

Replace `pickle_compat_load(` with `pickle.load(` on lines 62 and 68:
```python
# Line 62: replace pickle_compat_load(io.BytesIO(data)) with pickle.load(io.BytesIO(data))
# Line 68: replace pickle_compat_load(io.BytesIO(data), encoding=encoding) with pickle.load(io.BytesIO(data), encoding=encoding)
```

- [ ] **Step 3: Remove `pickle_compat_load` from `_version_store_utils.py`**

Remove lines 136-139:
```python
# Remove:
# pandas 3.x removed pandas.compat.pickle_compat; for supported runtimes,
# the valid unpickler is the stdlib loader. Legacy Python 2 payload handling
# is provided by callers via pickle.load(..., encoding='latin_1').
pickle_compat_load = pickle.load
```

Check if `pickle` import is still needed for other uses in `_version_store_utils.py`. If `pickle` was only imported for `pickle_compat_load`, remove the `import pickle` line as well.

- [ ] **Step 4: Run pickle store tests**

Run: `pytest tests/unit/store/test_pickle_store.py -v --timeout=60 2>&1 | tail -20`
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add arctic/store/_pickle_store.py arctic/store/_version_store_utils.py
git commit -m "refactor: remove pickle protocol 4 cap and pickle_compat_load alias"
```

---

### Task 6: Remove Python 2 permanently-skipped tests

**Files:**
- Modify: `tests/unit/store/test_pickle_store.py`
- Modify: `tests/integration/store/test_pandas_store.py`

- [ ] **Step 1: Remove the xfail test in `test_pickle_store.py`**

Remove lines 89-104 (the entire `test_read_backward_compatibility` function and its decorator):
```python
# Remove:
@pytest.mark.xfail(sys.version_info >= (3,),
                   reason="lz4 data written with python2 not compatible with python3")
def test_read_backward_compatibility():
    """Test backwards compatibility with a pickled file that's created with Python 2.7.3,
    Numpy 1.7.1_ahl2 and Pandas 0.14.1
    """
    fname = path.join(path.dirname(__file__), "data", "test-data.pkl")
    # ...
```

Also check if the `sys` import is still needed after removing. Remove it if `sys` was only used for `sys.version_info`.

- [ ] **Step 2: Remove skipped py2 tests in `test_pandas_store.py`**

Remove lines 988-1009 (`test_forced_encodings_with_df_mixed_types`) and lines 1054-1080 (`test_forced_encodings_with_df`):
```python
# Remove both test functions and their @pytest.mark.skip decorators:
# - test_forced_encodings_with_df_mixed_types
# - test_forced_encodings_with_df
```

- [ ] **Step 3: Run affected test files**

Run: `pytest tests/unit/store/test_pickle_store.py tests/integration/store/test_pandas_store.py -v --timeout=120 2>&1 | tail -20`
Expected: All remaining tests pass

- [ ] **Step 4: Commit**

```bash
git add tests/unit/store/test_pickle_store.py tests/integration/store/test_pandas_store.py
git commit -m "test: remove Python 2 permanently-skipped/xfail tests"
```

---

### Task 7: Clean up Python 2/3 comments and legacy references

**Files:**
- Modify: `arctic/tickstore/tickstore.py`
- Modify: `arctic/serialization/numpy_records.py`

- [ ] **Step 1: Remove Python 2/3 comments in `tickstore.py`**

Find and remove the py2/py3 comments around lines 651-656:
```python
# Remove comments like:
# `string` in python2 and `bytes` in python3
# `UnicodeDecodeError` in python2 and `SystemError` in python3
```

- [ ] **Step 2: Remove Python 2 migration comments in `numpy_records.py`**

Remove the comment block around lines 353-357:
```python
# Remove:
# This is needed due to 'str' type in py2 when read back in py3 is 'bytes' which breaks the workflow
# of people migrating to py3. # https://github.com/manahl/arctic/issues/598
# This should not be used for a normal flow, and you should instead of writing unicode strings
# if you want to work with str in py3.,
```

Note: Keep the `force_bytes_to_unicode` logic itself — it still has valid use for data written under Python 2 in existing databases. Only the comments are being cleaned up.

- [ ] **Step 3: Commit**

```bash
git add arctic/tickstore/tickstore.py arctic/serialization/numpy_records.py
git commit -m "chore: remove outdated Python 2/3 migration comments"
```

---

### Task 8: Final verification — full test suite

**Files:** None (verification only)

- [ ] **Step 1: Run the full test suite**

Run: `pytest tests/ -v --timeout=120 2>&1 | tail -40`
Expected: All tests pass, no import errors, no references to removed symbols

- [ ] **Step 2: Verify no remaining references to removed symbols**

Run: `grep -r "PandasPanelStore\|pandaspan\|pickle_compat_load\|_detect_new_count_api\|_use_new_count_api" arctic/ tests/ 2>&1`
Expected: No output (no remaining references)

- [ ] **Step 3: Final commit (if any straggling cleanup needed)**

```bash
git add -A
git commit -m "chore: final cleanup of deprecated and py2 backward compat code"
```

---

## Self-Review

**1. Spec coverage:** All items in the findings summary are addressed:
- D1, B5: pymongo count() → Task 1
- B1: pd.concat monkey-patch → Task 2
- B2, D4: pandas version branch + to_pydatetime → Task 3
- D2, D3: PandasPanelStore → Task 4
- B3, B4: pickle_compat_load + protocol cap → Task 5
- B6, B7, B8: Dead tests → Task 6
- B9, B10, B11: force_bytes_to_unicode logic intentionally kept (data migration concern); only comments cleaned → Task 7

**2. Placeholder scan:** No TBD, TODO, or hand-wavy steps. Every step has exact code changes.

**3. Type consistency:** No cross-task type/function dependencies — each task is self-contained.
