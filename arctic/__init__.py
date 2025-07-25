""" The Arctic TimeSeries and Tick store."""

import pandas

from .arctic import Arctic, register_library_type
from .arctic import VERSION_STORE, TICK_STORE, CHUNK_STORE, METADATA_STORE, SEQUENCE_STORE
from .store._ndarray_store import NdarrayStore
from .store._pandas_ndarray_store import PandasDataFrameStore, PandasSeriesStore, PandasPanelStore
from .store.version_store import register_versioned_storage, register_version

try:
    from packaging.version import Version
    import importlib_metadata
    str_version = importlib_metadata.version("arctic_new")
    parsed_version = Version(str_version)
    num_version = sum([1000 ** i * v for i, v in enumerate(reversed(parsed_version.release))])
    register_version(str_version, num_version)
except Exception as e:
    __version__ = None
    __version_parts__ = tuple()
    __version_numerical__ = 0
else:
    __version__ = str_version
    __version_parts__ = parsed_version.release
    __version_numerical__ = num_version


register_versioned_storage(PandasDataFrameStore)
register_versioned_storage(PandasSeriesStore)
register_versioned_storage(NdarrayStore)

