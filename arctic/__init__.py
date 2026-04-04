""" The Arctic TimeSeries and Tick store."""

import re

from .arctic import Arctic, register_library_type
from .arctic import VERSION_STORE, TICK_STORE, CHUNK_STORE, METADATA_STORE, SEQUENCE_STORE
from .store._ndarray_store import NdarrayStore
from .store._pandas_ndarray_store import PandasDataFrameStore, PandasSeriesStore, PandasPanelStore
from .store.version_store import register_versioned_storage, register_version

try:
    from importlib.metadata import version as metadata_version
    str_version = metadata_version("arctic_new")
    version_match = re.match(r"^(\d+(?:\.\d+)*)", str_version)
    release = tuple(int(v) for v in version_match.group(1).split(".")) if version_match else tuple()
    num_version = sum([1000 ** i * v for i, v in enumerate(reversed(release))])
    register_version(str_version, num_version)
except Exception:
    __version__ = None
    __version_parts__ = tuple()
    __version_numerical__ = 0
else:
    __version__ = str_version
    __version_parts__ = release
    __version_numerical__ = num_version


register_versioned_storage(PandasDataFrameStore)
register_versioned_storage(PandasSeriesStore)
register_versioned_storage(NdarrayStore)


# Public package API ---------------------------------------------------------
# Exported names: core API, store types and registration helpers, and version
# metadata. Keep this list explicit so tools and `from arctic import *`
# behave predictably.
__all__ = [
    "Arctic",
    "register_library_type",

    # Store type constants
    "VERSION_STORE",
    "TICK_STORE",
    "CHUNK_STORE",
    "METADATA_STORE",
    "SEQUENCE_STORE",

    # Concrete store classes
    "NdarrayStore",
    "PandasDataFrameStore",
    "PandasSeriesStore",
    "PandasPanelStore",

    # Registration / version helpers
    "register_versioned_storage",
    "register_version",

    # Version metadata
    "__version__",
    "__version_parts__",
    "__version_numerical__",
]

