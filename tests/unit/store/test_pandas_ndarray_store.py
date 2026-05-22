import numpy as np

# Do not remove PandasStore
from arctic.store._pandas_ndarray_store import PandasDataFrameStore
from tests.util import read_str_as_pandas


def test_read_multi_index_with_no_ts_info():
    # github #81: old multi-index ts would not have tz info in metadata. Ensure read is not broken
    df = read_str_as_pandas("""index 1 |    index 2 | SPAM
                            2012-09-08 | 2015-01-01 |  1.0
                            2012-09-09 | 2015-01-02 |  1.1
                            2012-10-08 | 2015-01-03 |  2.0""", num_index=2)
    store = PandasDataFrameStore()
    record = store.SERIALIZER.serialize(df)[0]

    # now take away timezone info from metadata
    record = np.array(record.tolist(), dtype=np.dtype([('index 1', '<M8[ns]'), ('index 2', '<M8[ns]'), ('SPAM', '<f8')],
                                                      metadata={'index': ['index 1', 'index 2'], 'columns': ['SPAM']}))
    assert store.SERIALIZER._index_from_records(record).equals(df.index)
