import pandas as pd

from arctic.store.versioned_item import VersionedItem


def test_versioned_item_str():
    item = VersionedItem(symbol="sym",
                         library="ONEMINUTE",
                         data=pd.DataFrame(),
                         version=1.0,
                         host='myhost',
                         metadata={'metadata': 'foo'})
    # pandas 3.x moved DataFrame to top-level namespace; pandas 2.x uses pandas.core.DataFrame
    major = int(pd.__version__.split('.')[0])
    if major >= 3:
        data_type = "pandas.DataFrame"
    else:
        data_type = "pandas.core.frame.DataFrame"

    expected = (
        "VersionedItem(symbol=sym,library=ONEMINUTE,"
        "data=<class '{}'>,version=1.0,metadata={{'metadata': 'foo'}},host=myhost)".format(data_type)
    )
    assert str(item) == expected
    assert repr(item) == expected


def test_versioned_item_default_host():
    item = VersionedItem(symbol="sym",
                         library="ONEMINUTE",
                         data=[1, 2, 3],
                         version=1.0,
                         metadata={'metadata': 'foo'})

    expected_item = VersionedItem(symbol="sym",
                                  library="ONEMINUTE",
                                  data=[1, 2, 3],
                                  version=1.0,
                                  host=None,
                                  metadata={'metadata': 'foo'})

    assert item == expected_item


def test_versioned_item_str_handles_none():
    item = VersionedItem(symbol=None,
                         library=None,
                         data=None,
                         version=None,
                         metadata=None,
                         host=None)

    assert str(item)


def test_versioned_item_metadata_dict():
    item = VersionedItem(symbol="test",
                         library="test_lib",
                         data=None,
                         version=1.2,
                         metadata=None,
                         host=None)
    assert(item.metadata_dict() == {'symbol': 'test', 'library': 'test_lib', 'version': 1.2})
