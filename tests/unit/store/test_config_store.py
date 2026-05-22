from datetime import datetime as dt
from unittest.mock import create_autospec, call

from arctic.store.config_store import ConfigStore

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
