from unittest.mock import patch, sentinel, call

from arctic.scripts.arctic_fsck import main
from ...util import run_as_main


def test_main():
    stats = {
        'chunks': {'sharded': False, 'count': 10, 'size': 1024 * 1024 * 5, 'avgObjSize': 1024 * 1024},
        'versions': {'count': 2, 'size': 1024 * 1024 * 1, 'avgObjSize': 1024 * 1024},
    }

    with patch('arctic.scripts.arctic_fsck.Arctic') as Arctic, \
         patch('arctic.scripts.arctic_fsck.get_mongodb_uri') as get_mongodb_uri:
        # Configure the mocked Arctic instance to return sensible stats and symbols
        Arctic.return_value.__getitem__.return_value.stats.return_value = stats
        Arctic.return_value.__getitem__.return_value.list_symbols.return_value = ['s1', 's2']

        run_as_main(main, '--host', f'{sentinel.host}:{sentinel.port}',
                          '-v', '--library', 'sentinel.library', 'lib2', '-f')
    get_mongodb_uri.assert_called_once_with('sentinel.host:sentinel.port')
    Arctic.assert_called_once_with(get_mongodb_uri.return_value)
    assert Arctic.return_value.__getitem__.return_value._fsck.call_args_list == [call(False),
                                                                                   call(False), ]


def test_main_dry_run():
    stats = {
        'chunks': {'sharded': False, 'count': 5, 'size': 1024 * 1024 * 2, 'avgObjSize': 1024 * 1024},
        'versions': {'count': 1, 'size': 1024 * 1024 * 1, 'avgObjSize': 1024 * 1024},
    }

    with patch('arctic.scripts.arctic_fsck.Arctic') as Arctic, \
         patch('arctic.scripts.arctic_fsck.get_mongodb_uri') as get_mongodb_uri:
        Arctic.return_value.__getitem__.return_value.stats.return_value = stats
        Arctic.return_value.__getitem__.return_value.list_symbols.return_value = ['s1']

        run_as_main(main, '--host', f'{sentinel.host}:{sentinel.port}',
                    '-v', '--library', 'sentinel.library', 'sentinel.lib2')
    get_mongodb_uri.assert_called_once_with('sentinel.host:sentinel.port')
    Arctic.assert_called_once_with(get_mongodb_uri.return_value)
    assert Arctic.return_value.__getitem__.return_value._fsck.call_args_list == [call(True),
                                                                                   call(True), ]
