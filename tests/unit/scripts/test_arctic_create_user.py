from unittest.mock import patch, call, ANY

from arctic.scripts.arctic_create_user import main
from ...util import run_as_main


def test_main_minimal():
    with patch('arctic.scripts.arctic_create_user.logger', autospec=True) as logger, \
         patch('arctic.scripts.arctic_create_user.MongoClient', autospec=True) as MC, \
         patch('arctic.scripts.arctic_create_user.get_mongodb_uri', autospec=True) as get_mongodb_uri:
        run_as_main(main, '--host', 'some_host',
                          '--password', 'asdf',
                          'user')
    get_mongodb_uri.assert_called_once_with('some_host')
    MC.assert_called_once_with(get_mongodb_uri.return_value)
    assert MC.return_value.__getitem__.call_args_list == [call('arctic_user')]
    db = MC.return_value.__getitem__.return_value
    assert [call('user', ANY, read_only=False)] == db.add_user.call_args_list
    assert logger.info.call_args_list == [call('Granted: user [WRITE] to arctic_user'),
                                          call('User creds: arctic_user/user/asdf')]


def test_main_with_db():
    with patch('arctic.scripts.arctic_create_user.MongoClient', autospec=True) as MC, \
         patch('arctic.scripts.arctic_create_user.get_mongodb_uri', autospec=True) as get_mongodb_uri:
        run_as_main(main, '--host', 'some_host',
                    '--db', 'some_db',
                    'jblackburn')
    get_mongodb_uri.assert_called_once_with('some_host')
    MC.assert_called_once_with(get_mongodb_uri.return_value)
    assert MC.return_value.__getitem__.call_args_list == [call('some_db')]
    db = MC.return_value.__getitem__.return_value
    assert [call('jblackburn', ANY, read_only=True)] == db.add_user.call_args_list


def test_main_with_db_write():
    with patch('arctic.scripts.arctic_create_user.MongoClient', autospec=True) as MC, \
         patch('arctic.scripts.arctic_create_user.get_mongodb_uri', autospec=True) as get_mongodb_uri:
        run_as_main(main, '--host', 'some_host',
                    '--db', 'some_db',
                    '--write',
                    'jblackburn')
    get_mongodb_uri.assert_called_once_with('some_host')
    MC.assert_called_once_with(get_mongodb_uri.return_value)
    assert MC.return_value.__getitem__.call_args_list == [call('some_db')]
    db = MC.return_value.__getitem__.return_value
    assert [call('jblackburn', ANY, read_only=False)] == db.add_user.call_args_list
