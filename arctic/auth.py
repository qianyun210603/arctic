import logging
from collections import namedtuple

logger = logging.getLogger(__name__)


Credential = namedtuple("MongoCredentials", ['database', 'user', 'password'])


def get_auth(host, app_name, database_name):
    """
    Authentication hook to allow plugging in custom authentication credential providers
    """
    from .hooks import _get_auth_hook
    return _get_auth_hook(host, app_name, database_name)
