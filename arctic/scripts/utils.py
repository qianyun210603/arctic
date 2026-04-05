
import logging

logger = logging.getLogger(__name__)

def setup_logging():
    """ Logging setup for console scripts
    """
    logging.basicConfig(format='%(asctime)s %(message)s', level='INFO')
