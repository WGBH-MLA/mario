from os import remove
from os.path import join

from mario.config import MEDIA_DIR
from mario.log import log


class CLAMSAppError(Exception):
    """Error raised when a CLAMS app fails to run"""


def rm(filename: str):
    """
    Remove a file from the media directory.
    """
    try:
        remove(join(MEDIA_DIR, filename))
    except FileNotFoundError:
        log.debug(f'File {filename} not found.')
    log.success(f'File {filename} removed.')
