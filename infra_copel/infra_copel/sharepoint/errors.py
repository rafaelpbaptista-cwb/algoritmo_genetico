# -*- coding: utf-8 -*-
"""Exceptions do sharepoint."""
import re
from urllib.parse import unquote

RE_SHAREPLUMREQUESTERROR = re.compile(r'(?P<err_shareplum>.*?) :'
                                      r' (?P<err_resp>.*?): '
                                      r'(?P<msg>.*): '
                                      r'(?P<func>.*)'
                                      r'\((?P<arg>.*)\)/'
                                      r'(?P<method>.*)')


class SharepointError(Exception):
    """Erros gerais do sharepoint."""


class NotFoundError(SharepointError):
    """Localização não encontrada."""


class LockedError(SharepointError):
    """Localização travada."""


class SharepointTimeoutError(SharepointError):
    """Timeout no Sharepoint."""


def _analyze_shareplum_err(err):
    """Analisador das exceções do shareplum."""
    full_err = str(err)

    if match := RE_SHAREPLUMREQUESTERROR.match(full_err):

        err_dict = match.groupdict()
        msg = unquote(err_dict['arg'])
        # 404 Not Found
        if '404' in err_dict['err_resp']:
            raise NotFoundError(msg)
        # 423 Locked
        if '423' in err_dict['err_resp']:
            raise LockedError(msg)
    # Read timed out
    if 'Read timed out.' in full_err:
        raise SharepointTimeoutError(full_err)

    raise err
