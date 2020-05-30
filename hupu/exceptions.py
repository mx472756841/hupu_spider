# -*- coding: utf-8 -*-


class BaseException(Exception):
    def __init__(self, *args, **kwargs):
        self.error_info = kwargs.pop("error_info", "未知错误")


class CookieException(BaseException):

    def __init__(self, *args, **kwargs):
        kwargs['error_info'] = kwargs.get('error_info', "cookie信息错误")
        super(CookieException, self).__init__(*args, **kwargs)
