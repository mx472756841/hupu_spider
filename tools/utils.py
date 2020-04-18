import datetime
import json

import settings
from tools.db import RedisClient


def recursive_unicode(obj):
    """Walks a simple data structure, converting byte strings to unicode.

    Supports lists, tuples, and dictionaries.
    """
    if isinstance(obj, dict):
        return dict((recursive_unicode(k), recursive_unicode(v)) for (k, v) in obj.items())
    elif isinstance(obj, list):
        return list(recursive_unicode(i) for i in obj)
    elif isinstance(obj, tuple):
        return tuple(recursive_unicode(i) for i in obj)
    elif isinstance(obj, bytes):
        return obj.decode("utf-8")
    else:
        return obj


def get_player(kw):
    """
    根据关键字查找人物名字对应的ID，用于标记关键字
    :param kw:
    :return:
    """
    try:
        redis_client = RedisClient.get_client()
        data = redis_client.hget(settings.SHH_KW_HASH, kw)
        if data:
            return json.loads(recursive_unicode(data))
        else:
            return []
    except:
        settings.logger.exception("获取人物信息异常")
        return []


def get_week_period(datetime_str, format="%Y-%m-%d %H:%M"):
    """
    根据给定的时间，返回周的区间
    20180101-20180107
    :param datatime_str:
    :return:
    """
    dt = datetime.datetime.strptime(datetime_str, format)
    week_day = dt.weekday()
    week_start_date = dt - datetime.timedelta(days=week_day)
    week_end_date = dt + datetime.timedelta(days=6 - week_day)
    return f"{week_start_date.strftime('%Y%m%d')}-{week_end_date.strftime('%Y%m%d')}"


def get_month_period(datetime_str, format="%Y-%m-%d %H:%M"):
    """
    根据给定的时间，返回月的信息
    yyyymm
    :param datetime_str:
    :param format:
    :return:
    """
    return int(datetime.datetime.strptime(datetime_str, format).strftime("%Y%m"))
