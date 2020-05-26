import pymongo
import pymysql
import redis
from pymysql.cursors import DictCursor

import settings


class RedisClient(object):
    _client = None

    def __init__(self):
        if self._client is None:
            self._create_redis_client()

    @classmethod
    def _create_redis_client(cls):
        """
        创建连接
        :return:
        """
        # not to use the connection pooling when using the redis-py client in Tornado applications
        # http://stackoverflow.com/questions/5953786/how-do-you-properly-query-redis-from-tornado/15596969#15596969
        # 注意这里必须是 settings.REDIS_HOST
        # 否则在 runserver 中若修改了 settings.REDIS_HOST，这里就不能生效
        RedisClient._client = redis.StrictRedis(
            host=settings.REDIS_HOST, port=settings.REDIS_PORT,
            db=settings.REDIS_DB, password=settings.REDIS_PASS)

    @classmethod
    def get_client(cls):
        if RedisClient._client is None:
            cls._create_redis_client()
        return RedisClient._client


def get_conn():
    """
    获取数据库连接
    :return:
    """
    connection = pymysql.connect(host=settings.MYSQL_DB_HOST,
                                 user=settings.MYSQL_DB_USER,
                                 password=settings.MYSQL_DB_PASSWORD,
                                 db=settings.MYSQL_DB_DBNAME,
                                 charset='utf8mb4',
                                 port=settings.MYSQL_DB_PORT,
                                 cursorclass=DictCursor)
    return connection


class MongoClient(object):
    _client = None

    def __init__(self):
        if self._client is None:
            self._create_mongo_client()

    @classmethod
    def _create_mongo_client(cls):
        """
        创建连接
        :return:
        """
        MongoClient._client = pymongo.mongo_client.MongoClient(
            host=settings.MONGO_HOST, port=settings.MONGO_PORT,
            username=settings.MONGO_USER, password=settings.MONGO_PASS,
            authSource=settings.MONGO_DB)

    @classmethod
    def get_client(cls):
        if MongoClient._client is None:
            cls._create_mongo_client()
        return MongoClient._client

if __name__ == "__main__":
    mongodb = MongoClient.get_client()
    print(mongodb)