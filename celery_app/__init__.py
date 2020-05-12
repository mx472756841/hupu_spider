import json
import os

import jieba
import jieba.analyse
from celery import Celery

import settings
from tools.db import get_conn, RedisClient
from tools.kw2name import KW2NAME_MAPPING


def init_kw2name_cache():
    jieba.load_userdict(os.sep.join([settings.BASE_PATH, 'doc', 'shh_dict.txt']))
    jieba.analyse.set_idf_path(os.sep.join([settings.BASE_PATH, 'doc', 'kw_dict_gt_500.idf.txt']))
    # 初始化时，将关键字信息与人物名字绑定关系
    person2id = dict()
    kw2name_mapping = dict()
    with get_conn().cursor() as cursor:
        sql = """
            select name, id from person_info
        """
        cursor.execute(sql)
        persons = cursor.fetchall()

    for person in persons:
        person2id[person['name']] = person['id']

    for key, value in KW2NAME_MAPPING:
        values = json.loads(kw2name_mapping.get(key, json.dumps([])))
        if value not in person2id:
            print(key, value, "不存在的用户")
        else:
            values.append(person2id[value].upper())
        kw2name_mapping[key] = json.dumps(list(set(values)))

    if kw2name_mapping:
        client = RedisClient.get_client()
        client.hmset(settings.SHH_KW_HASH, kw2name_mapping)


app = Celery('hupu_spider')  # 创建 Celery 实例
app.config_from_object('celery_app.celeryconfig')  # 通过 Celery 实例加载配置模块
init_kw2name_cache()
