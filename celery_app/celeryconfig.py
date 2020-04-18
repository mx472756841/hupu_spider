# -*- coding: utf-8 -*-
from celery.schedules import crontab

import settings

BROKER_URL = settings.CELERY_BROKER_URL  # 指定 Broker
CELERY_RESULT_BACKEND = settings.CELERY_RESULT_BACKEND  # 指定 Backend
CELERY_TIMEZONE = 'Asia/Shanghai'  # 指定时区，默认是 UTC
CELERY_TASK_SERIALIZER = 'json'  # 指定task序列化方式
CELERY_RESULT_SERIALIZER = 'json'  # 指定结果序列化方式
CELERY_ACCEPT_CONTENT = ['json']  # 指定接收内容序列化方式

CELERY_IMPORTS = (  # 指定导入的任务模块
    'celery_app.tasks.shh_task',
)

CELERYBEAT_SCHEDULE = {
    "shh_index": {
        'task': 'celery_app.tasks.shh_task.index_handler',
        'schedule': crontab(minute="*/6"),
    },
}

# 更改任务的属性或者方法
# task_annotations = {'tasks.add': {'rate_limit': '10/s'}}
# 更改发送消息时,是否压缩
# task_compression =
# 储存结果,过期时间,默认1天,设置为0或者None则永不删除
# result_expires = 0
# 结果缓存,是否启用,启用设置为0,为1表示禁用,默认是不禁用的
# result_cache_max = 1

# 消息路由 Message Routing
# 任务路由设置,默认都是None
# task_queues
# task_routes
