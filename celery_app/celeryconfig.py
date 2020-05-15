# -*- coding: utf-8 -*-
import datetime

from celery.schedules import crontab

import settings

broker_url = settings.CELERY_BROKER_URL  # 指定 Broker
result_backend = settings.CELERY_RESULT_BACKEND  # 指定 Backend
result_expires = datetime.timedelta(hours=1)
timezone = 'Asia/Shanghai'  # 指定时区，默认是 UTC
task_serializer = 'json'  # 指定task序列化方式
result_serializer = 'json'  # 指定结果序列化方式
accept_content = ['json']  # 指定接收内容序列化方式

imports = (  # 指定导入的任务模块
    'celery_app.tasks.shh_task',
)

beat_schedule = {
    "shh_index": {
        'task': 'celery_app.tasks.shh_task.index_handler',
        'schedule': crontab(minute="*/6"),
    },
    "real_time_update_ranking": {  # 实时更新数据
        'task': 'celery_app.tasks.shh_task.real_time_update_ranking',
        'schedule': crontab(minute="*/10"),
    },
    "update_day_finally_ranking": {  # 每天 1点10分更新昨天数据
        'task': 'celery_app.tasks.shh_task.update_day_finally_ranking',
        'schedule': crontab(minute="10", hour="1"),
    },
    "update_week_finally_ranking": {  # 每周一 1点10分更新上周数据
        'task': 'celery_app.tasks.shh_task.update_week_finally_ranking',
        'schedule': crontab(minute="10", hour="1", day_of_week='1'),
    },
    "update_month_finally_ranking": {  # 每月1号 1点10分更新上月数据
        'task': 'celery_app.tasks.shh_task.update_month_finally_ranking',
        'schedule': crontab(minute="10", hour="1", day_of_month='1'),
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
