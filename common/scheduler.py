#!/usr/bin/python3
# -*- coding: utf-8
""" 
@author: mengx@funsun.cn 
@file: scheduler.py
@time: 2019/2/18 15:58
"""

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED, EVENT_JOB_MAX_INSTANCES
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import settings

# 信息放在持久化的平台中，防止数据丢失
redis_jobstore = RedisJobStore(db=settings.REDIS_DB,
                               jobs_key="apschedulers.jobs",
                               run_times_key="apschedulers.run_times",
                               host=settings.REDIS_HOST,
                               port=settings.REDIS_PORT,
                               password=settings.REDIS_PASS
                               )
scheduler = AsyncIOScheduler()
scheduler.add_jobstore(redis_jobstore)
print("xxxxxxx")


def job_unusual(event):
    """
    监听事件处理
    :param event:
    :return:
    """
    settings.logger.error(
        "job执行异常:\n code => {}\n job.id => {}\n job.exception => {}\n job.traceback => {}\n jobstore=>{}".format(
            event.code,
            event.job_id,
            event.exception if hasattr(event, 'exception') else "",
            event.traceback if hasattr(event, 'traceback') else "",
            event.jobstore
        ))


scheduler.add_listener(job_unusual, EVENT_JOB_ERROR | EVENT_JOB_MISSED | EVENT_JOB_MAX_INSTANCES)
