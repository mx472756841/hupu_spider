#!/usr/bin/python3
# -*- coding: utf-8
""" 
@author: mengx@funsun.cn 
@file: scheduler.py
@time: 2019/2/18 15:58
"""
import datetime
import json
import random

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED, EVENT_JOB_MAX_INSTANCES
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import settings

# 信息放在持久化的平台中，防止数据丢失
from tools.db import RedisClient

redis_jobstore = RedisJobStore(db=settings.REDIS_DB,
                               jobs_key="apschedulers.jobs",
                               run_times_key="apschedulers.run_times",
                               host=settings.REDIS_HOST,
                               port=settings.REDIS_PORT,
                               password=settings.REDIS_PASS
                               )
scheduler = AsyncIOScheduler()
scheduler.add_jobstore(redis_jobstore)


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
    if event.code == EVENT_JOB_MISSED:
        # 如果是过期，就在当前时间再加5分钟后执行
        client = RedisClient.get_client()
        if event.job_id.startswith("download_comment_"):
            seconds = random.randint(60, 9000)
            settings.logger.info(f"任务{event.job_id}过期未执行，将在当前时间{seconds}秒钟后再次执行")
            task_data = {
                "func_name": "download_comment",
                "args": [int(event.job_id.split("_")[-1]), 1],
                "task_id": event.job_id,
                "execute_datetime": (datetime.datetime.now() + datetime.timedelta(seconds=seconds)).strftime(
                    "%Y-%m-%d %H:%M:%S")
            }
            client.rpush(settings.CELERY_TO_APSCHEDULER_LIST, json.dumps(task_data))


scheduler.add_listener(job_unusual, EVENT_JOB_ERROR | EVENT_JOB_MISSED | EVENT_JOB_MAX_INSTANCES)
