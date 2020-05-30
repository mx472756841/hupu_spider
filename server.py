# -*- coding: utf-8 -*-
import asyncio
import datetime
import json

from apscheduler.triggers.date import DateTrigger

import celery_app.tasks.shh_task as shh_task
import settings
from common.scheduler import scheduler
from tools.db import RedisClient
from tools.utils import recursive_unicode


def delay_run_spider_task(func_name, args):
    if func_name == "download_article":
        shh_task.download_article.apply_async(args=args)
    elif func_name == "download_comment":
        shh_task.download_comment.apply_async(args=args)
    elif func_name == "download_author_profile":
        shh_task.download_author_profile.apply_async(args=args)
    else:
        settings.logger.error(f"暂时不支持的爬虫任务{func_name} args = {args}")


def scheduler_sipder_task():
    """
    因为celery的定时任务 countdown和eta 超过1小时会有2个问题
    1. 任务重复执行， 使用redis异常时【官方解释】 [https://docs.celeryproject.org/en/stable/getting-started/brokers/redis.html?highlight=Visibility%20timeout#configuration]
    2. 任务超过限制时 超过65535，出现任务丢失的现象【官方解释】[https://docs.celeryproject.org/en/stable/history/changelog-2.2.html?highlight=65535]
    :return:
    """
    settings.logger.info("scheduler_sipder_task start >>>> ")
    redis_client = RedisClient.get_client()
    while redis_client.llen(settings.CELERY_TO_APSCHEDULER_LIST) > 0:
        data = None
        try:
            data = redis_client.lpop(settings.CELERY_TO_APSCHEDULER_LIST)
            data = json.loads(recursive_unicode(data))
            func_name = data['func_name']
            args = data['args']
            task_id = data['task_id']
            execute_datetime = data['execute_datetime']
            if not scheduler.get_job(task_id):
                dt = datetime.datetime.strptime(execute_datetime, "%Y-%m-%d %H:%M:%S")
                trigger = DateTrigger(dt)
                scheduler.add_job(delay_run_spider_task, args=[func_name, args], id=task_id,
                                  trigger=trigger, misfire_grace_time=60)
            else:
                settings.logger.error(f"任务{task_id}已经存在，此次退出")
        except:
            settings.logger.exception("处理任务异常 {}".format(data))

    settings.logger.info("scheduler_sipder_task end >>>> ")


def scheduler_start():
    """
    启动后台调度进程
    :return:
    """
    settings.logger.info("start scheduler ...")
    scheduler.start()
    settings.logger.info("start scheduler ok ...")
    # 每隔60s，执行一次心跳
    if not scheduler.get_job('scheduler_sipder_task'):
        scheduler.add_job(scheduler_sipder_task, id='scheduler_sipder_task', trigger='interval', seconds=60,
                          misfire_grace_time=10)


def main():
    # 启动scheduler
    scheduler_start()
    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    main()
