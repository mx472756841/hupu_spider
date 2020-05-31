import copy
import datetime
import json
import re
import time

import jieba.analyse
import urllib3

import settings
from celery_app import app
from hupu.community.base import get_article, get_commtents, get_article_list, get_user_detail
from hupu.exceptions import CookieException, BaseException
from settings import logger, ARTICLE_DOWNLOAD_COMMENT_PAGE, LAST_DOWNLOAD_ARTICLE_ID_KEY, HUPU_DOWNLOAD_COOKIES_KEY
from tools.db import get_conn, RedisClient, MongoClient
from tools.utils import get_player, get_month_period, get_week_period, recursive_unicode

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 内容中有http连接及Twitter关键字时影响分词，此处做处理
P_CONTENT = [re.compile("[a-zA-z]+://[^\s]*"), re.compile("twitter", re.I)]


@app.task
def index_handler():
    """
    1. 记录每次第一次的第一个点，每次完成之后，更新这个点，每次以上次的点为结束退出
    :return:
    """
    page = 1
    is_delete_cache = False
    client = RedisClient.get_client()
    try:
        is_handler = client.getset("shh:index:handler", 1)
        if is_handler:
            logger.info("shh index handlering... ")
            return
        is_delete_cache = True

        last_times_max_article_id = int(client.get(LAST_DOWNLOAD_ARTICLE_ID_KEY))
        first_article_id = None
        while page:
            logger.info(f"spider page {page} start ...")
            # 获取虎扑cookies，下载超过10页时就必须使用cookie，防止每次修改cookie时重启服务，将cookie存入缓存
            cookies = json.loads(recursive_unicode(client.get(HUPU_DOWNLOAD_COOKIES_KEY)))
            articles = get_article_list("vote", page, cookies)
            # 第一页时，记录下来第一条的文章ID
            if page == 1:
                first_article_id = int(articles[0]['article_id'])

            for article in articles:
                if int(article['article_id']) <= last_times_max_article_id:
                    # 文章ID小于则表示已经下载完成，退出循环
                    break
                else:
                    logger.info(f"添加到任务队列文章和评论 {article['article_id']} ...")
                    # 记录任务下载文章内容和评论内容
                    download_article.apply_async(args=[article['article_id'], 1])
                    download_comment.apply_async(args=[article['article_id'], 1])

            logger.info(f"spider page {page} end ...")
            logger.info(f"first_article_id = {first_article_id}")
            # 如果最后一个文章ID大于上次下载的ID,则表示下载完成，记录本次下载的ID，同时结束本次处理
            if int(articles[-1]['article_id']) <= last_times_max_article_id:
                break
            else:
                page += 1
        if first_article_id and first_article_id > last_times_max_article_id:
            client.set(LAST_DOWNLOAD_ARTICLE_ID_KEY, first_article_id)
    except:
        logger.exception("首页下载异常")
    finally:
        if is_delete_cache:
            client.delete("shh:index:handler")


@app.task
def download_article(article_id, times):
    """
    下载湿乎乎文章信息
    :param article_id: 文章ID
    :param times: 下载次数 超过三次则停止下载
    :return:
    """
    try:
        logger.info(f"start spider article {article_id}")
        with get_conn().cursor() as cursor:
            sql = "select id from hupu_article where id = %s"
            cursor.execute(sql, article_id)
            article = cursor.fetchone()
            if article:
                logger.warning(f"文章{article_id}已经下载完成，退出")
                return

        article = get_article(article_id)
        if article:
            client = RedisClient.get_client()
            # 文件下载成功
            # 结巴分词解析content的关键词
            # 文本中有链接时，分词时会将部分关键词分出来，因此分词时要将文章中链接删除
            content = article.content
            for p in P_CONTENT:
                content = re.sub(p, "", content)

            title = article.title
            for p in P_CONTENT:
                title = re.sub(p, "", title)

            if article.title in article.content:
                kws = jieba.analyse.extract_tags(content, topK=10)
            else:
                kws = jieba.analyse.extract_tags(" ".join([title, content]), topK=10)

            setex_cache = []
            mongo_db = MongoClient.get_client()
            conn = get_conn()
            with conn.cursor() as cursor:
                # 获取关键字对应人物
                persons = []
                for kw in kws:
                    persons.extend(get_player(kw))
                if persons:
                    persons = list(set(persons))

                # 根据人物插入周榜数据
                week_period = get_week_period(article.publish_date)
                # 根据人物插入月榜数据
                month_period = get_month_period(article.publish_date)

                # 插入文章数据库
                sql = """
                    insert into hupu_article(id, title, publish_date, author, author_id, source, content, kws, persons)
                    values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, [
                    article.id, article.title, article.publish_date, article.author,
                    article.author_id, article.source, article.content, json.dumps(kws), json.dumps(persons)
                ])

                try:
                    mongo_db.hupu.article.insert_one({
                        "_id": article.id,
                        "title": article.title,
                        "publish_date": article.publish_date,
                        "author": article.author,
                        "author_id": article.author_id,
                        "content": article.content,
                        "kws": kws,
                        "persons": persons
                    })
                except:
                    logger.exception("插入mongo失败")

                # 不再用 DUPLICATE KEY UPDATE 方式更新，效率太低，该用redis缓存  日期+person_id是否存在，存在就更新，不存在就新增。同时处理防止重复处理
                for person in persons:
                    day_user_key = "is:insert:day:%s:person:%s" % (article.publish_date[:10], person)
                    if not client.exists(day_user_key):
                        # 插入周榜信息
                        sql = """
                            INSERT INTO hupu_day_list(`day`, person_id, article_cnt)
                            VALUE(%s, %s, 1) ON DUPLICATE KEY UPDATE article_cnt = article_cnt + 1
                        """
                        cursor.execute(sql, [article.publish_date[:10], person])
                        # 对于日期的，设置过期时间为36小时
                        # client.setex(day_user_key, 36 * 3600, 1)
                        setex_cache.append({"key": day_user_key, "time": 36 * 3600})
                    else:
                        sql = """
                            UPDATE hupu_day_list set article_cnt = article_cnt + 1 where day = %s and person_id = %s
                        """
                        cursor.execute(sql, [article.publish_date[:10], person])

                    week_user_key = "is:insert:week:%s:person:%s" % (week_period, person)
                    if not client.exists(week_user_key):
                        # 插入周榜信息
                        sql = """
                            INSERT INTO hupu_week_list(week_info, person_id, article_cnt)
                            VALUE(%s, %s, 1) ON DUPLICATE KEY UPDATE article_cnt = article_cnt + 1
                        """
                        cursor.execute(sql, [week_period, person])
                        # 对于周的，设置过期时间为8天
                        # client.setex(week_user_key, 8 * 24 * 3600, 1)
                        setex_cache.append({"key": week_user_key, "time": 8 * 24 * 3600})
                    else:
                        sql = """
                            UPDATE hupu_week_list set article_cnt = article_cnt + 1 where week_info = %s and person_id = %s
                        """
                        cursor.execute(sql, [week_period, person])

                    # 插入月榜信息
                    month_user_key = "is:insert:month:%s:person:%s" % (month_period, person)
                    if not client.exists(month_user_key):
                        sql = """
                            INSERT INTO hupu_month_list(month_info, person_id, article_cnt)
                            VALUE(%s, %s, 1) ON DUPLICATE KEY UPDATE article_cnt = article_cnt + 1
                        """
                        cursor.execute(sql, [month_period, person])
                        # 对于周的，设置过期时间为32天
                        # client.setex(month_user_key, 32 * 24 * 3600, 1)
                        setex_cache.append({"key": month_user_key, "time": 32 * 24 * 3600})
                    else:
                        sql = """
                            UPDATE hupu_month_list set article_cnt = article_cnt + 1 where month_info = %s and person_id = %s
                        """
                        cursor.execute(sql, [month_period, person])
            conn.commit()
            conn.close()
            # 下载作者信息
            download_author_profile.apply_async(args=[article.author_id, 1])
            for line in setex_cache:
                client.setex(line['key'], line['time'], 1)

        logger.info(f"end spider article {article_id}")
    except:
        logger.exception(f"download shh article {article_id} error, fail times {times + 1}")
        if times < 3:
            # 最多不超过3次, 三分钟之后再次执行
            # download_article.apply_async(args=[article_id, times + 1], countdown=60 * 3)
            redis_client = RedisClient.get_client()
            task_data = {
                "func_name": "download_article",
                "args": [article_id, times + 1],
                "task_id": f"download_article_{article_id}",
                "execute_datetime": (datetime.datetime.now() + datetime.timedelta(minutes=3)).strftime(
                    "%Y-%m-%d %H:%M:%S")
            }
            redis_client.rpush(settings.CELERY_TO_APSCHEDULER_LIST, json.dumps(task_data))
        else:
            logger.error(f"fail max times 3, article {article_id} fail times {times + 1} ")


@app.task
def download_comment(article_id, times):
    """
    下载文章评论
    1. 一次性下载完成当前所有的评论，并记录到已下载到第几页的评论
    2. 15天之后的评论就不再下载
    :param article_id:
    :param page:
    :return:
    """
    page = 1
    first_datetime = ""
    try:
        download_page_key = ARTICLE_DOWNLOAD_COMMENT_PAGE % article_id
        redis = RedisClient.get_client()
        download_redis_info = recursive_unicode(redis.hgetall(download_page_key))
        if download_redis_info:
            # 确定已经爬完的页数，如果是第一页，但是总页数不大于1页。则还是存一页
            page = int(download_redis_info.get("page"))
            first_datetime = str(download_redis_info.get("first_datetime"))

        comments = get_commtents(article_id, page)
        if comments:
            # 当前文章的总评论页数
            total_page = comments['total_page']
            # 当前页的文章评论信息
            current_comments = comments['current_comments']
            if page == 1 and current_comments:
                first_datetime = current_comments[0].publish_date

            mongo_db = MongoClient.get_client()
            for comment in current_comments:
                setex_cache = []
                conn = get_conn()
                with conn.cursor() as cursor:
                    sql = "select id from hupu_comment where article_id = %s and comment_id = %s"
                    cursor.execute(sql, [article_id, comment.id])
                    db_info = cursor.fetchone()
                    if db_info:
                        # 已插入数据库则跳过
                        continue

                    # 结巴分词解析content的关键词
                    # 文本中有链接时，分词时会将部分关键词分出来，因此分词时要将文章中链接删除
                    reply_comment = comment.reply_comment
                    if reply_comment:
                        for p in P_CONTENT:
                            reply_comment = re.sub(p, "", comment.reply_comment)

                    comment_str = comment.comment
                    for p in P_CONTENT:
                        comment_str = re.sub(p, "", comment_str)

                    if comment.reply_comment and "隐藏" not in comment.reply_comment:
                        kws = jieba.analyse.extract_tags(",".join([reply_comment, comment_str]), topK=10)
                    else:
                        kws = jieba.analyse.extract_tags(comment_str, topK=10)
                    # 获取关键字对应人物
                    persons = []
                    for kw in kws:
                        persons.extend(get_player(kw))
                    if persons:
                        persons = list(set(persons))

                    # 根据人物插入周榜数据
                    week_period = get_week_period(comment.publish_date)
                    # 根据人物插入月榜数据
                    month_period = get_month_period(comment.publish_date)

                    # 插入评论表
                    sql = """
                        insert into hupu_comment(article_id, comment_id, publish_date, author, author_id, comment, reply_comment, kws, persons)
                        value(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql,
                                   [article_id, comment.id, comment.publish_date, comment.author, comment.author_id,
                                    comment.comment, comment.reply_comment, json.dumps(kws), json.dumps(persons)])

                    local_comment_id = cursor.lastrowid

                    try:
                        mongo_db.hupu.comment.insert_one({
                            "_id": local_comment_id,
                            "article_id": article_id,
                            "comment_id": comment.id,
                            "publish_date": comment.publish_date,
                            "author": comment.author,
                            "author_id": comment.author_id,
                            "comment": comment.comment,
                            "reply_comment": comment.reply_comment,
                            "kws": kws,
                            "persons": persons
                        })
                    except:
                        logger.exception("插入mongo失败")

                    for person in persons:
                        day_user_key = "is:insert:day:%s:person:%s" % (comment.publish_date[:10], person)
                        if not redis.exists(day_user_key):
                            # 插入日榜信息
                            sql = """
                                INSERT INTO hupu_day_list(`day`, person_id, comment_cnt)
                                VALUE(%s, %s, 1) ON DUPLICATE KEY UPDATE comment_cnt = comment_cnt + 1
                            """
                            cursor.execute(sql, [comment.publish_date[:10], person])
                            # 对于日期的，设置过期时间为36小时
                            # redis.setex(day_user_key, 36 * 3600, 1)
                            setex_cache.append({"key": day_user_key, "time": 36 * 3600})
                        else:
                            sql = """
                                UPDATE hupu_day_list set comment_cnt = comment_cnt + 1 where day = %s and person_id = %s
                            """
                            cursor.execute(sql, [comment.publish_date[:10], person])

                        week_user_key = "is:insert:week:%s:person:%s" % (week_period, person)

                        if not redis.exists(week_user_key):
                            # 插入周榜信息
                            sql = """
                                INSERT INTO hupu_week_list(week_info, person_id, comment_cnt)
                                VALUE(%s, %s, 1) ON DUPLICATE KEY UPDATE comment_cnt = comment_cnt + 1
                            """
                            cursor.execute(sql, [week_period, person])
                            # 对于周的，设置过期时间为8天
                            # redis.setex(week_user_key, 8 * 24 * 3600, 1)
                            setex_cache.append({"key": week_user_key, "time": 8 * 24 * 3600})
                        else:
                            sql = """
                                UPDATE hupu_week_list set comment_cnt = comment_cnt + 1 where week_info = %s and person_id = %s
                            """
                            cursor.execute(sql, [week_period, person])

                        month_user_key = "is:insert:month:%s:person:%s" % (month_period, person)
                        if not redis.exists(month_user_key):
                            # 插入月榜信息
                            sql = """
                                INSERT INTO hupu_month_list(month_info, person_id, comment_cnt)
                                VALUE(%s, %s, 1) ON DUPLICATE KEY UPDATE comment_cnt = comment_cnt + 1
                            """
                            cursor.execute(sql, [month_period, person])
                            # 对于周的，设置过期时间为32天
                            # redis.setex(month_user_key, 32 * 24 * 3600, 1)
                            setex_cache.append({"key": month_user_key, "time": 32 * 24 * 3600})
                        else:
                            sql = """
                                UPDATE hupu_month_list set comment_cnt = comment_cnt + 1 where month_info = %s and person_id = %s
                            """
                            cursor.execute(sql, [month_period, person])

                        # v1.2.0 添加到缓存，用于后续用户投票处理
                        if person in settings.PERSONS_ID:
                            key = f"{person}-{local_comment_id}"
                            redis.sadd(settings.ALL_COMMENT_DIRECTION_SET, key)
                conn.commit()
                conn.close()

                # 下载作者信息
                download_author_profile.apply_async(args=[comment.author_id, 1])

                for line in setex_cache:
                    redis.setex(line['key'], line['time'], 1)
            # 第一次爬取且没有评论，设置第一次爬取时间作为first_time用于判定下次执行时间
            cache_data = {
                "page": page + 1 if total_page > page else page,
                "first_datetime": first_datetime or datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            }
            redis.hmset(download_page_key, cache_data)
            if total_page > page:
                # 总页数大于当前页数，说明还是有下一页，继续爬取
                download_comment.apply_async(args=[article_id, 1])
            else:
                # 根据first_time 计算下一次爬取时间
                today = datetime.datetime.today()
                first_datetime = datetime.datetime.strptime(cache_data['first_datetime'], "%Y-%m-%d %H:%M")
                if (today - first_datetime).days < 7:
                    # 7天内，10分钟执行一次
                    if (today - first_datetime).days < 1:
                        days = 1
                    else:
                        days = (today - first_datetime).days

                    # download_comment.apply_async(args=[article_id, 1], countdown=60 * 10 * days)
                    task_data = {
                        "func_name": "download_comment",
                        "args": [article_id, 1],
                        "task_id": f"download_comment_{article_id}",
                        "execute_datetime": (datetime.datetime.now() + datetime.timedelta(minutes=10 * days)).strftime(
                            "%Y-%m-%d %H:%M:%S")
                    }
                    redis.rpush(settings.CELERY_TO_APSCHEDULER_LIST, json.dumps(task_data))
                elif (today - first_datetime).days < 16:
                    # 一天执行一次
                    # download_comment.apply_async(args=[article_id, 1], countdown=60 * 60 * 24)

                    task_data = {
                        "func_name": "download_comment",
                        "args": [article_id, 1],
                        "task_id": f"download_comment_{article_id}",
                        "execute_datetime": (datetime.datetime.now() + datetime.timedelta(minutes=60 * 24)).strftime(
                            "%Y-%m-%d %H:%M:%S")
                    }
                    redis.rpush(settings.CELERY_TO_APSCHEDULER_LIST, json.dumps(task_data))
                else:
                    logger.info(f"超过{(today - first_datetime).days}天，不再执行任务")

            if not download_redis_info:
                # 设置过期时间为15天
                redis.expire(download_page_key, 15 * 24 * 60 * 60)
    except:
        logger.exception(f"download shh article {article_id} page {page} comment error, fail times {times + 1}")
        if times < 3:
            # 最多不超过3次, 三分钟之后再次执行
            # download_comment.apply_async(args=[article_id, times + 1], countdown=60 * 3)
            redis_client = RedisClient.get_client()
            task_data = {
                "func_name": "download_comment",
                "args": [article_id, times + 1],
                "task_id": f"download_comment_{article_id}",
                "execute_datetime": (datetime.datetime.now() + datetime.timedelta(minutes=3)).strftime(
                    "%Y-%m-%d %H:%M:%S")
            }
            redis_client.rpush(settings.CELERY_TO_APSCHEDULER_LIST, json.dumps(task_data))
        else:
            logger.error(f"fail max times 3, article {article_id} page {page} comment fail times {times + 1} ")


@app.task
def download_author_profile(author_id, times):
    """
    下载用户信息
    当日下载过用户信息后，今日不再下载
    :param author_id:
    :param times:
    :return:
    """
    try:
        redis = RedisClient.get_client()
        is_download_key = settings.IS_DOWNLOAD_AUTHOR_PROFILE % (datetime.datetime.now().strftime("%Y%m%d"), author_id)
        if redis.exists(is_download_key):
            logger.warning(f"用户{author_id}今日信息已下载，本次退出")
            return

        cookies = json.loads(recursive_unicode(redis.get(HUPU_DOWNLOAD_COOKIES_KEY)))
        if not cookies:
            # todo
            logger.error("需要先下载cookie信息")
        author_profile = get_user_detail(author_id, cookies or {})
        if author_profile:
            # 将下载内容放入mongodb
            mongo_db = MongoClient.get_client()
            mongo_db.hupu.author.update_one(
                {
                    "_id": author_id
                }, {
                    "$set":
                        {
                            "_id": author_id,
                            "author_name": author_profile.author_name,
                            "gener": author_profile.gener,
                            "level": author_profile.level,
                            "province": author_profile.province,
                            "city": author_profile.city,
                            "register_date": author_profile.register_date,
                        },
                },
                upsert=True
            )
            # 过期时间 次日3点
            now = datetime.datetime.now()
            del_datetime = datetime.datetime.strptime(
                "{} 03:00:00".format((now + datetime.timedelta(days=1)).strftime("%Y-%m-%d")), "%Y-%m-%d %H:%M:%S")
            redis.setex(is_download_key, del_datetime - now, 1)
    except CookieException as e:
        # todo 重新下载cookie信息
        logger.error("需要重新下载cookie信息")
    except BaseException as e:
        logger.error(f"下载失败:{e.error_info}")
    except:
        logger.exception(f"download hupu author {author_id}  error, fail times {times + 1}")
        if times < 3:
            # 最多不超过3次, 三分钟之后再次执行
            redis_client = RedisClient.get_client()
            task_data = {
                "func_name": "download_author_profile",
                "args": [author_id, times + 1],
                "task_id": f"download_author_profile_{author_id}",
                "execute_datetime": (datetime.datetime.now() + datetime.timedelta(minutes=3)).strftime(
                    "%Y-%m-%d %H:%M:%S")
            }
            redis_client.rpush(settings.CELERY_TO_APSCHEDULER_LIST, json.dumps(task_data))
        else:
            logger.error(f"fail max times 3,  hupu author {author_id} fail times {times + 1} ")


@app.task
def real_time_update_ranking():
    """
    更新榜单排名，更新每日，每周，每月实时排名顺位，会有延迟，主要是用于展示折线图
    :return:
    """
    is_delete_cache = False
    client = RedisClient.get_client()
    key = "real_time_update_ranking"
    is_handler = client.getset(key, 1)
    try:
        # 防止重复处理
        if is_handler:
            logger.info(f"real_time_update_ranking handlering... ")
            return
        is_delete_cache = True

        today = datetime.datetime.today().strftime("%Y-%m-%d")
        # 查询日期数据排名
        try:
            conn = get_conn()
            with conn.cursor() as cursor:
                sql = """
                SELECT
                    a.id
                FROM
                    hupu_day_list AS a 
                WHERE a.day = %s
                order by a.article_cnt * %s + a.comment_cnt desc
                """
                cursor.execute(sql, [today, settings.ARTICLE_TO_COMMENT])
                datas = cursor.fetchall()
            conn.close()

            if datas:
                last_update_time = int(time.time())
                for idx, data in enumerate(datas):
                    max_fail_times = 3
                    while max_fail_times:
                        try:
                            conn = get_conn()
                            with conn.cursor() as cursor:
                                sql = "update hupu_day_list set ranking = %s, last_update_time = %s where id = %s"
                                cursor.execute(sql, [idx + 1, last_update_time, data['id']])
                            conn.commit()
                            max_fail_times = 0
                        except:
                            settings.logger.exception("更新失败，等待1s再更新")
                            time.sleep(1)
                            max_fail_times = max_fail_times - 1
                        finally:
                            conn.close()
        except:
            logger.exception("更新日期的失败")

        # 查询按照周数据排名
        try:
            week_str = get_week_period(today, "%Y-%m-%d")
            # 查询数据排名
            conn = get_conn()
            with conn.cursor() as cursor:
                sql = """
                            SELECT
                                a.id
                            FROM
                                hupu_week_list AS a 
                            WHERE a.week_info = %s
                            order by a.article_cnt * %s + a.comment_cnt desc
                        """
                cursor.execute(sql, [week_str, settings.ARTICLE_TO_COMMENT])
                datas = cursor.fetchall()
            conn.close()

            if datas:
                last_update_time = int(time.time())
                for idx, data in enumerate(datas):
                    max_fail_times = 3
                    while max_fail_times:
                        try:
                            conn = get_conn()
                            with conn.cursor() as cursor:
                                sql = "update hupu_week_list set ranking = %s, last_update_time = %s where id = %s"
                                cursor.execute(sql, [idx + 1, last_update_time, data['id']])
                            conn.commit()
                            max_fail_times = 0
                        except:
                            settings.logger.exception("更新失败，等待1s再更新")
                            time.sleep(1)
                            max_fail_times = max_fail_times - 1
                        finally:
                            conn.close()
        except:
            logger.exception("更新周数据失败")

        # 查询按照月数据排名
        try:
            month_str = get_month_period(today, "%Y-%m-%d")
            conn = get_conn()
            with conn.cursor() as cursor:
                sql = """
                    SELECT
                        a.id
                    FROM
                        hupu_month_list AS a 
                    WHERE a.month_info = %s
                    order by a.article_cnt * %s + a.comment_cnt desc
                """
                cursor.execute(sql, [month_str, settings.ARTICLE_TO_COMMENT])
                datas = cursor.fetchall()
            conn.close()

            if datas:
                last_update_time = int(time.time())
                for idx, data in enumerate(datas):
                    max_fail_times = 3
                    while max_fail_times:
                        try:
                            conn = get_conn()
                            with conn.cursor() as cursor:
                                sql = "update hupu_month_list set ranking = %s, last_update_time = %s where id = %s"
                                cursor.execute(sql, [idx + 1, last_update_time, data['id']])
                            conn.commit()
                            max_fail_times = 0
                        except:
                            settings.logger.exception("更新失败，等待1s再更新")
                            time.sleep(1)
                            max_fail_times = max_fail_times - 1
                        finally:
                            conn.close()
        except:
            logger.exception("更新月数据失败")

    finally:
        if is_delete_cache:
            client.delete(key)


@app.task
def update_day_finally_ranking(date_str=None):
    """
    更新每天球员的排名， 一般是于次日凌晨更新，如果不指定datetime就取上一天的排名
    :param datetime:
    :return:
    """
    if not date_str:
        date_str = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    is_delete_cache = False
    client = RedisClient.get_client()
    key = "update_day_finally_ranking:%s" % date_str
    is_handler = client.getset(key, 1)
    try:
        # 防止重复处理
        if is_handler:
            logger.info(f"update_day_finally_ranking {date_str} handlering... ")
            return
        is_delete_cache = True

        # 查询数据排名
        conn = get_conn()
        with conn.cursor() as cursor:
            sql = """
                SELECT
                    a.id
                FROM
                    hupu_day_list AS a 
                WHERE a.day = %s
                order by a.article_cnt * %s + a.comment_cnt desc
            """
            cursor.execute(sql, [date_str, settings.ARTICLE_TO_COMMENT])
            datas = cursor.fetchall()
        conn.close()

        if datas:
            last_update_time = int(time.time())
            for idx, data in enumerate(datas):
                max_fail_times = 3
                while max_fail_times:
                    try:
                        conn = get_conn()
                        with conn.cursor() as cursor:
                            sql = "update hupu_day_list set ranking = %s, last_update_time = %s where id = %s"
                            cursor.execute(sql, [idx + 1, last_update_time, data['id']])
                        conn.commit()
                        max_fail_times = 0
                    except:
                        settings.logger.exception("更新失败，等待1s再更新")
                        time.sleep(1)
                        max_fail_times = max_fail_times - 1
                    finally:
                        conn.close()
    finally:
        if is_delete_cache:
            client.delete(key)


@app.task
def update_week_finally_ranking(week_str=None):
    """
    更新每周球员的排名， 一般是于周一凌晨更新，如果不指定datetime就取上一天的所对应的周的信息
    :param datetime:
    :return:
    """
    if not week_str:
        week_str = get_week_period((datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
                                   "%Y-%m-%d")

    is_delete_cache = False
    client = RedisClient.get_client()
    key = "update_week_finally_ranking:%s" % week_str
    is_handler = client.getset(key, 1)
    try:
        # 防止重复处理
        if is_handler:
            logger.info(f"update_week_finally_ranking {week_str} handlering... ")
            return
        is_delete_cache = True

        # 查询数据排名
        conn = get_conn()
        with conn.cursor() as cursor:
            sql = """
                SELECT
                    a.id
                FROM
                    hupu_week_list AS a 
                WHERE a.week_info = %s
                order by a.article_cnt * %s + a.comment_cnt desc
            """
            cursor.execute(sql, [week_str, settings.ARTICLE_TO_COMMENT])
            datas = cursor.fetchall()
        conn.close()

        if datas:
            last_update_time = int(time.time())
            for idx, data in enumerate(datas):
                max_fail_times = 3
                while max_fail_times:
                    try:
                        conn = get_conn()
                        with conn.cursor() as cursor:
                            sql = "update hupu_week_list set ranking = %s, last_update_time = %s where id = %s"
                            cursor.execute(sql, [idx + 1, last_update_time, data['id']])
                        conn.commit()
                        max_fail_times = 0
                    except:
                        settings.logger.exception("更新失败，等待1s再更新")
                        time.sleep(1)
                        max_fail_times = max_fail_times - 1
                    finally:
                        conn.close()
    finally:
        if is_delete_cache:
            client.delete(key)


@app.task
def update_month_finally_ranking(month_str=None):
    """
    更新每月球员的排名， 一般是于1号凌晨更新，如果不指定datetime就取上一天的所对应的月的信息
    :param datetime:
    :return:
    """
    if not month_str:
        month_str = get_month_period((datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
                                     "%Y-%m-%d")

    is_delete_cache = False
    client = RedisClient.get_client()
    key = "update_month_finally_ranking:%s" % month_str
    is_handler = client.getset(key, 1)
    try:
        # 防止重复处理
        if is_handler:
            logger.info(f"update_month_finally_ranking {month_str} handlering... ")
            return
        is_delete_cache = True

        # 查询数据排名
        conn = get_conn()
        with conn.cursor() as cursor:
            sql = """
                SELECT
                    a.id
                FROM
                    hupu_month_list AS a 
                WHERE a.month_info = %s
                order by a.article_cnt * %s + a.comment_cnt desc
            """
            cursor.execute(sql, [month_str, settings.ARTICLE_TO_COMMENT])
            datas = cursor.fetchall()
        conn.close()

        if datas:
            last_update_time = int(time.time())
            for idx, data in enumerate(datas):
                max_fail_times = 3
                while max_fail_times:
                    try:
                        conn = get_conn()
                        with conn.cursor() as cursor:
                            sql = "update hupu_month_list set ranking = %s, last_update_time = %s where id = %s"
                            cursor.execute(sql, [idx + 1, last_update_time, data['id']])
                        conn.commit()
                        max_fail_times = 0
                    except:
                        settings.logger.exception("更新失败，等待1s再更新")
                        time.sleep(1)
                        max_fail_times = max_fail_times - 1
                    finally:
                        conn.close()
    finally:
        if is_delete_cache:
            client.delete(key)


@app.task
def update_every_day_shh_report():
    """
    更新每天湿乎乎用户报告
    1. 根据文章和评论的最后一次更新， 查询之后的，按照日期归档存储
    2. 将更新数据放到mongodb
    3. 数据结构为
    {
        "_id":"20200503",
        "total_article": 100,
        "total_comment": 100,
        "total_user_cnt": 100,
        "sex": {  固定三项
            "男": 40,
            "女":10,
            "保密": 50
        },
        "regtion": { 多项
            "山东省": 100,
            "北京市": 100,
            "上海市":100
        },
        "days":{ # 固定五项
            "0-10天": 10,
            "10-30天": 10,
            "半年以内": 10,
            "1年以内": 10,
            "1年以上": 10,
        },
        "level": { # 固定7项
            "0-5级": 10,
            "5-10级": 10,
            "10-30级": 10,
            "30-60级": 10,
            "60-90级": 10,
            "90-120级": 10,
            "120级以上": 10
        }
    }
    :return:
    """
    is_delete_cache = False
    client = RedisClient.get_client()
    key = "update_every_day_shh_report:cache"
    is_handler = client.getset(key, 1)
    try:
        # 防止重复处理
        if is_handler:
            logger.info(f"update_every_day_shh_report handlering... ")
            return
        is_delete_cache = True
        mongo_db = MongoClient.get_client()
        # 查询缓存key，当前已经处理到的key，如果没有则从当日开始
        hanlder_cache = "update_every_day_shh_report:hash:key"
        data = recursive_unicode(client.hgetall(hanlder_cache))
        last_article_id = data.get('article_id')
        last_comment_id = data.get('comment_id')
        # 初始化结构数据
        init_data = {
            "total_article": 0,
            "total_comment": 0,
            "total_user_cnt": 0,
            "gener.男": 0,
            "gener.女": 0,
            "gener.保密": 0,
            "days.0-30天": 0,
            "days.30-180天": 0,
            "days.1年以内": 0,
            "days.1-3年": 0,
            "days.3-5年": 0,
            "days.5-10年": 0,
            "days.10年以上": 0,
            "level.0-5": 0,
            "level.5-10": 0,
            "level.10-30": 0,
            "level.30-60": 0,
            "level.60-90": 0,
            "level.90以上": 0
        }
        if last_article_id:
            # 查询大于最后一次处理的文章ID
            sql = """
                select author_id, publish_date, id
                from hupu_article 
                where id > %s order by id asc 
            """
            execute_data = [last_article_id]
        else:
            # 查询今日的文章id。按照id倒序排序
            sql = """
                select author_id, publish_date, id
                from hupu_article 
                where publish_date > %s order by id asc 
            """
            execute_data = [datetime.datetime.now().strftime("%Y-%m-%d 00:00:00")]
        # 处理文章
        try:
            conn = get_conn()
            with conn.cursor() as cursor:
                cursor.execute(sql, execute_data)
                all_article_data = cursor.fetchall()
            conn.close()
            logger.info(f"本次共有{len(all_article_data)}篇文章的作者信息待处理")
            # 最后处理的文章ID
            this_last_article_id = ""
            # 日期为key 后续为可直接更新的数据
            update_article_data = {}
            for article_info in all_article_data:
                date = article_info['publish_date'].strftime("%Y-%m-%d")
                update_data = update_article_data.get(date, copy.deepcopy(init_data))
                # 查询指定用户的信息，如果用户已经处理过的则不处理，只增加total_article
                is_handler = settings.BEEN_HANDLER_AUTHOR_SET % date
                is_temp_handler = settings.TMP_BEEN_HANDLER_AUTHOR_SET % date
                if client.sismember(is_handler, article_info['author_id']) or \
                        client.sismember(is_temp_handler, article_info['author_id']):
                    # 用户已经统计，或者已经在临时中，则只增加文章数
                    update_data['total_article'] += 1
                else:
                    # 查询用户类型 年龄，地域，级数，地域
                    author_info = recursive_unicode(mongo_db.hupu.author.find_one({"_id": article_info['author_id']}))
                    if not author_info:
                        if (datetime.datetime.now() - article_info['publish_date']).seconds > 20 * 60:
                            # 超过20分钟，还没有用户的信息，说明用户数是0级，虎扑还没有数据，直接跳过
                            author_info = {}
                        else:
                            last_stop = recursive_unicode(
                                client.hget(settings.STOP_ARTICLE_REPORT_AUTHOR, article_info['author_id']))
                            if last_stop:
                                if int(last_stop) == 2:
                                    logger.error(f"已经连续三次停止在用户{article_info['author_id']}，跳过此用户，请注意查看!!!!")
                                    # 删除缓存
                                    client.delete(settings.STOP_ARTICLE_REPORT_AUTHOR)
                                    author_info = {}
                                else:
                                    client.hincrby(settings.STOP_ARTICLE_REPORT_AUTHOR, article_info['author_id'])
                                    break
                            else:
                                client.delete(settings.STOP_ARTICLE_REPORT_AUTHOR)
                                client.hincrby(settings.STOP_ARTICLE_REPORT_AUTHOR, article_info['author_id'])
                                break

                    # 计算报告数据
                    gener = author_info.get('gener')
                    level = author_info.get('level', 0)
                    province = author_info.get("province")
                    register_date = author_info.get("register_date")
                    if gener and gener in ("保密", "男", "女"):
                        update_data[f'gener.{gener}'] += 1
                    if level:
                        if level <= 5:
                            update_data['level.0-5'] += 1
                        elif level <= 10:
                            update_data['level.5-10'] += 1
                        elif level <= 30:
                            update_data['level.10-30'] += 1
                        elif level <= 60:
                            update_data['level.30-60'] += 1
                        elif level <= 90:
                            update_data['level.60-90'] += 1
                        else:
                            update_data['level.90以上'] += 1
                    if province:
                        province_cnt = update_data.get(f'regtion.{province}', 0)
                        province_cnt += 1
                        update_data[f'regtion.{province}'] = province_cnt
                    if register_date:
                        try:
                            register_datetime = datetime.datetime.strptime(register_date, "%Y-%m-%d")
                            register_day = (article_info['publish_date'] - register_datetime).days
                            if register_day <= 30:
                                update_data['days.0-30天'] += 1
                            elif register_day <= 180:
                                update_data['days.30-180天'] += 1
                            elif register_day <= 365:
                                update_data['days.1年以内'] += 1
                            elif register_day <= 1095:
                                update_data['days.1-3年'] += 1
                            elif register_day <= 1826:
                                update_data['days.3-5年'] += 1
                            elif register_day <= 3652:
                                update_data['days.5-10年'] += 1
                            else:
                                update_data['days.10年以上'] += 1
                        except:
                            logger.exception("处理日期出错")
                    update_data['total_user_cnt'] += 1
                    # 将指定用户添加到临时key，防止多次计入 实际更新完之后，再将所有缓存数据删除
                    client.sadd(is_temp_handler, article_info['author_id'])
                    update_data['total_article'] += 1
                update_article_data[date] = update_data
                # 记录最后一次的文章id以及卡在的用户id
                this_last_article_id = article_info['id']

            # 有最后处理的文章ID，表示有数据处理
            if update_article_data:
                for k, value in update_article_data.items():
                    mongo_db.hupu.shh_report.update_one(
                        {
                            "_id": k
                        }, {
                            "$inc": value,
                        },
                        upsert=True
                    )
                    is_handler = settings.BEEN_HANDLER_AUTHOR_SET % k
                    is_temp_handler = settings.TMP_BEEN_HANDLER_AUTHOR_SET % k
                    # 添加到缓存的已经处理过的日期的用户KEY
                    authors = client.smembers(is_temp_handler)
                    if authors:
                        authors = recursive_unicode(list(authors))
                        client.sadd(is_handler, *authors)
                        # 删除添加到缓存的临时 日期
                        client.delete(is_temp_handler)
                # 更新最后处理的文章ID
                client.hset(hanlder_cache, 'article_id', this_last_article_id)
        except:
            logger.exception("处理文章失败，下次启动时，继续处理")

        # 处理评论
        if last_comment_id:
            # 查询大于最后一次处理的文章ID
            sql = """
                select author_id, publish_date, id
                from hupu_comment
                where id > %s order by id asc 
            """
            execute_data = [last_comment_id]
        else:
            # 查询今日的文章id。按照id倒序排序
            sql = """
                select author_id, publish_date, id
                from hupu_comment 
                where publish_date > %s order by id asc 
            """
            execute_data = [datetime.datetime.now().strftime("%Y-%m-%d 00:00:00")]
        try:

            conn = get_conn()
            with conn.cursor() as cursor:
                cursor.execute(sql, execute_data)
                all_comment_data = cursor.fetchall()
            conn.close()
            logger.info(f"本次共有{len(all_comment_data)}篇评论的作者信息待处理")
            # 最后处理的评论ID
            this_last_comment_id = ""
            # 日期为key 后续为可直接更新的数据
            update_comment_data = {}
            for comment_info in all_comment_data:
                date = comment_info['publish_date'].strftime("%Y-%m-%d")
                update_data = update_comment_data.get(date, copy.deepcopy(init_data))
                # 查询指定用户的信息，如果用户已经处理过的则不处理，只增加total_article
                is_handler = settings.BEEN_HANDLER_AUTHOR_SET % date
                is_temp_handler = settings.TMP_BEEN_HANDLER_AUTHOR_SET % date
                if client.sismember(is_handler, comment_info['author_id']) or \
                        client.sismember(is_temp_handler, comment_info['author_id']):
                    # 用户已经统计，或者已经在临时中，则只增加文章数
                    update_data['total_comment'] += 1
                else:
                    # 查询用户类型 年龄，地域，级数，地域
                    author_info = recursive_unicode(mongo_db.hupu.author.find_one({"_id": comment_info['author_id']}))
                    if not author_info:
                        if (datetime.datetime.now() - comment_info['publish_date']).seconds > 20 * 60:
                            # 超过20分钟，还没有用户的信息，说明用户数是0级，虎扑还没有数据，直接跳过
                            author_info = {}
                        else:
                            last_stop = recursive_unicode(
                                client.hget(settings.STOP_ARTICLE_REPORT_AUTHOR, comment_info['author_id']))
                            if last_stop:
                                if int(last_stop) == 2:
                                    logger.error(f"已经连续三次停止在用户{comment_info['author_id']}，跳过此用户，请注意查看!!!!")
                                    # 删除缓存
                                    client.delete(settings.STOP_ARTICLE_REPORT_AUTHOR)
                                    author_info = {}
                                else:
                                    client.hincrby(settings.STOP_ARTICLE_REPORT_AUTHOR, comment_info['author_id'])
                                    break
                            else:
                                client.delete(settings.STOP_ARTICLE_REPORT_AUTHOR)
                                client.hincrby(settings.STOP_ARTICLE_REPORT_AUTHOR, comment_info['author_id'])
                                break

                    # 计算报告数据
                    gener = author_info.get('gener')
                    level = author_info.get('level')
                    province = author_info.get("province")
                    register_date = author_info.get("register_date")
                    if gener and gener in ("保密", "男", "女"):
                        update_data[f'gener.{gener}'] += 1
                    if level:
                        if level <= 5:
                            update_data['level.0-5'] += 1
                        elif level <= 10:
                            update_data['level.5-10'] += 1
                        elif level <= 30:
                            update_data['level.10-30'] += 1
                        elif level <= 60:
                            update_data['level.30-60'] += 1
                        elif level <= 90:
                            update_data['level.60-90'] += 1
                        else:
                            update_data['level.90以上'] += 1
                    if province:
                        province_cnt = update_data.get(f'regtion.{province}', 0)
                        province_cnt += 1
                        update_data[f'regtion.{province}'] = province_cnt
                    if register_date:
                        try:
                            register_datetime = datetime.datetime.strptime(register_date, "%Y-%m-%d")
                            register_day = (comment_info['publish_date'] - register_datetime).days
                            if register_day <= 30:
                                update_data['days.0-30天'] += 1
                            elif register_day <= 180:
                                update_data['days.30-180天'] += 1
                            elif register_day <= 365:
                                update_data['days.1年以内'] += 1
                            elif register_day <= 1095:
                                update_data['days.1-3年'] += 1
                            elif register_day <= 1826:
                                update_data['days.3-5年'] += 1
                            elif register_day <= 3652:
                                update_data['days.5-10年'] += 1
                            else:
                                update_data['days.10年以上'] += 1
                        except:
                            logger.exception("处理日期出错")
                    # 将指定用户添加到临时key，防止多次计入 实际更新完之后，再将所有缓存数据删除
                    update_data['total_user_cnt'] += 1
                    client.sadd(is_temp_handler, comment_info['author_id'])
                    update_data['total_comment'] += 1
                update_comment_data[date] = update_data
                # 记录最后一次的文章id以及卡在的用户id
                this_last_comment_id = comment_info['id']

            # 有最后处理的文章ID，表示有数据处理
            if update_comment_data:
                for k, value in update_comment_data.items():
                    mongo_db.hupu.shh_report.update_one(
                        {
                            "_id": k
                        }, {
                            "$inc": value,
                        },
                        upsert=True
                    )
                    is_handler = settings.BEEN_HANDLER_AUTHOR_SET % k
                    is_temp_handler = settings.TMP_BEEN_HANDLER_AUTHOR_SET % k
                    # 添加到缓存的已经处理过的日期的用户KEY
                    authors = client.smembers(is_temp_handler)
                    if authors:
                        authors = recursive_unicode(list(authors))
                        client.sadd(is_handler, *authors)
                        # 删除添加到缓存的临时 日期
                        client.delete(is_temp_handler)
                # 更新最后处理的文章ID
                client.hset(hanlder_cache, 'comment_id', this_last_comment_id)
        except:
            logger.exception("处理评论失败，下次启动时，继续处理")
    finally:
        if is_delete_cache:
            client.delete(key)


if __name__ == "__main__":
    # download_author_profile(158592523874574, 1)  # 所在地是澳洲
    # download_author_profile(20301818386678, 1)  # 所在地是陕西省榆林市
    # download_author_profile(51768945318600, 1)  # 所在地是湖南省长沙市
    # download_author_profile(233475226037282, 1)  # 所在地是广东省 广州市
    # download_author_profile(204148885683605, 1)  # 所在地是浙江省台州市
    # download_author_profile(165934265824606, 1)  # 所在地是上海市浦东新区
    # download_author_profile(236419949250105, 1)  # 所在地是山东省 青岛市
    # download_author_profile(82935735042486, 1)  # 所在地是北京市海淀区
    # download_author_profile(280923583165420, 1)  # 保密 所在地是上海市
    # download_author_profile(205226121481456, 1)  # 所在地是null
    # download_author_profile(35512074689389, 1)  # 保密
    # download_author_profile(238467143452731, 1)  # 男 所在地是没有
    # download_author_profile(30193012086352, 1)  # 女 所在地是上海市浦东新区
    update_every_day_shh_report()
    # mongo_db = MongoClient.get_client()
    # datas = mongo_db.hupu.author.find({})
    # with open("author.csv", "a") as f:
    #     for author in datas:
    #         f.write(f"{author.get('province')}\n")
    # for author in datas:
    #     download_author_profile(author['_id'], 1)
