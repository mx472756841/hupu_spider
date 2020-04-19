import datetime
import json
import re

import jieba.analyse

from celery_app import app
from hupu.community.base import get_article, get_commtents, get_article_list
from settings import logger, ARTICLE_DOWNLOAD_COMMENT_PAGE, LAST_DOWNLOAD_ARTICLE_ID_KEY, HUPU_DOWNLOAD_COOKIES_KEY
from tools.db import get_conn, RedisClient
from tools.utils import get_player, get_month_period, get_week_period, recursive_unicode

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
                    download_article.apply_async(args=[article['article_id'], 1], priority=9)
                    download_comment.apply_async(args=[article['article_id'], 1], priority=9)

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
        with get_conn().cursor() as cursor:
            sql = "select id from hupu_article where id = %s"
            cursor.execute(sql, article_id)
            article = cursor.fetchone()
            if article:
                logger.warning(f"文章{article_id}已经下载完成，退出")
                return

        article = get_article(article_id)
        if article:
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

            with get_conn() as cursor:
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

                for person in persons:
                    # 插入周榜信息
                    sql = """
                        INSERT INTO hupu_week_list(week_info, person_id, article_cnt)
                        VALUE(%s, %s, 1) ON DUPLICATE KEY UPDATE article_cnt = article_cnt + 1
                    """
                    cursor.execute(sql, [week_period, person])

                    # 插入月榜信息
                    sql = """
                        INSERT INTO hupu_month_list(month_info, person_id, article_cnt)
                        VALUE(%s, %s, 1) ON DUPLICATE KEY UPDATE article_cnt = article_cnt + 1
                    """
                    cursor.execute(sql, [month_period, person])
    except:
        logger.exception(f"download shh article {article_id} error, fail times {times + 1}")
        if times < 3:
            # 最多不超过3次, 三分钟之后再次执行
            download_article.apply_async(args=[article_id, times + 1], countdown=60 * 3)
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
            with get_conn() as cursor:
                if page == 1 and current_comments:
                    first_datetime = current_comments[0].publish_date

                for comment in current_comments:
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

                    for person in persons:
                        # 插入周榜信息
                        sql = """
                            INSERT INTO hupu_week_list(week_info, person_id, comment_cnt)
                            VALUE(%s, %s, 1) ON DUPLICATE KEY UPDATE comment_cnt = comment_cnt + 1
                        """
                        cursor.execute(sql, [week_period, person])

                        # 插入月榜信息
                        sql = """
                            INSERT INTO hupu_month_list(month_info, person_id, comment_cnt)
                            VALUE(%s, %s, 1) ON DUPLICATE KEY UPDATE comment_cnt = comment_cnt + 1
                        """
                        cursor.execute(sql, [month_period, person])

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
                    download_comment.apply_async(args=[article_id, 1], countdown=60 * 10 * days)
                else:
                    # 一天执行一次
                    download_comment.apply_async(args=[article_id, 1], countdown=60 * 60 * 24)
            if not download_redis_info:
                # 设置过期时间为15天
                redis.expire(download_page_key, 15 * 24 * 60 * 60)
    except:
        logger.exception(f"download shh article {article_id} page {page} comment error, fail times {times + 1}")
        if times < 3:
            # 最多不超过3次, 三分钟之后再次执行
            download_comment.apply_async(args=[article_id, times + 1], countdown=60 * 3)
        else:
            logger.error(f"fail max times 3, article {article_id} page {page} comment fail times {times + 1} ")
