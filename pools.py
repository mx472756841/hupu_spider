import json
import re
import time
from concurrent.futures import ThreadPoolExecutor

import jieba
import jieba.analyse

from celery_app.tasks.shh_task import P_CONTENT
from hupu.community.base import get_article_list, get_commtents, get_article
from settings import logger, HUPU_DOWNLOAD_COOKIES_KEY
from tools.db import RedisClient, get_conn
from tools.utils import recursive_unicode, get_player, get_week_period, get_month_period


def download_article(article_id):
    # 插入数据库
    conn = get_conn()
    try:
        print(f"start spider article {article_id}")
        with conn.cursor() as cursor:
            sql = "select id from hupu_article where id = %s"
            cursor.execute(sql, article_id)
            article = cursor.fetchone()
            if article:
                print(f"文章{article_id}已经下载完成，退出")
                return
    finally:
        conn.close()
    max_times = 3
    while max_times:
        conn = None
        try:
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

                    for person in persons:
                        # 插入周榜信息
                        sql = """
                            INSERT INTO hupu_day_list(`day`, person_id, article_cnt)
                            VALUE(%s, %s, 1) ON DUPLICATE KEY UPDATE article_cnt = article_cnt + 1
                        """
                        cursor.execute(sql, [article.publish_date[:10], person])

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
                conn.commit()
            print(f"end spider article {article_id}")
            max_times = 0
        except:
            print(f"文章{article_id}下载失败，倒数{max_times}次,等待10s再下载")
            time.sleep(10)
            max_times = max_times - 1
        finally:
            if conn: conn.close()


def download_comment(article_id):
    """
    下载文章评论
    1. 一次性下载完成当前所有的评论，并记录到已下载到第几页的评论
    2. 15天之后的评论就不再下载
    :param article_id:
    :param page:
    :return:
    """
    total_times = 3
    while total_times:
        try:
            comments = get_commtents(article_id, 1)
            if comments:
                # 当前文章的总评论页数
                total_page = comments['total_page']
                # 当前页的文章评论信息
                current_comments = comments['current_comments']
                for comment in current_comments:
                    conn = get_conn()
                    try:
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
                                           [article_id, comment.id, comment.publish_date, comment.author,
                                            comment.author_id,
                                            comment.comment, comment.reply_comment, json.dumps(kws),
                                            json.dumps(persons)])

                            for person in persons:
                                # 插入日榜信息
                                sql = """
                                    INSERT INTO hupu_day_list(`day`, person_id, comment_cnt)
                                    VALUE(%s, %s, 1) ON DUPLICATE KEY UPDATE comment_cnt = comment_cnt + 1
                                """
                                cursor.execute(sql, [comment.publish_date[:10], person])

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
                        conn.commit()
                    except:
                        print(f"{comment}入库失败跳过，处理下一个")
                    finally:
                        conn.close()
                print(f"文章{article_id} 第1页 评论下载完成")
                # 下载剩余页数
                for real_page in range(2, total_page + 1):
                    max_times = 3
                    print(f"开始下载 文章{article_id} 第{real_page}页 评论")
                    while max_times:
                        try:
                            comments = get_commtents(article_id, 1)
                            current_comments = comments['current_comments']
                            for comment in current_comments:
                                conn = get_conn()
                                try:
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
                                            kws = jieba.analyse.extract_tags(",".join([reply_comment, comment_str]),
                                                                             topK=10)
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
                                                       [article_id, comment.id, comment.publish_date, comment.author,
                                                        comment.author_id,
                                                        comment.comment, comment.reply_comment, json.dumps(kws),
                                                        json.dumps(persons)])

                                        for person in persons:
                                            # 插入日榜信息
                                            sql = """
                                                        INSERT INTO hupu_day_list(`day`, person_id, comment_cnt)
                                                        VALUE(%s, %s, 1) ON DUPLICATE KEY UPDATE comment_cnt = comment_cnt + 1
                                                    """
                                            cursor.execute(sql, [comment.publish_date[:10], person])

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
                                    conn.commit()
                                except:
                                    print(f"文章{article_id} 评论{comment}插入数据库失败")
                                finally:
                                    conn.close()
                            max_times = 0
                        except:
                            print(f"下载文章{article_id} 第{real_page}页评论失败倒数{max_times}次 暂停5s再次请求")
                            time.sleep(5)
                            max_times = max_times - 1
                    print(f"文章{article_id} 第{real_page}页 评论下载完成")
            total_times = 0
        except:
            print(f"下载文章{article_id}评论第1页失败，等待5s在处理，倒数失败次数{total_times}")
            total_times = total_times - 1
            time.sleep(5)


# def spider(queue):
#     """
#     根据队列中的请求数据，实际爬取
#     :param queue:
#     :return:
#     """
#     while True:
#         try:
#             spider_info = queue.get()
#             if spider_info:
#                 spider_info = spider_info
#                 dtype = spider_info['type']
#                 data = spider_info['data']
#                 if dtype == 'article':
#                     download_article(data['article_id'])
#                     print(f"{threading.get_ident()} - 文章{data['article_id']}信息入库成功...")
#                 elif dtype == 'comment':
#                     download_comments(data['article_id'], data['page'])
#                     print(f"{threading.get_ident()} - 文章{data['article_id']} 第{data['page']}页信息入库成功...")
#                 else:
#                     print(f"{threading.get_ident()} - 不支持的类型{dtype}")
#             else:
#                 print(f"{threading.get_ident()} -任务结束")
#                 break
#         except:
#             traceback.print_exc()
#     # 保证每个进程都停掉
#     queue.put(None)


# def test():
#     from tools.db import RedisClient
#     from tools.utils import recursive_unicode
#     import json
#     client = RedisClient.get_client()
#     all_data = client.lrange("celelry", 0, -1)
#     print(len(all_data))
#     for line in all_data:
#         line = json.loads(recursive_unicode(line))
#         if line['headers']['task'] == "celery_app.tasks.shh_task.download_article":
#             print(line)


def index_handler():
    """
    下载4月1号-2019年的所有文章数据
    :return:
    """
    executor = ThreadPoolExecutor(4)
    min_article_id = 29629263
    try:
        client = RedisClient.get_client()
        # for page in range(500, 3390):
        for page in range(393, 3400):
            print(f"spider page {page} start ...")
            # 获取虎扑cookies，下载超过10页时就必须使用cookie，防止每次修改cookie时重启服务，将cookie存入缓存
            cookies = json.loads(recursive_unicode(client.get(HUPU_DOWNLOAD_COOKIES_KEY)))
            max_times = 3
            while max_times:
                try:
                    articles = get_article_list("vote", page, cookies)

                    for article in articles:
                        if int(article['article_id']) <= min_article_id:
                            # 文章ID小于则表示已经下载完成，退出循环
                            break
                        else:
                            # 记录任务下载文章内容和评论内容
                            is_pause = True
                            while is_pause:
                                if executor._work_queue.qsize() > 10000:
                                    print(f"队列过长{executor._work_queue.qsize()}，暂停2分钟再执行")
                                    time.sleep(60 * 2)
                                else:
                                    is_pause = False

                            print(f"开始下载文章{article['article_id']}")
                            executor.submit(download_article, article['article_id'])
                            print(f"开始下载文章的评论{article['article_id']}")
                            executor.submit(download_comment, article['article_id'])
                            # download_article(article['article_id'])
                    max_times = 0
                except:
                    logger.error("下载失败，等待1分钟再下载")
                    max_times = max_times - 1
                    time.sleep(60)
                print(f"spider page {page} end ...")
            if page % 20 == 0:
                print(f"暂停10分钟，等待处理，防止celery worker不足")
                time.sleep(60 * 10)

        # 全部执行完成之后，停止任务
        print("全部执行完成，等待关闭线程池`````")
        executor.shutdown()
    except:
        logger.exception("补充下载异常")


def test_index_handler():
    client = RedisClient.get_client()
    cookies = json.loads(recursive_unicode(client.get(HUPU_DOWNLOAD_COOKIES_KEY)))
    articles = get_article_list("vote", 1180, cookies)
    print(articles)


if __name__ == "__main__":
    index_handler()
