import json
import time

from celery_app.tasks.shh_task import download_article, download_comment
from hupu.community.base import get_article_list
from settings import logger, HUPU_DOWNLOAD_COOKIES_KEY
from tools.db import RedisClient
from tools.utils import recursive_unicode


#
# def download_comments(article_id, page):
#     comments = get_commtents(article_id, page)
#     # 插入数据库
#     # Connect to the database
#     connection = pymysql.connect(host='115.159.119.204',
#                                  user='root',
#                                  password='BnakQkfF2sf1',
#                                  db='hupu',
#                                  port=10020,
#                                  charset='utf8mb4',
#                                  cursorclass=pymysql.cursors.DictCursor)
#
#     try:
#         with connection.cursor() as cursor:
#             for comment in comments:
#                 # Create a new record
#                 sql = "select id from hupu_comment where id = %s and article_id = %s"
#                 cursor.execute(sql, [comment.id, article_id])
#                 fetch_data = cursor.fetchone()
#                 if not fetch_data:
#                     # 结巴分词解析content的关键词
#                     if comment.reply_comment and "隐藏" not in comment.reply_comment:
#                         kwords = jieba.cut(",".join([comment.reply_comment, comment.comment]))
#                     else:
#                         kwords = jieba.cut(comment.comment)
#
#                     # 排除纯数字，纯字符
#                     finally_kwords = filter(lambda x: not x.isdigit() and len(x) > 1, kwords)
#                     finally_kwords_str = " ".join(finally_kwords)
#
#                     # 本地尚未入库，则插入数据库
#                     sql = """
#                         insert into hupu_comment(id, article_id, publish_date, author, author_id, comment, reply_comment, keywards)
#                         values(%s, %s, %s, %s, %s, %s, %s, %s)
#                     """
#                     cursor.execute(sql, [
#                         comment.id, article_id, comment.publish_date, comment.author,
#                         comment.author_id, comment.comment, comment.reply_comment, finally_kwords_str
#                     ])
#
#         # connection is not autocommit by default. So you must commit to save
#         # your changes.
#         connection.commit()
#     finally:
#         connection.close()
#
#
# def download_article(article_id):
#     # 插入数据库
#     article = get_article(article_id)
#     if article:
#         # Connect to the database
#         connection = pymysql.connect(host='115.159.119.204',
#                                      user='root',
#                                      password='BnakQkfF2sf1',
#                                      db='hupu',
#                                      port=10020,
#                                      charset='utf8mb4',
#                                      cursorclass=pymysql.cursors.DictCursor)
#
#         try:
#             with connection.cursor() as cursor:
#                 # Create a new record
#                 sql = "select id from hupu_article where id = %s"
#                 cursor.execute(sql, article_id)
#                 fetch_data = cursor.fetchone()
#                 if not fetch_data:
#
#                     # 结巴分词解析content的关键词
#                     if article.title in article.content:
#                         kwords = jieba.cut(article.content)
#                     else:
#                         kwords = jieba.cut(" ".join([article.title, article.content]))
#
#                     # 排除纯数字，纯字符
#                     finally_kwords = filter(lambda x: not x.isdigit() and len(x) > 1, kwords)
#                     finally_kwords_str = " ".join(finally_kwords)
#
#                     # 本地尚未入库，则插入数据库
#                     sql = """
#                         insert into hupu_article(id, title, publish_date, author, author_id, source, content, keywards)
#                         values(%s, %s, %s, %s, %s, %s, %s, %s)
#                     """
#                     cursor.execute(sql, [
#                         article.id, article.title, article.publish_date, article.author,
#                         article.author_id, article.source, article.content, finally_kwords_str
#                     ])
#
#             # connection is not autocommit by default. So you must commit to save
#             # your changes.
#             connection.commit()
#         finally:
#             connection.close()


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
    min_article_id = 29629263
    try:
        client = RedisClient.get_client()
        for page in range(373, 3390):
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
                            logger.info(f"添加到任务队列文章和评论 {article['article_id']} ...")
                            # 记录任务下载文章内容和评论内容
                            download_article.apply_async(args=[article['article_id'], 1])
                            download_comment.apply_async(args=[article['article_id'], 1])
                    break
                except:
                    logger.error("下载失败，等待1分钟再下载")
                    max_times = max_times - 1
                    time.sleep(60)
                print(f"spider page {page} end ...")
            if page % 40 == 0:
                print(f"暂停10分钟，等待处理，防止celery worker不足")
                time.sleep(60 * 10)
    except:
        logger.exception("补充下载异常")


def test_index_handler():
    client = RedisClient.get_client()
    cookies = json.loads(recursive_unicode(client.get(HUPU_DOWNLOAD_COOKIES_KEY)))
    articles = get_article_list("vote", 1180, cookies)
    print(articles)


if __name__ == "__main__":
    test_index_handler()
