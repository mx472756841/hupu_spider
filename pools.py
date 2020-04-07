import traceback

import jieba
import pymysql.cursors

from hupu.community.base import get_commtents, get_article


def download_comments(article_id, page):
    for comment in get_commtents(article_id, page):
        # 插入数据库
        # Connect to the database
        connection = pymysql.connect(host='115.159.119.204',
                                     user='root',
                                     password='BnakQkfF2sf1',
                                     db='hupu',
                                     port=10020,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "select id from hupu_comment where id = %s"
                cursor.execute(sql, comment.id)
                fetch_data = cursor.fetchone()
                if not fetch_data:
                    # 本地尚未入库，则插入数据库
                    sql = """
                        insert into hupu_comment(id, article_id, publish_date, author, author_id, comment, reply_comment)
                        values(%s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql, [
                        comment.id, article_id, comment.publish_date, comment.author,
                        comment.author_id, comment.comment, comment.reply_comment
                    ])

                    # 结巴分词解析content的关键词
                    if comment.reply_comment and "隐藏" not in comment.reply_comment:
                        kwords = jieba.cut(",".join([comment.reply_comment, comment.comment]))
                    else:
                        kwords = jieba.cut(comment.comment)

                    # 排除纯数字，纯字符
                    finally_kwords = filter(lambda x: not x.isdigit() and len(x) > 1, kwords)
                    for kword in finally_kwords:
                        sql = """
                            insert into hupu_keywards(keyword) value(%s)
                        """
                        cursor.execute(sql, kword)

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            connection.commit()
        finally:
            connection.close()


def download_article(article_id):
    # 插入数据库
    article = get_article(article_id)
    if article:
        # Connect to the database
        connection = pymysql.connect(host='115.159.119.204',
                                     user='root',
                                     password='BnakQkfF2sf1',
                                     db='hupu',
                                     port=10020,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "select id from hupu_article where id = %s"
                cursor.execute(sql, article_id)
                fetch_data = cursor.fetchone()
                if not fetch_data:
                    # 本地尚未入库，则插入数据库
                    sql = """
                        insert into hupu_article(id, title, publish_date, author, author_id, source, content)
                        values(%s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql, [
                        article.id, article.title, article.publish_date, article.author,
                        article.author_id, article.source, article.content
                    ])

                    # 结巴分词解析content的关键词
                    if article.title in article.content:
                        kwords = jieba.cut(article.content)
                    else:
                        kwords = jieba.cut(",".join([article.title, article.content]))

                    # 排除纯数字，纯字符
                    finally_kwords = filter(lambda x: not x.isdigit() and len(x) > 1, kwords)
                    for kword in finally_kwords:
                        sql = """
                            insert into hupu_keywards(keyword) value(%s)
                        """
                        cursor.execute(sql, kword)

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            connection.commit()
        finally:
            connection.close()


def spider(queue):
    """
    根据队列中的请求数据，实际爬取
    :param queue:
    :return:
    """
    while True:
        try:
            spider_info = queue.get()
            if spider_info:
                spider_info = spider_info
                dtype = spider_info['type']
                data = spider_info['data']
                if dtype == 'article':
                    download_article(data['article_id'])
                elif dtype == 'comment':
                    download_comments(data['article_id'], data['page'])
                else:
                    print(f"不支持的类型{dtype}")
            else:
                print("任务结束")
                break
        except:
            traceback.print_exc()


if __name__ == "__main__":
    download_comments(33487046, 1)
