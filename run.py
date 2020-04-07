import time
from multiprocessing import Process, Queue

import pymysql

from hupu.community.base import get_article_list
from pools import spider


def run_get_article_list(q):
    for idx in range(1, 600):
        articles = get_article_list("vote", idx)

        if articles:
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
                    sql = """
                        insert into datas(idx, `length`) values(%s, %s)
                    """
                    cursor.execute(sql, [idx, len(articles)])
                connection.commit()
            finally:
                connection.close()

        for row in articles:
            # 爬取文章信息
            article_data = {
                "type": "article",
                "data": {
                    "article_id": row['article_id']
                }
            }
            q.put(article_data)
            # 爬取文章的评论信息
            for i in range(1, row['comment_pages'] + 1):
                comment_data = {
                    "type": "comment",
                    "data": {
                        "article_id": row['article_id'],
                        "page": i
                    }
                }
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
                        sql = """
                           insert into datas(article_id, comments) values(%s, %s)
                       """
                        cursor.execute(sql, [row['article_id'], i])
                    connection.commit()
                finally:
                    connection.close()

                q.put(comment_data)
            if q.size() > 1500:
                time.sleep(60 * 10)
    q.put(None)


def run():
    q = Queue()

    p = Process(target=run_get_article_list, args=(q,), name="get_article_list")
    p.start()

    for i in range(0, 2):
        p = Process(target=spider, args=(q,), name=f"spider-{i}")
        p.start()


if __name__ == "__main__":
    run()
