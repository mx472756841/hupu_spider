from multiprocessing import Process, Queue

import pymysql

from hupu.community.base import get_article_list
from pools import spider


def run_get_article_list(q):
    # 虎扑登录后，产生的cookie，对于虎扑而言u这个cookie是实际使用的key
    cookies = {
        "u": "28543539|6JCn55Gf5pyX|9b49|26d02c47a8424dd9fb9d72416a700969|a8424dd9fb9d7241|aHVwdV8wZWVlNTQ1YjA0NGY2OTlm"
    }
    for idx in range(1, 600):
        articles = get_article_list("vote", idx, cookies)

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
    q.put(None)


def run():
    q = Queue(2000)

    p = Process(target=run_get_article_list, args=(q,), name="get_article_list")
    p.start()

    for i in range(0, 2):
        p = Process(target=spider, args=(q,), name=f"spider-{i}")
        p.start()


def test_get_article_list():
    for idx in range(11, 12):
        # 虎扑登录后，产生的cookie，对于虎扑而言u这个cookie是实际使用的key
        cookies = {
            "u": "28543539|6JCn55Gf5pyX|9b49|26d02c47a8424dd9fb9d72416a700969|a8424dd9fb9d7241|aHVwdV8wZWVlNTQ1YjA0NGY2OTlm"
        }
        articles = get_article_list("vote", idx, cookies)
        print(f"spider {idx} articles {len(articles)}...")
        for row in articles:
            # 爬取文章信息
            for i in range(1, row['comment_pages'] + 1):
                print(f"spider articles {row['article_id']} page {i} comment ...")


if __name__ == "__main__":
    test_get_article_list()
