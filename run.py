import time
import threading
from multiprocessing import Process, Queue

from hupu.community.base import get_article_list
from pools import spider


def run_get_article_list(q):
    # 虎扑登录后，产生的cookie，对于虎扑而言u这个cookie是实际使用的key
    cookies = {
        "u": "28543539|6JCn55Gf5pyX|9b49|26d02c47a8424dd9fb9d72416a700969|a8424dd9fb9d7241|aHVwdV8wZWVlNTQ1YjA0NGY2OTlm"
    }
    for idx in range(2000, 0, -1):
        articles = get_article_list("vote", idx, cookies)
        print(f"{threading.get_ident()} - 第{idx}页，共抓取{len(articles)}条...")
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
                q.put(comment_data)
                print(f"{threading.get_ident()} - 文章{row['article_id']}抓取{row['comment_pages'] + 1}页评论...")
            if q.qsize() > 1500:
                print(f"{threading.get_ident()}  - 队列长度为{q.qsize()}， 暂停10分钟...")
                time.sleep(600)
    q.put(None)
    print("2000页已经加载完成")


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
    run()
