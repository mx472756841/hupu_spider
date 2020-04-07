from multiprocessing import Process, SimpleQueue, cpu_count

from hupu.community.base import get_article_list
from pools import spider


def run_get_article_list(q):
    for idx in range(1, 2):
        articles = get_article_list("vote", idx)
        print(articles)
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
    q.put(None)


def run():
    q = SimpleQueue()

    p = Process(target=run_get_article_list, args=(q,), name="get_article_list")
    p.start()


    for i in range(0, cpu_count() - 3):
        p = Process(target=spider, args=(q,), name=f"spider-{i}")
        p.start()


if __name__ == "__main__":
    run()
