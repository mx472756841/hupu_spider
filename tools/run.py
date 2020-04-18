import threading
import time
from multiprocessing import Process, Queue

from hupu.community.base import get_article_list
from pools import spider
from tools.db import RedisClient


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


def test_extract_tags():
    import jieba
    from jieba.analyse import textrank, extract_tags
    contents = [
        # "霍夫德是季后赛给字母准备的礼物",
        # "我是杜兰特，10000%留雷霆，有钱又有名，要啥垃圾两连FMVP",
        # "同理，听你们说詹姆斯没技术不会罚球，废队友，废教练，那么他又是怎么混到现在呢？",
        # "伙夫养生篮球了吗？",
        # "目前总篮板数9176，助攻数9059，本赛季场均篮板8个，场均助攻10.7次",
        # "欧文：麻溜的！！！排队🐶🐶 道歉！!!",
        # "这就是冠军后卫 凯里欧文！！",
        """虎扑4月11日讯 近日，开拓者球星达米安-利拉德在直播中分享了自己之前打棒球的趣事。利拉德说：我跟大家也谈过到不少次，其实我之前是打棒球的。我爸爸是棒球运动员，因此他也希望我们能够成为棒球手，我从小就开始打棒球，我小时候棒球打得不错。但是你知道为什么后来我打篮球了？我很喜欢棒球，可是我小时候有一次打棒球联赛，结果被投手扔出的球给砸到了，直接给我砸放弃了，我记得当时我就直接从球场上面走下去了，哈哈哈""",
    ]
    # for i in contents:
    #     # print(textrank(i, withWeight=True))
    #     print(extract_tags(i, withWeight=True))
    # print("-------------------")

    jieba.load_userdict("F:\\mengxiang\\hupu_spider\\etc\\shh_dict.txt")
    jieba.analyse.set_idf_path('F:\\mengxiang\\hupu_spider\\tools\\kw_dict_gt_500.idf.txt')
    for i in contents:
        print(extract_tags(i, withWeight=True))
    print("-------------------")
    #     print(" ".join(jieba.cut(i)))
    # jieba.load_userdict('etc/shh_dict.txt')
    # for i in contents:
    #     print(" ".join(jieba.cut(i)))
    # for i in contents:
    #     print(textrank(i, withWeight=True))
    #     print(extract_tags(i, withWeight=True))


if __name__ == "__main__":
    test_extract_tags()