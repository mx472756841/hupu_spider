import math
import re
import time

import jieba
import pymysql

# jieba.load_userdict("F:\\mengxiang\\hupu_spider\\etc\\shh_dict.txt")
char = re.compile(
    "^[。？！，、；：“” ‘’「」『』（）\[\]〔〕【】——……—\-～·《》〈〉﹏﹏___.－_+±×÷=≠≡≌≈<>≮≯≤≥%‰∞∝√∵∴∷∠⌒⊙○π△⊥∪∩∫∑°′″℃\{\}\(\)\(\)\[\]\[\]\|&\*///#\\~\.,:;\?!'...\"‖&～§→a-zA-Z0-9]*&")


def generate_jieba_tf_idf_kw():
    """
    基于jieba分词的TF-IDF提取关键词算法中，根据不同领域自定义所使用逆向文件频率（IDF）的文本语料库
    根据虎扑抓取湿乎乎文章的数据进行关键字提取

    将大量数据计算出每个关键词的权重用于后续使用
    1. 抓取文章及评论，并进行分词
    2. 按照tf算法计算关键字的权重
    :return:
    """
    all_kw_dict = {}
    kw_dict = {}
    interval = 2000
    total_cnt = 64567 + 3014729
    # 1. 获取所有条目数 文章数+评论数
    for page in range(1, math.ceil(64567 / interval) + 1):
        print(f"处理文章第{page}页...")
        offset = (page - 1) * interval
        sql = """
            select id, title, content from hupu_article order by id desc limit %s, %s
        """

        connection = pymysql.connect(host='115.159.119.204',
                                     user='root',
                                     password='BnakQkfF2sf1',
                                     db='hupu',
                                     port=10020,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        with connection.cursor() as cursor:
            cursor.execute(sql, [offset, interval])
            fetch_data = cursor.fetchall()

        for row in fetch_data:
            # 结巴分词解析content的关键词
            if row['title'] in row['content']:
                kwords = jieba.cut(row['content'])
            else:
                kwords = jieba.cut(" ".join([row['title'], row['content']]))

            # 排除纯数字，纯字符
            finally_kwords = filter(lambda x: not x.isdigit() and len(x) > 1, kwords)
            tmp_kws = set()
            for kw in finally_kwords:
                all_kw_dict[kw] = all_kw_dict.get(kw, 0) + 1
                if kw not in tmp_kws:
                    kw_dict[kw] = kw_dict.get(kw, 0) + 1
                    tmp_kws.add(kw)

        connection.close()
        print(f"处理文章第{page}页完成...")

    for page in range(1, math.ceil(3014729 / interval) + 1):
        times = 1
        while True:
            try:
                print(f"处理评论第{page}页...")
                offset = (page - 1) * interval
                sql = """
                    select id, comment, reply_comment from hupu_comment order by id, article_id desc limit %s, %s
                """

                connection = pymysql.connect(host='115.159.119.204',
                                             user='root',
                                             password='BnakQkfF2sf1',
                                             db='hupu',
                                             port=10020,
                                             charset='utf8mb4',
                                             cursorclass=pymysql.cursors.DictCursor)

                with connection.cursor() as cursor:
                    cursor.execute(sql, [offset, interval])
                    fetch_data = cursor.fetchall()

                for comment in fetch_data:
                    # 结巴分词解析content的关键词
                    if comment['reply_comment'] and "隐藏" not in comment['reply_comment']:
                        kwords = jieba.cut(",".join([comment['reply_comment'], comment['comment']]))
                    else:
                        kwords = jieba.cut(comment['comment'])

                    # 排除纯数字，纯字符
                    finally_kwords = filter(lambda x: not x.isdigit() and len(x) > 1, kwords)
                    tmp_kws = set()
                    for kw in finally_kwords:
                        all_kw_dict[kw] = all_kw_dict.get(kw, 0) + 1
                        if kw not in tmp_kws:
                            kw_dict[kw] = kw_dict.get(kw, 0) + 1
                            tmp_kws.add(kw)

                connection.close()
                print(f"处理评论第{page}页完成...")
                break
            except:
                print(f"处理评论第{page}页 第{times}次异常...")
                if times == 3:
                    print("失败次数太多退出,获取下一页")
                    break
                else:
                    times += 1
                    print(f"休息30s")
                    time.sleep(30)

    with open("all_kw_dict.txt", 'w') as f:
        for k, v in all_kw_dict.items():
            f.write(f"{k} {v}\n")

    with open("kw_dict_all.txt", 'w') as f:
        for k, v in kw_dict.items():
            f.write(f"{k} {v}\n")

    with open("kw_dict_gt_500.txt", 'w') as f:
        for k, v in filter(lambda x: x[1] > 1, kw_dict.items()):
            f.write(f"{k} {math.log(total_cnt / (1 + v)):8.4f}\n")

    # 2. 一篇文章一条评论的处理，累计每个关键词在文章/评论中出现的次数

    # 3. 计算每个词的数据


def generate_jieba_cut_kw():
    all_players = set()
    with open("F:\\mengxiang\\hupu_spider\\etc\\player_name.txt", 'r', encoding='utf-8') as f:
        data = f.readlines()
        for row in data:
            line = row.split("-")
            # 只取后面的
            if line[-1]:
                all_players.add(line[-1].strip())
            all_players.add(row.strip())
            new_row = row.replace('-', '').strip()
            all_players.add(new_row)

    with open("F:\\mengxiang\\hupu_spider\\etc\\player_kw.txt", 'r', encoding='utf-8') as f:
        data = f.readlines()
        for row in data:
            all_players.add(row.strip())

    with open("F:\\mengxiang\\hupu_spider\\etc\\shh_dict.txt", 'w', encoding='utf-8') as f:
        f.write("\n".join(list(all_players)))


def generate_aka2playname():
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
            sql = "select aka, player_name from hupu_players_tmp"
            cursor.execute(sql)
            data = cursor.fetchall()
            aka2player_name = {}
            for line in data:
                aka_data = aka2player_name.get(line['aka'], [])
                aka_data.append(line['player_name'])
                aka2player_name[line['aka']] = aka_data
    finally:
        connection.close()


if __name__ == "__main__":
    generate_aka2playname()
    # generate_jieba_cut_kw()
    # total_cnt = 64567 + 3014729
    # with open("F:\\mengxiang\\hupu_spider\\etc\\shh_dict.txt", 'r', encoding='utf-8') as f:
    #     players = [i.strip() for i in f.readlines()]
    #
    # exists_kw = set()
    # with open("kw_dict_gt_500.idf.txt", 'wb') as f:
    #     with open("kw_dict_all.txt", 'r') as f1:
    #         data = f1.readlines()
    #
    #     for line in data:
    #         k, w = line.strip().split(" ")
    #         if int(w) > 10:
    #             if k.strip() in players:
    #                 f.write(f"{k.strip()} 14.0000\n".encode('utf-8'))
    #                 exists_kw.add(k.strip())
    #             else:
    #                 f.write(f"{k.strip()} {math.log(total_cnt / (1 + int(w))):.4f}\n".encode('utf-8'))
    #
    #     for k in list(set(players) - exists_kw):
    #         f.write(f"{k} 14.0000\n".encode('utf-8'))

    # with open("all_kw_dict.txt", 'r') as f1:
    #     data = f1.readlines()

    # connection = pymysql.connect(host='115.159.119.204',
    #                              user='root',
    #                              password='BnakQkfF2sf1',
    #                              db='hupu',
    #                              port=10020,
    #                              charset='utf8mb4',
    #                              cursorclass=pymysql.cursors.DictCursor)
    #
    # with connection.cursor() as cursor:
    #     sql = "insert into hupu_keywards(keyword, times) value(%s, %s)"
    #     for idx, line in enumerate(data):
    #         k, w = line.strip().split(" ")
    #         if len(k) < 50 and int(w) > 50:
    #             cursor.execute(sql, [k, w])
    #
    #         if (idx + 1) % 50 == 0:
    #             print(f"已处理完成{idx + 1}条")
    #
    # connection.commit()
