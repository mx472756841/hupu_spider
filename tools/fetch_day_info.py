import json

import pymysql
import datetime
from settings import logger


def main():
    """
    处理历史数据
    :return:
    """
    try:
        logger.info("处理历史文章开始")
        print(f"{datetime.datetime.now()} 处理历史文章开始")
        connection = pymysql.connect(host='115.159.119.204',
                                     user='root',
                                     password='BnakQkfF2sf1',
                                     db='hupu',
                                     port=10020,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        with connection.cursor() as cursor:
            # 处理文章
            sql = """
                SELECT
                    DATE_FORMAT( publish_date, '%Y-%m-%d' ) as datestr,
                    persons 
                FROM
                    hupu_article 
                WHERE
                    id BETWEEN 33394052 
                    AND 35170245
            """
            cursor.execute(sql)
            articles = cursor.fetchall()
            for article in articles:
                try:
                    persons = json.loads(article['persons'])
                    for person in persons:
                        # 插入周榜信息
                        sql = """
                            INSERT INTO hupu_day_list(`day`, person_id, article_cnt)
                            VALUE(%s, %s, 1) ON DUPLICATE KEY UPDATE article_cnt = article_cnt + 1
                        """
                        cursor.execute(sql, [article['datestr'], person])
                except:
                    logger.exception(f"处理失败 {article}")
        connection.commit()
        logger.info("处理历史文章结束")
        print(f"{datetime.datetime.now()} 处理历史文章结束")
    except:
        logger.exception("处理历史文章失败")

    try:
        logger.info("处理历史评论开始")
        print(f"{datetime.datetime.now()} 处理历史评论开始")
        for i in range(0, 1212016, 5000):
            print(f"{datetime.datetime.now()} 开始处理第{(i + 1) * 5000}条评论")
            connection = pymysql.connect(host='115.159.119.204',
                                         user='root',
                                         password='BnakQkfF2sf1',
                                         db='hupu',
                                         port=10020,
                                         charset='utf8mb4',
                                         cursorclass=pymysql.cursors.DictCursor)
            with connection.cursor() as cursor:
                # 处理文章
                sql = """
                    SELECT
                        DATE_FORMAT( publish_date, '%Y-%m-%d' ) as datestr,
                        persons 
                    FROM
                        hupu_comment 
                    WHERE id > {} limit 5000
                """.format(i)
                cursor.execute(sql)
                comments = cursor.fetchall()
                for comment in comments:
                    try:
                        persons = json.loads(comment['persons'])
                        for person in persons:
                            # 插入周榜信息
                            sql = """
                                INSERT INTO hupu_day_list(`day`, person_id, comment_cnt)
                                VALUE(%s, %s, 1) ON DUPLICATE KEY UPDATE comment_cnt = comment_cnt + 1
                            """
                            cursor.execute(sql, [comment['datestr'], person])
                    except:
                        logger.exception(f"处理失败 {comment}")
            connection.commit()

            print(f"{datetime.datetime.now()} 处理完成第{(i + 1) * 5000}条评论")

        print(f"{datetime.datetime.now()} 处理历史评论结束")
    except:
        logger.exception("处理历史评论失败")
