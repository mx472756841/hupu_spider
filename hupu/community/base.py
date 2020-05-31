import json
import re
from http.cookiejar import CookieJar

from pyquery.pyquery import PyQuery as pquery

from hupu.exceptions import CookieException, BaseException
from hupu.structures import *
from hupu.structures.author import Author
from hupu.utils.fetch import fetch
from settings import logger, HUPU_DOWNLOAD_COOKIES_KEY
from tools.db import RedisClient
from tools.utils import recursive_unicode

PAGE_COUNT = re.compile("pageCount:(?P<page_count>\d+)", re.S)


def get_article(article_id):
    """
    获取文章信息

    https://bbs.hupu.com/[33465957].html
    :return:
    """
    real_article_url = f"{real_bbs_url}/{article_id}.html"
    result = fetch(real_article_url)
    if result:
        root = pquery(result)
        article_node = root.find(".floor_box").eq(0)
        user_node = article_node.find('div.author')
        author = user_node.find("a.u").text()
        author_id = user_node.find("a.u").attr("href").split("/")[-1]
        publish_date = user_node.find("span.stime").text()
        content_node = article_node.find('table.case')
        title = content_node.find(".subhead").children("span").eq(0).text()
        # 内容中图片
        image_nodes = content_node.find("img")
        image_urls = []
        for image in image_nodes.items():
            image_urls.append(image.attr("src"))

        # 内容中视频
        video_nodes = content_node.find("video")
        video_urls = []
        for video in video_nodes.items():
            video_urls.append(video.attr("src"))

        # 手机上发布获取到内容的方式 最后标签是small
        is_mobile = content_node.find(".quote-content").children("small:last")
        if is_mobile and "发自虎扑" in is_mobile.text():
            content = content_node.find(".quote-content").children("p").text()
        else:
            # 电脑上发布回去到内容的方式
            content = content_node.find(".quote-content").children("div[data-type='normal']").text()
            # 如果上述方式无法获得，则将.quote-content中 f666样式删除，取所有节点的text
            if not content:
                content_node.find(".quote-content").remove(".f666")
                content = content_node.find(".quote-content").text()

        return Article(
            id=article_id,
            title=title,
            author_id=author_id,
            author=author,
            content=content,
            source=real_article_url,
            publish_date=publish_date,
            images=image_urls,
            videos=video_urls
        ) if title else None
    elif result is None:
        # 文章已被删除
        logger.info(f"real_article_url {real_article_url} result = {result}")
        return None
    else:
        raise RuntimeError(f"get article {article_id} error!!!")


def get_commtents(article_id, page):
    """
    获取评论ID
    :param article_id: 文章ID
    :param page: 评论页数
    :return:
    """
    comment_url = f"{real_bbs_url}/{article_id}-{page}.html"
    result = fetch(comment_url)
    if result:
        return_data = {
            "total_page": 1,
            "current_comments": []
        }
        root = pquery(result)
        comments = root.find('div.floor:not([id="tpc"])')
        page_count = re.search(PAGE_COUNT, result.decode("utf-8"))
        if page_count:
            return_data['total_page'] = int(page_count.group("page_count"))

        return_comments = []
        for comment in comments.items():
            comment_id = comment.attr("id")
            comment_author_node = comment.find("div.author").find("a.u")
            comment_author = comment_author_node.text()
            comment_author_id = comment_author_node.attr("href").split("/")[-1]
            reply_comment = ""
            if comment.find("blockquote > p:gt(0)"):
                reply_comment = comment.find("blockquote > p:gt(0)").text()
            comment.remove("blockquote")
            comment.remove("small.f999")
            comment_str = comment.find("table.case").find("td").text()
            publish_date = comment.find("div.author > div.left > span.stime").text()
            if comment_id:
                return_comments.append(Comment(id=comment_id,
                                               author=comment_author,
                                               author_id=comment_author_id,
                                               comment=comment_str,
                                               publish_date=publish_date,
                                               reply_comment=reply_comment))
        return_data["current_comments"] = return_comments

        return return_data
    elif result is None:
        # 文章已被删除
        return None
    else:
        raise RuntimeError(f"get article {article_id} page {page} comment error!!!")


def get_article_list(plate, page, cookies=None):
    """
    获取文章列表，返回文章id列表，和每个文章的评论页数
    :param page:
    :return:
    """
    real_fetch_url = plate_url % (plate, page)
    if not isinstance(cookies, (dict, CookieJar)):
        cookies = {}
    # 添加cookies的几种方式
    # 1. 直接放在headers中，cookie键对应的值就是浏览器复制的值
    # 2. 在request时，增加key cookies，对应的是一个字典或者CookieJar类型的对象，从数据库取出需要的即可
    result = fetch(real_fetch_url, cookies=cookies)
    if result:
        return_data = []
        article_list = pquery('.show-list', result)
        articles = article_list.find("ul.for-list li").items()
        for article in articles:
            # 刨除置顶数据
            if "[置顶]" in article.find("span").text():
                continue

            source = article.find("a.truetit").eq(0).attr("href")
            # 文章ID
            article_id = source.split('/')[1].split('.')[0]
            # 评论页数
            multipage_node = article.find("span.multipage a:last")
            if multipage_node:
                comment_pages = int(multipage_node.text())
            else:
                comment_pages = 1
            return_data.append({"article_id": article_id, "comment_pages": comment_pages})
        return return_data
    else:
        raise RuntimeError(f"get plate {plate} {page} error!!!")


def get_user_detail(user_id, cookies):
    """
    获取用户明细信息 需要登录
    :param user_id:
    :return:
    """
    real_fetch_url = user_profile_url % user_id
    # 添加cookies的几种方式
    # 1. 直接放在headers中，cookie键对应的值就是浏览器复制的值
    # 2. 在request时，增加key cookies，对应的是一个字典或者CookieJar类型的对象，从数据库取出需要的即可
    result = fetch(real_fetch_url, cookies=cookies)
    if result:
        root = pquery(result)
        title = root.find("head title").text()
        if title == "嗯，出错了...":
            raise CookieException()
        elif not title.endswith("的档案"):
            raise BaseException(error_info="非正常的用户ID")
        profile = root("table.profile_table").eq(0)
        profile_list = profile.find('tr').items()
        profile_info = {
            "author_id": user_id,
            "author_name": root("#headtop > h1").text().strip("的档案"),
        }
        for p in profile_list:
            key, v = p('td').eq(0).text().strip()[:-1], p('td').eq(1).text()
            if key == "所在地":
                if v != "null":
                    profile_info['place'] = v.strip()
            if key == '性别':
                profile_info['gener'] = v.strip()
            if key == '论坛等级':
                profile_info['level'] = int(v.strip())
            if key == '注册时间':
                profile_info['register_date'] = v.strip()
        return Author(**profile_info)
    elif result is None:
        # 用户不存在
        return None
    else:
        raise RuntimeError(f"get user_detail {user_id} error!!!")


if __name__ == "__main__":
    # article = get_article(34576735)
    # print(article.content)
    # print(article.images)
    # print(article.videos)
    # article = get_article(34577737)
    # print(article.content)
    # print(article.images)
    # print(article.videos)
    # print("content = {}".format("Paddle Mode: " + '/'.join(list(jieba.cut(article.content)))))
    # for row in get_commtents(33458078, 1):
    #     print("comment = {}".format("Paddle Mode: " + '/'.join(list(jieba.cut(row.comment)))))
    # get_commtents("33458078", 1)
    # get_commtents("34563541", 1)
    # print(get_commtents("34593801", 1))

    # url = "https://bbs.hupu.com/33600265.html"
    # result = fetch(url)
    # print(result)

    client = RedisClient.get_client()
    cookies = json.loads(recursive_unicode(client.get(HUPU_DOWNLOAD_COOKIES_KEY)))
    # print(cookies)
    # get_user_detail(35512074689389, cookies)  # 保密
    # get_user_detail(238467143452731, cookies)  # 男 所在地是没有
    # data = get_user_detail(30193012086352, cookies)  # 女 所在地是上海市浦东新区
    # print(get_user_detail(280923583165420, cookies))  # 保密 所在地是上海市
    # data = get_user_detail(205226121481456, cookies)  # 所在地是null
    author = get_user_detail(95453713761116, cookies)
    print(author.province)
    print(author.city)
    print("================")
    author = get_user_detail(50447429476835, cookies)
    print(author.province)
    print(author.city)
    print("================")
    author = get_user_detail(188534634833820, cookies)
    print(author.province)
    print(author.city)
    print("================")
    author = get_user_detail(168510920646089, cookies)
    print(author.province)
    print(author.city)
    print("================")
    author = get_user_detail(60154180816327, cookies)
    print(author.province)
    print(author.city)
    print("================")
