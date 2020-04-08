from http.cookiejar import CookieJar

from pyquery.pyquery import PyQuery as pquery

from hupu.structures import *
from hupu.utils.fetch import fetch


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
        content = content_node.find(".quote-content").children("p").text()
        return Article(
            id=article_id,
            title=title,
            author_id=author_id,
            author=author,
            content=content,
            source=real_article_url,
            publish_date=publish_date) if title else None
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
        root = pquery(result)
        comments = root.find('div.floor:not([id="tpc"])')
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
        return return_comments
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


if __name__ == "__main__":
    # article = get_article(33458078)
    # print("content = {}".format("Paddle Mode: " + '/'.join(list(jieba.cut(article.content)))))
    # for row in get_commtents(33458078, 1):
    #     print("comment = {}".format("Paddle Mode: " + '/'.join(list(jieba.cut(row.comment)))))
    get_commtents("33458078", 1)
