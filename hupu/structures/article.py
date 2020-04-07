from hupu.config import *

from hupu.structures.base import Base


class ArticleList(object):

    def __init__(self, show_list):
        """
        :param show_list: pyquery对象，文章列表内容
        """
        self.articles = []
        articles = show_list.find("ul.for-list li").items()
        for article in articles:
            self.articles.append(self._parse_single(article))

    def _parse_single(self, article):
        source = article.find("a.truetit").eq(0).attr("href")
        # 文章ID
        article_id = source.split('/')[1].split('.')[0]
        # 文章地址
        real_source = f"{real_bbs_url}{source}"
        # 文章标题
        title = article.find("a.truetit").eq(0).text()
        # 文章作者id
        author_id = article.find("a.aulink").eq(0).attr('href').split()
        # 文章作者
        author = article.find("a.aulink").eq(0).text()
        # 发帖日期
        publish_date = article.find("a.aulink").eq(0).siblings('a').eq(0).text()
        # 主贴内容
        return Article(
            id=article_id,
            title=title,
            publish_date=publish_date,
            author_id=author_id,
            author=author,
            source=real_source
        )


class Article(Base):
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.title = kwargs.get("title")
        self.publish_date = kwargs.get("publish_date")
        self.author = kwargs.get("author")
        self.author_id = kwargs.get("author_id")
        self.source = kwargs.get("source")
        self.content = kwargs.get("content")
        if self.title not in self.content:
            content = f"{self.title} {self.content}"
        else:
            content = self.content
        self.tags = self.get_tags(content, 20)
