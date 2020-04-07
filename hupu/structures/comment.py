from hupu.structures import Base


class Comment(Base):
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.publish_date = kwargs.get("publish_date")
        self.author = kwargs.get("author")
        self.author_id = kwargs.get("author_id")
        self.comment = kwargs.get("comment")
        self.reply_comment = kwargs.get("reply_comment")
        if self.reply_comment:
            content = f"{self.reply_comment} {self.comment}"
        else:
            content = self.comment
        self.tags = self.get_tags(content)

    def __repr__(self):
        """
        :return:
        """
        return '<Comment: <%s, %s, %s>>' % (self.id, self.author, self.comment)
