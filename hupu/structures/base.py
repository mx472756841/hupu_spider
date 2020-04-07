import jieba.analyse
from copy import deepcopy


class Base(object):

    def json(self):
        """
        object to json
        :return:
        """
        # transfer object to dict
        d = deepcopy(self.__dict__)
        return d

    def get_tags(self, content, top=5):
        tags = jieba.analyse.extract_tags(content, topK=top, withWeight=False)
        return tags
