import re
from hupu.structures import Base

P_REGION = re.compile(
    "(?P<province>(北京市|天津市|河北省|山西省|内蒙古|辽宁省|吉林省|黑龙江省|上海市|江苏省|浙江省|安徽省|福建省|江西省|山东省|河南省|湖北省|湖南省|广东省|广西省|海南省|重庆市|四川省|贵州省|云南省|西藏|陕西省|甘肃省|青海省|宁夏|新疆|台湾省|澳门|香港|美国|英国|法国|瑞士|澳洲|新西兰|加拿大|奥地利|韩国|日本|德国|意大利|西班牙|俄罗斯|泰国|印度|荷兰|新加坡|欧洲|北美|南美|亚洲|非洲|大洋洲))(?P<city>.*)")


class Author(Base):
    def __init__(self, **kwargs):
        self.author_name = kwargs.get("author_name")
        self.author_id = kwargs.get("author_id")
        self.gener = kwargs.get("gener", "保密")
        self.level = kwargs.get("level", 0)
        self.province = kwargs.get("place", "")
        self.city = ""
        # 通过虎扑的regionhttps://b1.hoopchina.com.cn/pcbbs/js/region2013-12-25.js分析只有省和其他海外，所以可以对正常省份处理，海外的单独处理
        if self.province:
            match = re.match(P_REGION, self.province)
            if match:
                gd = match.groupdict()
                self.province = gd['province'].strip()
                self.city = gd['city'].strip()
        self.register_date = kwargs.get("register_date", "")

    def __repr__(self):
        """
        :return:
        """
        return '<Author: <%s, %s>>' % (self.author_id, self.author_name)
