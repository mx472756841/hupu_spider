import re

from hupu.structures import Base

PROVINCE_LEVEL_STR = "北京市|天津市|河北省|山西省|内蒙古|辽宁省|吉林省|黑龙江省|上海市|江苏省|浙江省|安徽省|福建省|江西省|山东省|河南省|湖北省|湖南省|广东省|广西省|海南省|重庆市|四川省|贵州省|云南省|西藏|陕西省|甘肃省|青海省|宁夏|新疆|台湾省|澳门|香港|美国|英国|法国|瑞士|澳洲|新西兰|加拿大|奥地利|韩国|日本|德国|意大利|西班牙|俄罗斯|泰国|印度|荷兰|新加坡|欧洲|北美|南美|亚洲|非洲|大洋洲"

P_REGION = re.compile(
    "(?P<province>({}))(?P<city>.*)".format(PROVINCE_LEVEL_STR))

# 针对出现 [广东 潮州], [广西壮族自治区 百色市]
P_REGION_TYPE_1 = re.compile("(?P<province>.*?) (?P<city>.*)")

# 针对出现[广西梧州市]
P_REGION_TYPE_2 = re.compile("(?P<province>广西)(?P<city>.*)")


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
            match1 = re.match(P_REGION_TYPE_1, self.province)
            match2 = re.match(P_REGION_TYPE_2, self.province)
            if match:
                # 正常可处理的情况
                gd = match.groupdict()
                self.province = gd['province'].strip()
                self.city = gd['city'].strip()
            elif match1:
                # 正常不可以处理，但是有空格隔开，且可以分辨出省份的情况
                gd = match1.groupdict()
                province = gd['province'].strip()
                start_idx = PROVINCE_LEVEL_STR.find(province[:2])
                if start_idx != -1:
                    end_idx = PROVINCE_LEVEL_STR.find("|", start_idx)
                    if end_idx == -1:
                        self.province = PROVINCE_LEVEL_STR[start_idx:]
                    else:
                        self.province = PROVINCE_LEVEL_STR[start_idx: end_idx]
                    self.city = gd['city'].strip()
            elif match2:
                gd = match2.groupdict()
                self.province = "广西省"
                self.city = gd['city'].strip()
            else:
                province = self.province.strip()
                start_idx = PROVINCE_LEVEL_STR.find(province[:2])
                if start_idx != -1:
                    end_idx = PROVINCE_LEVEL_STR.find("|", start_idx)
                    if end_idx == -1:
                        self.province = PROVINCE_LEVEL_STR[start_idx:]
                    else:
                        self.province = PROVINCE_LEVEL_STR[start_idx: end_idx]
                    self.city = province[2:].strip()
        self.register_date = kwargs.get("register_date", "")

    def __repr__(self):
        """
        :return:
        """
        return '<Author: <%s, %s>>' % (self.author_id, self.author_name)
