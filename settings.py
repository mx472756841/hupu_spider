import logging.config
import os

from tools.log import Log

# 当前目录所在路径

BASE_PATH = os.path.abspath(os.path.dirname(__file__))

# 日志所在目录
LOG_PATH = os.path.join(BASE_PATH, 'logs')

REDIS_HOST = os.environ.get("REDIS_HOST", "115.159.119.204")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6381))
REDIS_PASS = os.environ.get("REDIS_PASS", "CCcaQagK21A1A12")
REDIS_DB = int(os.environ.get("REDIS_DB", 0))

MYSQL_DB_HOST = os.environ.get("MYSQL_DB_HOST", "115.159.119.204")
MYSQL_DB_PORT = int(os.environ.get("MYSQL_DB_PORT", 10020))
MYSQL_DB_USER = os.environ.get("MYSQL_DB_USER", "root")
MYSQL_DB_PASSWORD = os.environ.get("MYSQL_DB_PASSWORD", "BnakQkfF2sf1")
MYSQL_DB_DBNAME = os.environ.get("MYSQL_DB_DBNAME", "hupu")

# 存储初始化，关键词对应用户ID
SHH_KW_HASH = "shh:kw:hash"
# 存储初始化，用户ID对应关键词
SHH_PERSON2KWS_HASH = "shh:person2kws:hash"

# 虎扑下载时的cookie信息
HUPU_DOWNLOAD_COOKIES_KEY = "hupu:download:cookies:key"

# 记录每次下载文章的点，下载到这个点之后就结束
LAST_DOWNLOAD_ARTICLE_ID_KEY = "last:download:article:id"

# 文章评论比
ARTICLE_TO_COMMENT = 50

# 记录文章评论下载的页数
# 第一条评论的日期 + 下载的页数，后续会根据第一条评论的日期，记录次数，7天之内，每30分钟执行一次， 7天之后，每天执行一次
ARTICLE_DOWNLOAD_COMMENT_PAGE = "article:%s:download:comment:page:hash"

# 中间key，celery定时调度常常有问题，增加中间key，使用apscheduler调度，列表内容为字典，func_name, args, task_id作为key
CELERY_TO_APSCHEDULER_LIST = "celery:2:apscheduler:list"

# 日志模块配置
if not os.path.exists(LOG_PATH):
    # 创建日志文件夹
    os.makedirs(LOG_PATH)

log = Log(LOG_PATH, "hupu")
logging.config.dictConfig(log.log_config_dict)
logger = logging.getLogger("only_file_logger")
# 执行自定义配置 如数据库等相关配置, 放在日志配置之前的原因,是日志会根据DEBUG变化而变化
etc_path = os.path.join(BASE_PATH, "etc", 'cfg.py')
if os.path.exists(etc_path):
    file = open(etc_path, 'r')
    text = file.read()
    file.close()
    try:
        exec(text)
    except Exception as e:
        print(e)

# celery相关配置,需要使用redis的相关信息,所以写在此处
CELERY_BROKER_URL = 'redis://:{2}@{0}:{1}'.format(REDIS_HOST, REDIS_PORT, REDIS_PASS)  # 指定 Broker
CELERY_RESULT_BACKEND = 'redis://:{2}@{0}:{1}/0'.format(REDIS_HOST, REDIS_PORT, REDIS_PASS)  # 指定 Backend
