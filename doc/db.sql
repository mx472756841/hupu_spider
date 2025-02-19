CREATE TABLE `user_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `openid` varchar(50) NOT NULL COMMENT '用户微信ID',
  `unionid` varchar(100) COMMENT '用户微信 UNIOIN ID',
  `nickname` varchar(100) COMMENT '用户昵称',
  `avatar` varchar(300) COMMENT '用户头像',
  `city` varchar(100) COMMENT '城市',
  `province` varchar(100) COMMENT '城市',
  `country` varchar(100) COMMENT '国家',
  `language` varchar(100) COMMENT '语言',
  `gender` varchar(5) COMMENT '性别',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `openid` (`openid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE `user_suggest`(
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL COMMENT '用户ID',
  `suggest` text COMMENT '用户建议',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `user_id`(`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `hupu_article` (
  `id` int(11) NOT NULL,
  `title` varchar(500) NOT NULL COMMENT '文章标题',
  `publish_date` DATETIME NOT NULL COMMENT '发布时间',
  `author` varchar(100) NOT NULL COMMENT '作者',
  `author_id` varchar(50) NOT NULL COMMENT '作者ID',
  `source` varchar(200) NOT NULL COMMENT '文章原链接',
  `content` text COMMENT '文章内容',
  `kws` text COMMENT '关键词',
  `persons` text COMMENT '关联人原信息',
  PRIMARY KEY (`id`),
  KEY `publish_date` (`publish_date`),
  KEY `author_id` (`author_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `hupu_comment` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '本地id',
  `article_id` int(11) NOT NULL COMMENT '文章ID',
  `comment_id` int(11) NOT NULL COMMENT '评论ID',
  `publish_date` DATETIME NOT NULL COMMENT '发布时间',
  `author` varchar(100) NOT NULL COMMENT '作者',
  `author_id` varchar(50) NOT NULL COMMENT '作者ID',
  `comment` text COMMENT '评论内容',
  `reply_comment` text COMMENT '被回复评论内容',
  `kws` text COMMENT '关键词',
  `persons` text COMMENT '关联人原信息',
  PRIMARY KEY (`id`),
  KEY `publish_date` (`publish_date`),
  KEY `article_comment_id` (`article_id`, comment_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE `hupu_keywards` (
  `keyword` varchar(200) NOT NULL COMMENT '关键字'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `person_info`(
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `name` varchar(200) NOT NULL COMMENT '人员名字',
  `desc` text COMMENT '人员备注',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='常用人员信息表';

CREATE TABLE `hupu_day_list`(
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `day` varchar(10) NOT NULL COMMENT 'yyyy-mm-dd',
  `person_id` int(11) NOT NULL COMMENT '人员ID',
  `ranking` int(5) COMMENT '排名 后续系统更新',
  `comment_cnt` int(11) DEFAULT 0 NOT NULL COMMENT '相关评论',
  `article_cnt` int(11) DEFAULT 0 NOT NULL COMMENT '相关文章',
  `last_update_time` int(13)  COMMENT '最后更新排名时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `person_day` (`person_id`, `day`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='日榜';

CREATE TABLE `hupu_week_list`(
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `week_info` varchar(17) NOT NULL COMMENT 'yyyymmdd-yyyymmdd',
  `person_id` int(11) NOT NULL COMMENT '人员ID',
  `ranking` int(5) COMMENT '排名 后续系统更新',
  `comment_cnt` int(11) DEFAULT 0 NOT NULL COMMENT '相关评论',
  `article_cnt` int(11) DEFAULT 0 NOT NULL COMMENT '相关文章',
  `last_update_time` int(13)  COMMENT '最后更新排名时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `person_week` (`person_id`, `week_info`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='周榜';

CREATE TABLE `hupu_month_list`(
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `month_info` int(11) NOT NULL COMMENT 'year+0N第几月',
  `person_id` int(11) NOT NULL COMMENT '人员ID',
  `ranking` int(5) COMMENT '排名 后续系统更新',
  `comment_cnt` int(11) DEFAULT 0 NOT NULL COMMENT '相关评论',
  `article_cnt` int(11) DEFAULT 0 NOT NULL COMMENT '相关文章',
  `last_update_time` int(13)  COMMENT '最后更新排名时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `person_month` (`person_id`, `month_info`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='月榜';

CREATE TABLE `hupu_user_post_pserson_kws`(
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `openid` varchar(60) NOT NULL COMMENT '用户ID',
  `person_id` int(11) NOT NULL COMMENT '人员ID',
  `kw` varchar(500) NOT NULL COMMENT '关键词',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  key `person_id` ( `person_id`),
  key `openid` ( `openid`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='用户提交关键词';


CREATE TABLE `hupu_author_info` (
  `author_id` varchar(25) NOT NULL COMMENT '用户ID',
  `author_name` varchar(200) NOT NULL COMMENT '用户昵称',
  `level` int(4) COMMENT '等级',
  `place1` varchar(20) COMMENT '一级地方',
  `place2` varchar(20) COMMENT '二级地方',
  `register_date` DATETIME COMMENT '注册时间',
  `gener` tinyint(1) COMMENT '性别 0:女 1:男 2:未知'
  PRIMARY KEY (`author_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;