CREATE TABLE `hupu_article` (
  `id` int(11) NOT NULL,
  `title` varchar(500) NOT NULL COMMENT '文章标题',
  `publish_date` DATETIME NOT NULL COMMENT '发布时间',
  `author` varchar(100) NOT NULL COMMENT '作者',
  `author_id` varchar(50) NOT NULL COMMENT '作者ID',
  `source` varchar(200) NOT NULL COMMENT '文章原链接',
  `content` text COMMENT '文章内容',
  PRIMARY KEY (`id`),
  KEY `publish_date` (`publish_date`),
  KEY `author_id` (`author_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `hupu_comment` (
  `id` int(11) NOT NULL,
  `article_id` int(11) NOT NULL COMMENT '文章ID',
  `publish_date` DATETIME NOT NULL COMMENT '发布时间',
  `author` varchar(100) NOT NULL COMMENT '作者',
  `author_id` varchar(50) NOT NULL COMMENT '作者ID',
  `comment` text COMMENT '评论内容',
  `reply_comment` text COMMENT '被回复评论内容',
  PRIMARY KEY (`id`),
  KEY `publish_date` (`publish_date`),
  KEY `author_id` (`author_id`),
  KEY `article_id` (`article_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE `hupu_keywards` (
  `keyword` varchar(200) NOT NULL COMMENT '关键字'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;