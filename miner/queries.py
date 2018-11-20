#!/usr/bin/env python3

SELECT_MAPS = '''
SELECT t1.id,t2.id,m.weight,m.timestamp
FROM orb.tag_map m, 
	work.tag t1, 
	work.tag t2
WHERE m.timestamp <= {timestamp}
AND m.src_tag = t1.hashtag
AND m.dst_tag = t2.hashtag
AND 0 = (
	SELECT count(*) as c
	FROM work.map map
	WHERE ( map.src = t1.id
		AND map.dst = t2.id )
	OR ( map.src = t2.id
		AND map.dst = t1.id )
)
ORDER BY m.timestamp DESC
''' # format(timestamp)

INSERT_MAP = '''
INSERT IGNORE INTO work.map 
(src,dst,weight)
VALUES
{values}
''' # format( values = '({src},{dst},{weight})' )

SELECT_COUNT_OF_TAGS = '''
SELECT count(*)
FROM work.tag
'''

CREAT_WORK_TABLES = [ 
'''
CREATE TABLE IF NOT EXISTS `work`.`tag` 
(
	`id` int UNSIGNED NOT NULL AUTO_INCREMENT,
	`hashtag` varchar(256) NOT NULL,
	`rank` float NOT NULL,
	PRIMARY KEY (`id`)
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;
''',
'''
INSERT INTO work.tag
(hashtag,rank)
(
	SELECT t1.hashtag, rank_q as rank
	FROM
	(
		SELECT hashtag,max(id) as id
		FROM orb.tag t1 
		WHERE hashtag not in (
			SELECT hashtag
			FROM work.tag
		)
		AND not isnull(rank_p)
		AND not isnull(rank_q)
		AND rank_q > 0
		GROUP BY hashtag
	) t1, 
	orb.tag as t2
	WHERE t1.id = t2.id
);
''',
'''
CREATE TABLE IF NOT EXISTS `work`.`map` 
(
	`src` int UNSIGNED NOT NULL,
	`dst` int UNSIGNED NOT NULL,
	`weight` float NOT NULL,
	PRIMARY KEY (`src`,`dst`)
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;
'''
]

SELECT_MAPS


