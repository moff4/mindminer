#!/usr/bin/env python3

SELECT_MAPS = '''
SELECT distinct hashtag
FROM orb.tag
WHERE not isnull(rank_q)
or not isnull(rank_p);
''' 

SELECT_COUNT_OF_TAGS = '''
SELECT count(distinct hashtag)
FROM orb.tag
WHERE not isnull(rank_q)
or not isnull(rank_p);
'''

SELECT_WEIGHT = '''
SELECT (sum(weight_1) + sum(weight_2))/2
FROM 
(
	SELECT 0 as weight_1
	UNION
	(
		SELECT weight as weight_1
		FROM orb.tag_map
		WHERE src_tag = '{src_tag}'
		AND dst_tag = '{dst_tag}'
		ORDER BY id DESC
		LIMIT 1
	)
) t1,
(
	SELECT 0 as weight_2
	UNION
	(
		SELECT weight as weight_2
		FROM orb.tag_map
		WHERE src_tag = '{dst_tag}'
		AND dst_tag = '{src_tag}'
		ORDER BY id DESC
		LIMIT 1
	)
) t2
''' # format( src_tag , dst_tag )


# FIXME test
CREAT_WORK_TABLE = '''
CREATE TABLE IF NOT EXISTS `work`.`tag` 
(
	`id` int UNSIGNED NOT NULL AUTO_INCREMENT,
	`hashtag` varchar(256) NOT NULL,
	`rank` float NOT NULL,
	PRIMARY KEY (`id`)
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;

INSERT INTO work.tag
(
	SELECT t1.hashtag, rank_q as rank
	FROM
	(
		SELECT hashtag,max(id) as id
		FROM orb.tag
		WHERE not isnull(rank_p)
		AND  not isnull(rank_q)
		and rank_q > 0
		GROUP BY hashtag
	) t1, 
	orb.tag as t2
	WHERE t1.id = t2.id
);

CREATE TABLE IF NOT EXISTS `work`.`map` 
(
	`src` int UNSIGNED NOT NULL,
	`dst` int UNSIGNED NOT NULL,
	`weight` float NOT NULL,
	PRIMARY KEY (`src`,`dst`)
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;

SELECT t1.id as src,t2.id as dst,t3.weight
FROM 
(
SELECT t1.hashtag, rank_q as rank
	FROM
	(
		SELECT hashtag,max(id) as id
		FROM orb.tag
		WHERE not isnull(rank_p)
		AND  not isnull(rank_q)
		and rank_q > 0
		GROUP BY hashtag
	) t1, 
	orb.tag as t2
	WHERE t1.id = t2.id
) t1,
(
SELECT t1.hashtag, rank_q as rank
	FROM
	(
		SELECT hashtag,max(id) as id
		FROM orb.tag
		WHERE not isnull(rank_p)
		AND  not isnull(rank_q)
		and rank_q > 0
		GROUP BY hashtag
	) t1, 
	orb.tag as t2
	WHERE t1.id = t2.id
) t2,
(
	SELECT t1.src_tag,t1.dst_tag,t2.weight
	FROM
	( 
		SELECT src_tag,dst_tag,max(id) as id
		FROM orb.tag_map tm
		WHERE tm.weight > 0
		GROUP BY src_tag,dst_tag
	) t1,
	orb.tag_map t2
	WHERE t1.id = t2.id
) t3
WHERE t1.hashtag = t3.src_tag
AND t2.hashtag = t3.dst_tag
LIMIT 10;
'''
