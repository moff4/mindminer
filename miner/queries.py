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