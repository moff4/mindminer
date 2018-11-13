#!/usr/bin/env python3

SELECT_MAPS = '''
SELECT src_tag,dst_tag
FROM orb.tag_map;
'''

SELECT_COUNT_OF_TAGS = '''
SELECT count(distinct hashtag)
FROM orb.tag
WHERE not isnull(rank_q)
or not isnull(rank_p);
'''

SELECT_WEIGHT = '''
SELECT weight
FROM orb.tag_map
WHERE src_tag = '{src_tag}'
AND dst_tag = '{dst_tag}'
ORDER BY id DESC
LIMIT 1;
''' # format( src_tag , dst_tag )