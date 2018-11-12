#!/usr/bin/env python3

SELECT_MAPS = '''
SELECT src_tag,dst_tag,weight
FROM orb.tag_map;
'''

SELECT_COUNT_OF_TAGS = '''
SELECT count(distinct hashtag)
FROM orb.tag
WHERE not isnull(rank_q)
or not isnull(rank_p);
'''