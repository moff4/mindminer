
SELECT_ALL_NEAR_POINTS = '''
SELECT src,dst,weight,sure
from work.graph
where src in ({points})
or dst in ({points});
'''
# points - int,int,...,int

SELECT_ALL_TAGS = '''
SELECT t.hashtag, t.id, t.rank
from work.tag t
where hashtag in ('{tags}')
'''
# tags - str,str,..,str

INSERT_WEIGHT_REWRITE = '''
INSERT INTO work.graph (src,dst,weight,sure)
VALUES ({i},{j},{weight},{sure}),({j},{i},{weight},{sure})
ON DUPLICATE KEY UPDATE weight={weight};
'''
# i, j, weight, sure

CONVERT_MAP_TO_GRAPH = '''
INSERT IGNORE INTO work.graph
(
    SELECT src, dst, 1.0/weight as weight, 0 as sure
    FROM work.map
)
UNION
(
    SELECT src as dst, dst as src, 1.0/weight as weight, 0 as sure
    FROM work.map
);
'''
