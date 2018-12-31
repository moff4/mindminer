
SELECT_ALL_NEAR_POINTS = '''
SELECT src,dst,weight
from work.graph
where src in ({points})
or dst in ({points});
'''
# points - int,int,...,int

SELECT_ALL_TAGS = '''
SELECT t.hashtag, t.id
from work.tag t
where hashtag in ('{tags}')
'''
# tags - str,str,..,str

INSERT_WEIGHT_REWRITE = '''
INSERT INTO work.graph (src,dst,weight)
VALUES ({i},{j},{weight}),({j},{i},{weight})
ON DUPLICATE KEY UPDATE weight={weight};
'''
# i, j, weight

CONVERT_MAP_TO_GRAPH = '''
INSERT IGNORE INTO work.graph
(
    SELECT src, dst, 1.0/weight as weight
    FROM work.map
)
UNION
(
    SELECT src as dst, dst as src, 1.0/weight as weight
    FROM work.map
);
'''
