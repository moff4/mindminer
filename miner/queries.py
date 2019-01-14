
SELECT_ALL_NEAR_POINTS = '''
SELECT src,dst,weight,sure
from work.graph
where src in ({points})
or dst in ({points})
limit {offset}, {limit};
'''
# points - int,int,...,int
# limit - int
# offset - int

SELECT_ALL_FAR_POINTS = '''
SELECT dst, count(*) as c
FROM work.graph
group by dst
order by c
limit {limit}
'''
# limit - int

SELECT_ALL_TAGS = '''
SELECT t.hashtag, t.id, t.rank
from work.tag t
where hashtag in ('{tags}')
'''
# tags - str,str,..,str

INSERT_WEIGHT_REWRITE = '''
INSERT IGNORE INTO work.graph (src,dst,weight,sure)
VALUES
{values}
'''
# values - ({i},{j},{weight},{sure}),({j},{i},{weight},{sure})
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
