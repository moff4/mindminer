
SELECT_ALL_NEAR_POINTS = '''
SELECT src,dst,weight
from work.graph
where src in ({points})
or dst in ({points});
''' # points - int,int,...,int

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