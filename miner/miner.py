#!/usr/bin/env python3

from kframe.base import Plugin

from .queries import *

class Router(Plugin):
	def init(self):
		self.cache = {}
		self.used = set()

	def _get_near(self, i, points):
		def add(d, i, j, v):
			if i not in d:
				d[i] = {j:v}
			else:
				d[i][j] = v
			return d
		l = len(self.used)
		try:
			if i not in self.cache:
				bz = list(filter(lambda x: x not in self.used, points))
				if len(bz) > 0:
					points=",".join(list(map(lambda x: str(x), bz)))
					for src, dst, weight in self.P.sql.select(SELECT_ALL_NEAR_POINTS.format(points=points)):
						add(self.cache, src, dst, weight)
						add(self.cache, dst, src, weight)
					for i in bz:
						self.used.add(i)
		except Exception as e:
			self.Error('add near points: {}', e)
		if l != len(self.used):
			self.Debug('Cached has {} points', len(self.used))

	def insert_nearest(self, points):
		az = []
		while len(points) > 0:
			i = points[0]
			self._get_near(i=i, points=points)
			self._get_near(i=i, points=list(self.cache[i].keys()))
			points.pop(0)
			for i in self.cache:
				az += [
					'({i},{j},{weight}),({j},{i},{weight})'.format(
						i=i,
						j=j,
						weight=self.cache[i][j] if j in self.cache[i] else \
							self.cache[j][i] if i in self.cache[j] else \
							self.route(i, j)
					) for j in self.cache.get(i, {})
				]
		print(len(az))
		self.P.sql.execute('''INSERT IGNORE INTO work.graph (src,dst,weight) VALUES %s
		''' % ','.join(az), commit=True)

	def _route(self, j, points):
		been = set()
		while len(points) > 0:
			i, already_weight = points[0]
			self._get_near(i=i, points=list(map(lambda x: x[0], points)))
			points.pop(0)
			if j in self.cache.get(i, {}):
				print(i)
				return already_weight + self.cache[i][j]
			for pt in filter(lambda x: x not in been, self.cache.get(i, {})):
				points.append((pt, already_weight + self.cache[i][pt]))
				been.add(pt)
		return None

	def route(self, i, j):
		print('inc')
		return self._route(
			j=j, 
			points=[
				(i, 0.0)
			]
		)

class Miner(Plugin):
	def init(self):
		self.P.add_plugin(target=Router, key="router", autostart=False)
		#self.P.sql.execute(CONVERT_MAP_TO_GRAPH, commit=True)
	
	def start(self):
		router = self.P.init_plugin(key="router")
		# x = router.route(10400, 152982)
		# x = router.route(8234, 194358)
		# self.log("ROUTER: X = %s"%x)
		router.insert_nearest(list(range(665, 668)))

		self.P.stop()

	def stop(self, wait):
		pass