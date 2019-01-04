#!/usr/bin/env python3

from kframe.base import Plugin
from .queries import *

from traceback import format_exc as Trace


def add(d, i, j, v):
    if i not in d:
        d[i] = {j: v}
    else:
        d[i][j] = v
    return d


class Router(Plugin):

    def init(self):
        self.cache = {}
        self.used = set()
        self.map_tag = {}
        self.map_id = {}
        # self.P.sql.execute(CONVERT_MAP_TO_GRAPH, commit=True)

    def _get_near(self, i, points):
        lll = len(self.used)
        c = 0
        try:
            if i not in self.used:
                bz = list(filter(lambda x: x not in self.used, points))
                if len(bz) > 0:
                    points = ",".join(list(map(lambda x: str(x), bz)))
                    for src, dst, weight, sure in self.P.sql.select(SELECT_ALL_NEAR_POINTS.format(points=points)):
                        add(self.cache, src, dst, (weight, sure))
                        add(self.cache, dst, src, (weight, sure))
                        c += 1
                    for i in bz:
                        self.used.add(i)
        except Exception as e:
            self.Error('add near points: {}', e)
        if lll != len(self.used):
            self.Debug('Cached has {} points ({})', len(self.used), c)

    # tags - iterible
    def cache_tags(self, tags):
        lll = len(self.map_tag)
        try:
            for tag, _id, rank in self.P.sql.select(
                SELECT_ALL_TAGS.format(
                    tags="','".join(
                        filter(
                            lambda x: x not in self.map_tag,
                            tags
                        )
                    )
                )
            ):
                if type(tag) != str:
                    tag = tag.decode()
                self.map_tag[tag] = (_id, rank)
                self.map_id[_id] = (tag, rank)
            self._get_near(None, self.map_id.keys())
        except Exception as e:
            self.Error('cache tags: {}', e)
            self.Debug('cache tags: {}', Trace())
        if lll != len(self.map_tag):
            self.Debug('Cached has {} tags', len(self.map_tag))

    def insert_nearest(self):
        k = 0
        c = 0
        try:
            for i, count in self.P.sql.select(SELECT_ALL_FAR_POINTS.format(limit=1)):
                self._get_near(i=i, points=[i])
                self._get_near(i=None, points=self.cache.keys())
                points = list(self.cache.keys())
                for j in filter(lambda j: j != i, points):
                    if j not in self.cache.get(i, {}):
                        self.route(i, j, save=True)
                        k += 1
                    c += 1
                    self.Debug('done %.2f' % (c / len(points) * 100.0))
        except KeyboardInterrupt:
            self.Debug('saved {} new routes', k)

    def _route(self, j, points):
        best = None
        been = set()
        i = points[0][0]
        self._get_near(i=j, points=[j, i])
        if j not in self.cache:
            return best
        near_pt = set(self.cache[j].keys())
        if j in self.cache.get(i, {}):
            return self.cache[i][j][0]
        elif i in self.cache.get(j, {}):
            return self.cache[j][i][0]
        elif i == j:
            raise ValueError('This should not happen')
        c = 0
        while len(points) > 0:
            c += 1
            i, already_weight = points.pop(0)
            if best is None or already_weight < best:
                self._get_near(i=i, points=[i] + list(map(lambda x: x[0], points)))
                if j in self.cache.get(i, {}):
                    w = already_weight + self.cache[i][j][0]
                    if best is None or w < best:
                        best = w
                elif i in self.cache.get(j, {}):
                    w = already_weight + self.cache[j][i][0]
                    if best is None or w < best:
                        best = w
                elif i == j:
                    if best is None or already_weight < best:
                        best = already_weight
                elif best is None or already_weight <= best:
                    for pt in filter(
                        lambda x: x not in been,
                        self.cache.get(i, {}).keys()
                    ):
                        if pt in near_pt:
                            w = already_weight + self.cache[i][pt][0] + self.cache[j][pt][0]
                            if best is None or w < best:
                                best = w
                        w = already_weight + self.cache[i][pt][0]
                        if best is None or w < best:
                            points.append((pt, w))
                        been.add(pt)
        self.Debug('loops: {}', c)
        return best

    def route(self, i, j, save=True, eps=False):
        weight = self._route(
            j=j,
            points=[
                (i, 0.0)
            ]
        )
        if weight is None:
            return None
        if weight == 0.0:
            return 10**(-10)
        add(self.cache, i, j, (weight, 1))
        add(self.cache, j, i, (weight, 1))
        if save:
            self.P.sql.execute(
                INSERT_WEIGHT_REWRITE.format(
                    i=i,
                    j=j,
                    weight=weight,
                    sure=1
                ),
                commit=True,
            )
        return weight

    def rank(self, point):
        return self.map_id[point][1] if point in self.map_id else None
