#!/usr/bin/env python3

import time

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
        self.to_save = {}
        self.RUN = True
        # self.P.sql.execute(CONVERT_MAP_TO_GRAPH, commit=True)

    def _get_near(self, i, points, sure=None):
        lll = len(self.used)
        c = 0
        try:
            _t = time.time()
            if i not in self.used:
                bz = list(filter(lambda x: x not in self.used, points))
                if len(bz) > 0:
                    points = ','.join(list(map(lambda x: str(x), bz)))
                    sure = ','.join(
                        [
                            str(j)
                            for j in (
                                sure
                                if sure is not None else
                                range(10)
                            )
                        ]
                    )
                    for src, dst, weight, sure in self.P.sql.select(
                        SELECT_ALL_NEAR_POINTS.format(
                            points=points,
                            sure=sure,
                        )
                    ):
                        add(self.cache, src, dst, (weight, sure))
                        add(self.cache, dst, src, (weight, sure))
                        c += 1
                    for i in bz:
                        self.used.add(i)
        except Exception as e:
            self.Error('add near points: {}', e)
        if lll != len(self.used):
            self.Debug('Cached has {} points ({}) in {}', len(self.used), c, time.time() - _t)

    def cache_tags(self, tags):
        """
            tags - iterible
            load tuples (hashtag, tag_id, rank)
        """
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

    def _route(self, j, points, sure=None):
        best = None
        # been = set()
        i = points[0][0]
        src = i
        self._get_near(i=j, points=[j, i], sure=sure)
        if j not in self.cache:
            return None, False
        near_pt = set(self.cache[j].keys())
        if j in self.cache.get(i, {}):
            return self.cache[i][j][0], False
        elif i in self.cache.get(j, {}):
            return self.cache[j][i][0], False
        elif i == j:
            raise ValueError('This should not happen')
        c = 0
        while len(points) > 0 and self.RUN:
            c += 1
            i, already_weight = points.pop(0)
            if best is None or already_weight < best:
                self._get_near(i=i, points=[i] + list(map(lambda x: x[0], points)), sure=sure)
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
                    for pt in self.cache.get(i, {}).keys():
                        #  filter(
                        #     lambda x: x not in been,
                        #     self.cache.get(i, {}).keys()
                        # ):
                        if pt in near_pt:
                            w = already_weight + self.cache[i][pt][0] + self.cache[j][pt][0]
                            if best is None or w < best:
                                best = w
                        w = already_weight + self.cache[i][pt][0]
                        if best is None or w < best:
                            points.append((pt, w))
                        # been.add(pt)
        self.Debug('loops {} -> {}: {}', self.map_id[src][0], self.map_id[j][0], c)
        return best, True

    def _route_many(self, desteny, i):
        """
            desteny - list or tuple of pts
            return dict of weights and flag if there is need to save this data
        """
        res = {}
        been = set()
        self._get_near(i=None, points=[i] + list(desteny))
        dst = {}
        for j in desteny:
            if j not in self.cache:
                res[j] = None
            if j in self.cache.get(i, {}):
                dst[j] = self.cache[i][j][0]
            elif i in self.cache.get(j, {}):
                dst[j] = self.cache[j][i][0]
            elif i == j:
                dst[j] = 1.0 / len(self.cache[j])
            else:
                dst[j] = None
        c = 0
        points = [
            (i, {
                key: 0.0
                for key in dst
            })
        ]
        while len(points) > 0 and self.RUN:
            c += 1
            pts = list(map(lambda x: x[0], points))
            new_pts = {}
            i, al_w = points.pop(0)
            for j in al_w:
                if dst[j] is None or al_w[j] < dst[j]:
                    self._get_near(i=i, points=pts)
                    if j in self.cache.get(i, {}):
                        w = al_w[j] + self.cache[i][j][0]
                        if dst[j] is None or w < dst[j]:
                            dst[j] = w
                    elif i in self.cache.get(j, {}):
                        w = al_w[j] + self.cache[j][i][0]
                        if dst[j] is None or w < dst[j]:
                            dst[j] = w
                    elif i == j:
                        if dst[j] is None or al_w[j] < dst[j]:
                            dst[j] = al_w[j]
                    elif dst[j] is None or al_w[j] <= dst[j]:
                        for pt in filter(
                            lambda x: x not in been,
                            self.cache.get(i, {}).keys()
                        ):
                            if pt in self.cache.get(j, {}):
                                w = al_w[j] + self.cache[i][pt][0] + self.cache[j][pt][0]
                                if dst[j] is None or w < dst[j]:
                                    dst[j] = w
                            w = al_w[j] + self.cache[i][pt][0]
                            if dst[j] is None or w < dst[j]:
                                if pt not in new_pts:
                                    new_pts[pt] = {j: w}
                                else:
                                    new_pts[pt].update({j: w})
            for pt in new_pts:
                points.append((pt, new_pts[pt]))
                been.add(pt)
        self.Debug('loops: {}', c)
        res.update(dst)
        return res, any(map(lambda x: dst[x] is not None, dst))

    def route_many(self, i, dst, save=True):
        dst, _ = self._route_many(desteny=dst, i=i)
        return dst

    def route(self, i, j, save=True, sure=None):
        weight, _save = self._route(
            j=j,
            points=[
                (i, 0.0)
            ],
            sure=sure,
        )
        if weight is None:
            return None
        if weight == 0.0:
            return 10**(-10)
        add(self.cache, i, j, (weight, 1))
        add(self.cache, j, i, (weight, 1))
        if save and _save:
            key = str(min(i, j)) + '@' + str(max(i, j))
            self.to_save[key] = {
                'i': i,
                'j': j,
                'weight': weight,
                'sure': 1
            }
        return weight

    def save(self):
        if len(self.to_save) > 0:
            try:
                az = []
                for key in self.to_save:
                    az.append("({i},{j},{weight},{sure}),({j},{i},{weight},{sure})".format(
                        **self.to_save[key]
                    ))
                self.P.sql.execute(
                    INSERT_WEIGHT_REWRITE.format(
                        values=','.join(
                            az
                        )
                    )
                )
                self.Debug('saved {} rows', len(self.to_save))
                self.to_save = {}
            except Exception as e:
                self.Error('save: {}', e)
                self.Debug('save: {}', Trace())

    def rank(self, point):
        return self.map_id[point][1] if point in self.map_id else None

    def stop(self, *args, **kwargs):
        self.RUN = False
        self.save()
