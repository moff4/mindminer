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

    def reset(self):
        self.cache = {}
        self.used = set()
        self.map_tag = {}
        self.map_id = {}
        self.to_save = {}

    def _get_near(self, i, points, limit=10000, _all=False):
        lll = len(self.used)
        c = 0
        try:
            _t = time.time()
            if i not in self.used:
                bz = list(filter(lambda x: x not in self.used, points))
                if len(bz) > 0:
                    query = SELECT_ALL if _all else SELECT_ALL_NEAR_POINTS
                    points = ",".join(list(map(lambda x: str(x), bz)))
                    j = 0
                    x = limit
                    while (x == limit):
                        x = 0
                        for src, dst, weight, sure in self.P.sql.select(
                            query.format(
                                points=points,
                                limit=limit,
                                offset=limit * j
                            ),
                            unique_cursor=True
                        ):
                            add(self.cache, src, dst, (weight, sure))
                            add(self.cache, dst, src, (weight, sure))
                            x += 1
                        j += 1
                        c += x
                    if _all:
                        self.used = set(self.cache.keys())
                    else:
                        for i in bz:
                            self.used.add(i)
        except Exception as e:
            self.Error('add near points: {}', e)
        if lll != len(self.used):
            self.Debug('Cached has {} points ({}) in %.3f' % (time.time() - _t), len(self.used), c)

    # tags - iterible
    def cache_tags(self, tags):
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

    def __optimization(self, points):
        az = {}
        for i, al in points:
            if i not in az or az[i] > al:
                az[i] = al
        return sorted(
            [(i, az[i]) for i in az],
            key=lambda x: x[1]
        )

    def insert_nearest(self, point):
        points = [
            (point, 0.0)
        ]
        c = 0
        _lr = 0
        local_map = {}  # dst => weight
        try:
            while len(points) > 0 and self.RUN:
                i, already_weight = points.pop(0)
                if i not in local_map or already_weight < local_map[i]:
                    self._get_near(
                        i=i,
                        points=[i] + list(map(lambda x: x[0], points)),
                        _all=True
                    )
                    for pt in filter(
                        lambda x: self.cache[i][x][1] == 0,
                        self.cache.get(i, {}).keys()
                    ):
                        w = already_weight + self.cache[i][pt][0]
                        if (pt not in local_map or w < local_map[pt]) and (pt != point):
                            local_map[i] = w
                            points.append((pt, w))
                c += 1
                if c % 1000 == 0:
                    if len(points) >= 1.2 * len(self.cache):
                        points = self.__optimization(points)
                    r = len(points) - _lr
                    self.Debug(
                        'loop {}, points {} ({}{}) DB: {}',
                        c,
                        len(points),
                        '+' if r >= 0 else '-',
                        abs(r),
                        len(local_map)
                    )
                    _lr = len(points)
        except KeyboardInterrupt:
            self.Debug('gonna be inserted {} rows', len(local_map))
        i = point
        for j in local_map:
            add(self.cache, i, j, (local_map[i], 1))
            add(self.cache, j, i, (local_map[i], 1))
            self.to_save[str(min(i, j)) + '@' + str(max(i, j))] = {
                'i': i,
                'j': j,
                'weight': local_map[i],
                'sure': 2
            }

    def _route(self, j, points):
        best = None
        been = set()
        i = points[0][0]
        self._get_near(i=j, points=[j, i])
        if j not in self.cache:
            return None, False
        near_pt = set(self.cache[j].keys())
        if j in self.cache.get(i, {}):
            return self.cache[i][j][0], False
        elif i in self.cache.get(j, {}):
            return self.cache[j][i][0], False
        elif i == j:
            if j in self.cache.get(i, {}):
                return self.cache[i][j][0], False
            else:
                return 1.0 / len(self.cache[i]), False
            raise ValueError('This should not happen')
        c = 0
        while len(points) > 0 and self.RUN:
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
                        lambda x: x not in been and x in near_pt,
                        self.cache.get(i, {}).keys()
                    ):
                        w = already_weight + self.cache[i][pt][0] + self.cache[j][pt][0]
                        if best is None or w < best:
                            best = w
                    for pt in sorted(
                        filter(
                            lambda x: x not in been,
                            self.cache.get(i, {}).keys()
                        ),
                        key=lambda x: already_weight + self.cache[i][x][0]
                    ):
                        w = already_weight + self.cache[i][pt][0]
                        if best is None or w < best:
                            points.append((pt, w))
                        been.add(pt)
        self.Debug('loops: {}', c)
        return best, True

    # desteny - list or tuple of pts
    # return dict of weights and flag if there is need to save this data
    def _route_many(self, desteny, i):
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

    def route(self, i, j, save=True):
        weight, _save = self._route(
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
                K = 10000
                while len(az) > 0:
                    bz = az[:K]
                    self.P.sql.execute(
                        INSERT_WEIGHT_REWRITE.format(
                            values=','.join(bz)
                        ),
                        commit=True,
                    )
                    az = az[K:]
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
