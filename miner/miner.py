#!/usr/bin/env python3

import time

from traceback import format_exc as Trace
from kframe.base import Plugin

from .queries import *


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
                    for src, dst, weight in self.P.sql.select(SELECT_ALL_NEAR_POINTS.format(points=points)):
                        add(self.cache, src, dst, weight)
                        add(self.cache, dst, src, weight)
                        c += 1
                    for i in bz:
                        self.used.add(i)
        except Exception as e:
            self.Error('add near points: {}', e)
        if lll != len(self.used):
            self.Debug('Cached has {} points ({})', len(self.used), c)

    # tags - list of str
    def cache_tags(self, tags):
        lll = len(self.map_tag)
        try:
            for tag, _id in self.P.sql.select(
                SELECT_ALL_TAGS.format(
                    tags="','".join(
                        filter(
                            lambda x: x not in self.map_tag,
                            tags
                        )
                    )
                )
            ):
                tag = tag.decode()
                self.map_tag[tag] = _id
                self.map_id[_id] = tag
        except Exception as e:
            self.Error('cache tags: {}', e)
            self.Debug('cache tags: {}', Trace())
        if lll != len(self.map_tag):
            self.Debug('Cached has {} tags', len(self.map_tag))

    def insert_nearest(self, points):
        c = 0
        cc = len(points) / 100
        for i in points:
            # az = []
            self._get_near(i=i, points=points)
            for j in points:
                if j not in self.cache.get(i, {}):
                    self.route(i, j)
                    # w = self.route(i, j)
                    # add(self.cache, i, j, w)
                    # add(self.cache, j, i, w)
                    # az.append('({i},{j},{weight}),({j},{i},{weight})'.format(
                    #     i=i,
                    #     j=j,
                    #     weight=w
                    # ))
            c += 1
            if c % 10 == 0:
                self.Debug("left: %.2f %%" % (c / cc))
            # if len(az) > 0:
            #     print('||az||=', len(az))
            #     x = 1000
            #     while len(az) > 0:
            #         self.P.sql.execute(
            #             '''INSERT IGNORE INTO work.graph (src,dst,weight) VALUES %s''' % ','.join(az[:x]),
            #             commit=True,
            #         )
            #         az = az[x:]

    def _route(self, j, points):
        best = None
        been = set()
        self._get_near(i=j, points=[j])
        c = 0
        while len(points) > 0:
            c += 1
            i, already_weight = points.pop(0)
            if best is None or already_weight < best:
                self._get_near(i=i, points=[i] + list(map(lambda x: x[0], points)))
                if j in self.cache.get(i, {}):
                    w = already_weight + self.cache[i][j]
                    if best is None or w < best:
                        best = w
                elif i in self.cache.get(j, {}):
                    w = already_weight + self.cache[j][i]
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
                        w = already_weight + self.cache[i][pt]
                        if best is None or w < best:
                            points.append((pt, w))
                            been.add(pt)
        self.Debug('loops: {}', c)
        return best

    def route(self, i, j, save=True):
        weight = self._route(
            j=j,
            points=[
                (i, 0.0)
            ]
        )
        add(self.cache, i, j, weight)
        add(self.cache, j, i, weight)
        if save:
            self.P.sql.execute(
                INSERT_WEIGHT_REWRITE.format(
                    i=i,
                    j=j,
                    weight=weight
                ),
                commit=True,
            )
        return weight


class Miner(Plugin):

    def init(self):
        self.router = self.P.add_plugin(target=Router, key="router", autostart=False).init_plugin(key="router")
        self.FATAL, self.errmsg = self.router.FATAL, self.router.errmsg

    # tags - dict: tag => weight
    def relevante(self, user_tags, post_tags):
        s = {i for i in user_tags}
        for i in post_tags:
            s.add(i)
        self.router.cache_tags([t for t in s])
        user_profile = {self.router.map_tag[tag] for tag in user_tags}
        post_profile = {self.router.map_tag[tag] for tag in post_tags}
        return sum([
            sum([
                self.router.route(i, j)
                for j in post_profile
            ])
            for i in user_profile
        ]) / (len(user_profile) * len(post_profile))

    def start(self):
        _t = time.time()
        # x = self.router.route(10400, 152982)
        # x = self.router.route(8234, 194358)
        # self.Debug("ROUTER: X = %s" % x)
        self.router.insert_nearest(list(range(1, 10**2)))
        return
        user_tags = {
            'футбол': 1,
            'фифа': 1,
            'fifa': 1,
        }
        post_tags = [
            ('latex', {
                # 'sex': 1,
                'latex': 1,
            }),
            ('bdsm', {
                'femdom': 1,
                'bdsm': 1,
            }),
            ('cat', {
                'кот': 1,
                'котик': 1,
            }),
            ('music', {
                'музыка': 1,
                'music': 1,
            }),
            ('kpop', {
                'kpop': 1,
                'bts': 1,
            })
        ]
        for i in sorted(
            list(map(
                lambda x: (
                    x[0],
                    x[1],
                    self.relevante(
                        user_tags=user_tags,
                        post_tags=x[1]
                    )
                ),
                post_tags
            )),
            key=lambda x: x[2]
        ):
            self.Debug("relevante {} - {}", i[0], i[2])
        self.Debug('time: {}', time.time() - _t)
        self.P.stop()

    def stop(self, wait):
        pass
