#!/usr/bin/env python3

import time
import math
from kframe.base import Plugin

from .router import Router


class Miner(Plugin):

    def init(self):
        self.router = self.P.add_plugin(target=Router, key="router", autostart=False).init_plugin(key="router")
        self.FATAL, self.errmsg = self.router.FATAL, self.router.errmsg

    def cache_tags(self, tags):
        self.router.cache_tags(tags)

    # tags - dict: tag => weight
    def relevante(self, user_tags, post_tags, flag=True, many=False):
        s = {i for i in user_tags}
        for i in post_tags:
            s.add(i)
        self.router.cache_tags([t for t in s])
        user_profile = {self.router.map_tag[tag][0] for tag in filter(lambda x: x in self.router.map_tag, user_tags)}
        post_profile = {self.router.map_tag[tag][0] for tag in filter(lambda x: x in self.router.map_tag, post_tags)}
        if len(user_profile) <= 0 or len(post_profile) <= 0:
            return None
        s = 0.0
        c = 0
        for i in user_profile:
            _i = self.router.rank(i)
            if _i is not None and _i > 0:
                _i = math.log10(_i + 1)
                _s = 0.0
                for j in post_profile:
                    _j = self.router.rank(j)
                    if _j is not None and _j > 0:
                        _j = math.log10(_j + 1)
                        w = self.router.route(
                            i=i,
                            j=j,
                            save=True
                        )
                        if w is not None:
                            c += 1
                            ds = _j / w
                            _s += ds * ds if flag else ds
                s += _i * _s
        w = s / c if c > 0 else 0.0
        return math.sqrt(w) if flag else w

    def test_relevante(self):
        user_tags = {
            'football': 1,
            'фифа': 1,
            'fifa': 1,
        }
        post_tags = [
            # ('latex', {
            #     'sex': 1,
            #     'latex': 1,
            # }),
            # ('hentai', {
            #     'hentai': 1,
            #     'anime': 1,
            #     'salormoon': 1,
            # }),
            # ('bdsm', {
            #     'femdom': 1,
            #     'bdsm': 1,
            # }),
            # ('cat', {
            #     'кот': 1,
            #     'котик': 1,
            # }),
            # ('music', {
            #     'музыка': 1,
            #     'music': 1,
            # }),
            # ('kpop', {
            #     'kpop': 1,
            #     'bts': 1,
            # }),
            ('fifa', {
                'fifa': 1,
                'футбол': 1,
            }),
            ('fifa more', {
                'fifa': 1,
                'футбол': 1,
                'football': 1,
                'kpop': 1,
            })
        ]
        az = []
        for tag in post_tags:
            az += [t for t in tag[1].keys()]
        self.router.cache_tags(az)
        az = list(map(
            lambda x: (
                x[0],
                x[1],
                self.relevante(
                    user_tags=user_tags,
                    post_tags=x[1],
                    flag=False
                ),
                self.relevante(
                    user_tags=user_tags,
                    post_tags=x[1],
                    flag=True
                )
            ),
            post_tags
        ))
        for i in sorted(
            az,
            key=lambda x: x[2]
        ):
            self.Debug("relevante {} - \nA {}\nS {}\n", i[0], i[2], i[3])

    def test_nearest(self):
        points = [
            34155,
            # 22173,
            # 18519,
            # 17817,
            # 20517,
            # 44341
        ]
        for i in points:
            self.router.insert_nearest(
                point=i
            )
            # self.router.reset()
        self.router.save()

    def test_route(self):
        pts = {
            34155,
            22173,
            18519,
            17817,
            20517,
        }
        j = 44341
        _t = time.time()
        for i in pts:
            x = self.router.route(i, j, save=False)
            self.Debug("ROUTER: X({},{}) = %s" % x, i, j)
        tx = time.time() - _t
        self.router.reset()
        _t = time.time()
        y = self.router.route_many(j, dst=pts, save=False)
        ty = time.time() - _t
        self.router.reset()
        for k in y:
            self.Debug("ROUTER: Y({},{}) = %s" % y[k], k, j)
        self.Debug('Tx = {}', tx)
        self.Debug('Ty = {}', ty)
        # x = self.router.route(10400, 152982, save=False)
        # self.Debug("ROUTER: X = %s" % x)
        # x = self.router.route(8234, 194358, save=False)
        # self.Debug("ROUTER: X = %s" % x)

    def start(self):
        _t = time.time()
        # self.test_relevante()
        self.test_route()
        # self.test_nearest()
        self.Debug('time: {}', time.time() - _t)
        self.P.stop()

    def stop(self, wait):
        pass
