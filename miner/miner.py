#!/usr/bin/env python3

import time
import math
from multiprocessing import Pool
from kframe.base import Plugin

from .router import Router


class Miner(Plugin):

    def init(self):
        self.router = self.P.add_plugin(target=Router, key="router", autostart=False).init_plugin(key="router")
        self.FATAL, self.errmsg = self.router.FATAL, self.router.errmsg

    def cache_tags(self, tags):
        self.router.cache_tags(tags)

    # tags - dict: tag => weight
    def relevante(self, user_tags, post_tags):
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
            if _i > 0:
                _i = math.log10(_i)
                for j in post_profile:
                    _j = self.router.rank(j)
                    if _j > 0:
                        _j = math.log10(_j)
                        w = self.router.route(
                            i=i,
                            j=j,
                            eps=True,
                            save=True
                        )
                        if w is not None:
                            c += 1
                            s += _i * _j / w
        return (s / c) if c > 0 else 0.0

    def test_relevante(self):
        user_tags = {
            'football': 2,
            'фифа': 1,
            'fifa': 1,
        }
        post_tags = [
            ('latex', {
                'sex': 1,
                'latex': 1,
            }),
            ('hentai', {
                'hentai': 1,
                'anime': 1,
                'salormoon': 1,
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
            }),
            ('fifa', {
                'fifa': 1,
                'футбол': 1,
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
                    post_tags=x[1]
                )
            ),
            post_tags
        ))
        for i in sorted(
            az,
            key=lambda x: x[2]
        ):
            self.Debug("relevante {} - {}", i[0], i[2])

    def test_nearest(self):
        offset = 100
        count = 10
        az = list(range(offset, offset + count))
        self.router.insert_nearest(az)
        # az = [az[i * x:][:x] for i in range(P)]
        # print(az)
        # with Pool(5) as p:
        #     p.map(self.router.insert_nearest, az)

    def start(self):
        _t = time.time()
        # x = self.router.route(10400, 152982)
        # x = self.router.route(8234, 194358)
        # self.Debug("ROUTER: X = %s" % x)
        # return
        self.test_relevante()
        self.Debug('time: {}', time.time() - _t)
        self.P.stop()

    def stop(self, wait):
        pass
