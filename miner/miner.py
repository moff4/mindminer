#!/usr/bin/env python3

import time
import math

from kframe.base import Plugin

from .router import Router


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
        user_profile = {self.router.map_tag[tag][0] for tag in user_tags}
        post_profile = {self.router.map_tag[tag][0] for tag in post_tags}
        if len(user_profile) <= 0 or len(post_profile) <= 0:
            return None
        return sum([
            sum([
                math.log10(self.router.rank(i)) * math.log10(self.router.rank(j)) / self.router.route(
                    i=i,
                    j=j,
                    eps=True,
                    save=True
                )
                for j in post_profile
            ])
            for i in user_profile
        ]) / (len(user_profile) * len(post_profile))

    def start(self):
        _t = time.time()
        # x = self.router.route(10400, 152982)
        # x = self.router.route(8234, 194358)
        # self.Debug("ROUTER: X = %s" % x)
        # self.router.insert_nearest(list(range(1, 10**2)))
        # return
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
