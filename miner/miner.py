#!/usr/bin/env python3

import math

from kframe.base import Plugin

from .router import Router


class Miner(Plugin):
    name = 'miner'

    def init(self):
        self.router = self.P.fast_init(target=Router)
        self.FATAL, self.errmsg = self.router.FATAL, self.router.errmsg

    def cache_tags(self, tags):
        self.router.cache_tags(tags)

    def save(self):
        self.router.save()

    def relevante(self, user_tags, post_tags, top=False):
        """
            *_tags - dict: tag(str) => weight(float) or list of tags(str)
            top - bool: use classic of fast(if possible) alg
            tag - hashtag witghout '#' at the begining
            return flaot
        """
        def f(tags):
            return {tag: 1 for tag in tags} if isinstance(tags, tuple) or isinstance(tags, list) else tags

        def q(tags, top=False):
            return {
                self.router.map_tag[tag][2]
                if top and self.router.map_tag[tag][2] else
                self.router.map_tag[tag][0]: tags[tag]
                for tag in filter(
                    lambda x: x in self.router.map_tag,
                    tags
                )
            }

        self.Debug('Gonna run Miner{} alg', 'Top' if top else 'Original')

        user_tags = f(user_tags)
        post_tags = f(post_tags)
        user_profile = q(user_tags)
        post_profile = q(post_tags, top)

        if not user_profile or not post_profile:
            return 0.0

        self.router.cache_tags(set(user_tags).union(set(post_tags)))

        s = 0.0
        c = 0
        ut_s = sum(user_profile.values())
        for i in user_profile:
            _i = self.router.rank(i)
            ut_w = user_profile[i]
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
                            save=True,
                            sure={0, 1, 2},
                        )
                        if w is not None:
                            c += 1
                            ds = _j / w
                            _s += ds * ds
                s += _i * _s * ut_w / ut_s
        w = s / c if c > 0 else 0.0
        return math.sqrt(w)

    def stop(self, *args, **kwargs):
        self.router.stop(args, **kwargs)
