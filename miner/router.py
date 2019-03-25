#!/usr/bin/env python3

import time

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
        self.to_save = {}
        self.RUN = True
        # self.P.sql.execute(CONVERT_MAP_TO_GRAPH, commit=True)

    def reset(self, cache=True, map=True):
        """
            reset cache
        """
        if cache:
            self.cache = {}
            self.used = set()
        if map:
            self.map_tag = {}
            self.map_id = {}

    def _get_near(self, i, points, sure=None):
        """
            i - point to load now
            points - list of points that might be loaded later
            sure - sure filter
        """
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
                        if i not in self.cache:
                            self.cache[i] = {}
                        self.used.add(i)
        except Exception as e:
            self.Error('add near points: {}', e)
            self.Trace('add near points:')
        if lll != len(self.used):
            self.Debug('Cached has {} points ({}) in {:.4f}', len(self.used), c, time.time() - _t)

    def cache_tags(self, tags=None, ids=None, get_near=True):
        """
            tags - iterible
            load tuples (hashtag, tag_id, rank)
        """
        try:
            if tags:
                query = SELECT_ALL_TAGS_BY_TAGS.format(
                    tags="','".join(
                        filter(
                            lambda x: x not in self.map_tag,
                            tags
                        )
                    )
                )
            elif ids:
                query = SELECT_ALL_TAGS_BY_IDS.format(
                    ids="','".join(
                        [str(x) for x in ids if x not in self.map_id]
                    )
                )
            elif ids is None and tags is None:
                raise ValueError('cache-tags: tags or ids must be passed')
            else:
                return
            l1 = len(self.map_tag)
            t1 = time.time()
            for tag, _id, rank, top in self.P.sql.select(query):
                if type(tag) != str:
                    tag = tag.decode()
                self.map_tag[tag] = (_id, rank, top)
                self.map_id[_id] = (tag, rank, top)
            self.Debug('cached {} new tags in {:.4f}', len(self.map_tag) - l1, time.time() - t1)
            if get_near:
                self._get_near(None, self.map_id.keys())
        except Exception as e:
            self.Error('cache tags: {}', e)
            self.Trace('cache tags:')

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
                        if pt in near_pt:
                            w = already_weight + self.cache[i][pt][0] + self.cache[j][pt][0]
                            if best is None or w < best:
                                best = w
                        w = already_weight + self.cache[i][pt][0]
                        if best is None or w < best:
                            points.append((pt, w))
        self.Debug('loops {} -> {}: {}', self.map_id[src][0], self.map_id[j][0], c)
        return best, True

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
                self.Trace('save:')

    def rank(self, point):
        return self.map_id[point][1] if point in self.map_id else None

    def set_top(self, loops=None):
        """
            set top attribute to all tags
        """
        c = 0
        pool = []
        try:
            while loops is None or loops > 0:
                if loops:
                    loops -= 1
                top_save = {}
                if pool:
                    tags = list(pool)
                    pool = []
                else:
                    tags = self.P.sql.select_all(
                        SELECT_UNTOP_RANK_TAGS.format(
                            limit=30,
                        )
                    )
                if not tags:
                    break
                for hashtag, _id, rank, top in tags:
                    self._get_near(i=_id, points=[_id])
                    self.cache_tags(ids=list(self.cache[_id]), get_near=False)
                    max_r = rank
                    top_src = None
                    for near_id in self.cache[_id]:
                        r = self.rank(near_id)
                        if r is None:
                            self.Debug('rank for {} is None', near_id)
                        elif r > max_r:
                            max_r = r
                            top_src = near_id
                            # self.map_id[near_id][2]
                    key = top_src if top_src is not None else _id
                    if key in top_save:
                        top_save[key].add(_id)
                    else:
                        top_save[key] = {_id}

                # save result
                for i in sorted(list(top_save), key=lambda x: len(top_save[x])):
                    if len(top_save[i]) == 1:
                        top_id = i
                    elif i in top_save[i]:
                        top_id = i
                    elif self.map_id[i][2] is None:
                        self.Warning('unexpected situation: i = {}'.format(i))
                        # (tag, rank, top)
                        pool.append((
                            self.map_id[i][0],
                            i,
                            self.map_id[i][1],
                            self.map_id[i][2],
                        ))
                        continue
                    else:
                        top_id = self.map_id[i][2]
                    q = UPDATE_TAGS_TOP.format(
                        top_id=top_id,
                        id_cond=(
                            'id = {}'.format(next(x for x in top_save[i]))
                            if len(top_save[i]) == 1 else
                            "id in ('{}')".format(
                                "','".join([str(x) for x in top_save[i]])
                            )
                        )
                    )
                    if self.P.sql.execute(q, commit=True)[0]:
                        c += len(top_save[i])
                self.reset(map=True, cache=False)

        except KeyboardInterrupt:
            self.Notify('Stopping')
        self.Notify('Topped {} tags', c)

    def stop(self, *args, **kwargs):
        self.RUN = False
        self.save()
