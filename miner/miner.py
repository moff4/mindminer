#!/usr/bin/env python3

import time
from traceback import format_exc as Trace
from kframe import Plugin

from .utils import Table, Row
from .queries import *

class Miner(Plugin):
	def init(self):
		pass
		
	#
	# do with time count
	#
	def do(self,target,desc=None,*args,**kwargs):
		_t = time.time()
		try:
			res = target(*args,**kwargs)
		except Exception as e:
			res = None
			self.Error("{desc} - {e}".format(desc=str(target) if desc is None else desc,e=e))
			self.Debug("{desc} - {e}".format(desc=str(target) if desc is None else desc,e=Trace()))
		self.Notify("{desc} {t} sec".format(desc=str(target) if desc is None else desc,t=time.time()-_t))
		return None

	def create_db(self):
		_c1 = self.P.sql.select_all(SELECT_COUNT_OF_TAGS)[0][0]
		j = 0
		for query in CREAT_WORK_TABLES:
			boo , res = self.P.sql.execute(query, commit=True)
			if not boo:
				self.Error("create-work-tables {} :(".format(j))
				return
			else:
				self.Notify("create-work-tables {} :)".format(j))
			j += 1
		_c2 = self.P.sql.select_all(SELECT_COUNT_OF_TAGS)[0][0]
		self.Notify("Added {} rows to work.tag".format(_c2 - _c1))

	def create_map(self):
		ADD_PER_LOOP = 10**5
		def update(az,cfg,dump=False,**kwargs):
			if all(map(lambda x:x in kwargs,['src','dst','weight'])):
				az.append("({src},{dst},{weight})".format(**kwargs))
			if len(az) > ADD_PER_LOOP or dump:
				boo , res = self.P.sql.execute(INSERT_MAP.format(values=",".join(az)),commit=True,unique_cursor=True)
				cfg['i'] += 1
				if 't' not in cfg:
					cfg['t'] = time.time()
				self.Notify("done: %2.4f%% ends in %s"%((cfg['i']/cfg['count']) , ( time.ctime(time.time() + (((time.time()-cfg['t'])*cfg['count']) / cfg['i'])))))
				az = []
				return boo
			else:
				return True
		az = []
		cfg = {
			'i': 0,
			'count':int(self.P.sql.select_all('''SELECT count(*)/{} FROM orb.tag_map;'''.format(ADD_PER_LOOP))[0][0]),
		}
		for row in self.P.sql.select(SELECT_MAPS):
			src,dst,weight = row
			if src > dst:
				dst , src = src, dst
			if not update(az=az,cfg=cfg,src=min(src,dst),dst=max(src,dst),weight=weight):
				break
		update(az=az,cfg=cfg,dump=True,src=min(src,dst),dst=max(src,dst),weight=weight)

	def start(self):
		self.do(self.create_db,desc="Create work database")
		self.do(self.create_map,desc="Create map")
		self.P.stop()

