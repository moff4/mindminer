#!/usr/bin/env python3
import json
import time
from traceback import format_exc as Trace
from kframe import Plugin

from .utils import Table, Row
from .queries import *

class Miner(Plugin):
	def init(self):
		try:
			self.timestamp = json.load(open('conf/internal.json'))['timestamp']
		except Exception:
			self.timestamp = 'unix_timestamp()'
		
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
				if 'timestamp' in kwargs:
					self.timestamp = kwargs['timestamp']
					with open("conf/internal.json",'w') as f:
						f.write(json.dumps({"timestamp":self.timestamp}))
				az = []
				return boo
			else:
				return True
		az = []
		cfg = {
			'i': 0,
			'count':int(self.P.sql.select_all('''SELECT count(*)/{count} FROM orb.tag_map where timestamp <= {timestamp};'''.format(count=ADD_PER_LOOP,timestamp=self.timestamp))[0][0]),
		}
		if cfg['count'] > 0:
			for row in self.P.sql.select(SELECT_MAPS.format(timestamp=self.timestamp)):
				src,dst,weight,timestamp = row
				if src > dst:
					dst , src = src, dst
				if not update(az=az,cfg=cfg,src=min(src,dst),dst=max(src,dst),weight=weight,timestamp=timestamp):
					break
			update(az=az,cfg=cfg,dump=True,src=min(src,dst),dst=max(src,dst),weight=weight)

	def start(self):
		self.do(self.create_db,desc="Create work database")
		self.do(self.create_map,desc="Create map")
		self.P.stop()

