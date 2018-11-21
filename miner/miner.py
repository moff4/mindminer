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
			tmp = json.load(open('conf/internal.json'))
		except Exception:
			tmp = {}
		self.timestamp = tmp['timestamp'] if 'timestamp' in tmp else 'unix_timestamp()'
		self.end_time  = tmp['end'] if 'end' in tmp else '0'
		
	def save_cfg(self):
		with open("conf/internal.json",'w') as f:
			f.write(json.dumps({
				"timestamp":self.timestamp,
				"end":self.end_time
			}))

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
		ADD_PER_LOOP = 10**4
		def update(az,cfg,dump=False,**kwargs):
			if all(map(lambda x:x in kwargs,['src','dst','weight'])):
				az.append("({src},{dst},{weight})".format(**kwargs))
			if (len(az) > ADD_PER_LOOP or dump) and len(az) > 0:
				boo , res = self.P.sql.execute(INSERT_MAP.format(values=",".join(az)),commit=True)
				if not boo:
					return False
				if 'timestamp' in kwargs and self.timestamp > kwargs['timestamp']:
					self.timestamp = kwargs['timestamp']
				cfg['i'] += len(az)
				self.Notify("done: (%s) %2.4f%% ends in %2.2f seconds"%(
					int(self.P.sql.select_all(SELECT_COUNT_OF_MAPS)[0][0]),
					(cfg['i']/cfg['count']) , 
					( (time.time()-cfg['t'] ) / cfg['i'] * cfg['count'] )
				))
				return True
			else:
				return None
		az = []
		cfg = {
			'i': 0,
			't':time.time(),
			'count':int(self.P.sql.select_all('''
				SELECT count(*) 
				FROM orb.tag_map 
				where timestamp <= {timestamp}
				AND timestamp >= {end_time}
				;'''.format(end_time=self.end_time,timestamp=self.timestamp))[0][0]),
		}
		boo = False
		_c1 = self.P.sql.select_all(SELECT_COUNT_OF_MAPS)[0][0]
		if cfg['count'] > 0:
			self.Notify("Gonna check {count} rows".format(**cfg))
			try:
				while cfg['count'] > 0:
					cfg['count'] = int(self.P.sql.select_all('''
					SELECT count(*) 
					FROM orb.tag_map 
					where timestamp <= {timestamp}
					AND timestamp >= {end_time}
					;'''.format(end_time=self.end_time,timestamp=self.timestamp))[0][0])
					for row in self.P.sql.select(SELECT_MAPS.format(timestamp=self.timestamp,end_time=self.end_time,limit=ADD_PER_LOOP)):
						try:
							src,dst,weight,timestamp = row
							if src > dst:
								dst , src = src, dst
							res = update(az=az,cfg=cfg,src=min(src,dst),dst=max(src,dst),weight=weight,timestamp=timestamp)
							if res == False:
								break
							elif res == True:
								az = []
						except KeyboardInterrupt:
							self.Warring("Got KeyboardInterrupt ; stopping")
							boo = True
						if boo:
							break
			except Exception as e:
				self.Error("create_db - loop: %s"%e)
				self.Debug("create_db - loop: %s"%Trace())
			update(az=az,cfg=cfg,dump=True)
			_c2 = self.P.sql.select_all(SELECT_COUNT_OF_MAPS)[0][0]
			self.Notify("add maps: {} new rows".format(_c2 - _c1))
		else:
			self.Notify("add maps: none new")


	def start(self):
		try:
			# self.do(self.create_db,desc="Create work database")
			self.do(self.create_map,desc="Create map")
		except KeyboardInterrupt:
			self.Warring("Got KeyboardInterrupt ; stopping")
		self.save_cfg()
		self.P.stop()

