#!/usr/bin/env python3
import json
import time
from traceback import format_exc as Trace
from kframe import Plugin

from .utils import Table, Row
from .queries import *

DELETE_MAX_SIZE = 10**6

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

	def delete_duples(self):
		def delete(az,bz):
			self.Notify("az={} , bz={}".format(len(az),len(bz)))
			if len(bz) <= 0:
				return
			self.Notify("Gonna delete them!")
			res = self.P.sql.execute('''
				DELETE FROM orb.tag_map
				WHERE id in ({values})
			'''.format(values=",".join(map(lambda x:str(x),bz))),commit=True)[0]
			self.Notify("Result: {status}".format(status="success" if res else "failed"))
		try:
			c1 = self.P.sql.select_all("SELECT count(*) FROM orb.tag_map;")[0][0]
			az = set()
			bz = []
			for row in self.P.sql.select(SELECT_MAP):
				_id , src , dst = row
				key = "@".join([src,dst])
				if key in az:
					bz.append(_id)
				else:
					az.add(key)
				if len(bz) > DELETE_MAX_SIZE:
					break
		except KeyboardInterrupt:
			pass
		delete(az,bz)
		c2 = self.P.sql.select_all("SELECT count(*) FROM orb.tag_map;")[0][0]
		self.Notify("COUNT: c1={c1}, c2={c2}, res={res}".format(c1=c1,c2=c2,res=c2-c1))

	def start(self):
		try:
			self.do(self.create_db,desc="Create work database")
			self.do(self.delete_duples,desc="Delete duplicates")
		except KeyboardInterrupt:
			self.Warring("Got KeyboardInterrupt ; stopping")
		#self.save_cfg()
		self.P.stop()

