#!/usr/bin/env python3

import time
from traceback import format_exc as Trace
from kframe import Plugin

from .utils import Table, Row
from .queries import *

class Miner(Plugin):
	def init(self):
		self.n_i   = {} # name -> index
		self.i_n   = {} # index -> name
		self.count = 0  # count of tags
		self._proc = 0.01 # procent of found tags

		self.P.add_plugin(key="table",target=Table,autostart=False)
		self.P.add_plugin(key="row",target=Row,autostart=False)

		self.map = self.P.init_plugin(key="table")
		self.cache_map = {}
		self.cache_count = 0

	#
	# do with time count
	#
	def do(self,target,desc=None,*args,**kwargs):
		_t = time.time()
		try:
			target(*args,**kwargs)
		except Exception as e:
			self.Error("{desc} - {e}".format(desc=str(target) if desc is None else desc,e=e))
			self.Debug("{desc} - {e}".format(desc=str(target) if desc is None else desc,e=Trace()))
		self.Notify("{desc} {t} sec".format(desc=str(target) if desc is None else desc,t=time.time()-_t))

	#
	# return index of tag
	# of None if index >= count
	#
	def get_or_add_tag(self,tag):
		if tag not in self.n_i:
			l = len(self.n_i)
			if l >= self.count:
				return None
			else:
				self.n_i[tag] = l
				self.i_n[l] = tag
				if (l / self.count) >= self._proc:
					self.Notify("found: %0.1f%%"%(100.0 * l / self.count))
					self._proc += 0.01
		else:
			l = self.n_i[tag]
		return l

	#
	# fill map and convert-tables
	#
	def load(self):
		res = self.P.sql.select_all(SELECT_COUNT_OF_TAGS)
		if res is None:	
			self.Error("SQL is unreachable")
			return
		self.count = res[0][0]
		for tag in self.P.sql.select(SELECT_MAPS):
			tag = tag[0]
			if self.get_or_add_tag(tag) is None:
				break
	
	def smth(self):
		for i in range(100):
			for j in range(100):
				self.Notify("smth: %2.0f.%2.0f%%"%(i,j))
				if 0 < self.map[i][j] <= 1:
					self.map[i][j] = 0
		print("cache size: %s"%self.cache_count)

	def create_db(self):
		self.P.sql.exec(???)

	def start(self):
		self.do(self.create_db,desc="Copy database")
		# self.do(self.load,desc="Load tags and indexs")
		# self.do(self.smth,desc="Smth")
		self.P.stop()