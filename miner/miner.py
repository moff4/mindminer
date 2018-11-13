#!/usr/bin/env python3

import time

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

		self.map = self.P.init_plugin(key="table",n_i=self.n_i,i_n=self.i_n)


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
		_t = time.time()

		res = self.P.sql.select_all(SELECT_COUNT_OF_TAGS)
		if res is None:	
			self.Error("SQL is unreachable")
			return
		self.count = res[0][0]

		self.count = 1000

		for row in self.P.sql.select(SELECT_MAPS):
			src,dst = row
			_is = self.get_or_add_tag(src)
			_id = self.get_or_add_tag(dst)
			if _is is None or _id is None:
				break

		self.Notify("Loaded database: {} sec".format(time.time() - _t))

	def start(self):
		self.load()

		self.P.stop()