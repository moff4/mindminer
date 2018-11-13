#!/usr/bin/env python3

from kframe import Plugin 

from .queries import *

class Table(Plugin):
	def init(self):
		pass
	def __getitem__(self,index):
		if 0 <= index < len(self.P.miner.i_n):
			return self.P.init_plugin(key="row",_is=index)
		raise ValueError("Table: index out of range: {index}; max = {max}".format(index=index,max=len(self.P.miner.i_n)))

class Row(Plugin):
	def init(self,_is):
		self._is = _is

	def __setitem__(self,index,value):
		if 0 <= index < len(self.P.miner.i_n):
			
			src = min(index,self._is)
			dst = max(index,self._is)
			
			if src not in self.P.miner.cache_map:
				self.P.miner.cache_map[src] = {}
			if dst not in self.P.miner.cache_map:
				self.P.miner.cache_map[src][dst] = {}

			self.P.miner.cache_map[src][dst] = value
			self.P.miner.cache_count += 1
		else:
			raise ValueError("Table: index out of range: {index}; max = {max}".format(index=index,max=len(self.P.miner.i_n)))


	def __getitem__(self,index):
		if 0 <= index < len(self.P.miner.i_n):

			src = min(index,self._is)
			dst = max(index,self._is)

			if src in self.P.miner.cache_map and dst in self.P.miner.cache_map[src]:
				return self.P.miner.cache_map[src][dst]

			src_tag = self.P.miner.i_n[src]
			dst_tag = self.P.miner.i_n[dst]

			res = self.P.sql.select_all(SELECT_WEIGHT.format(src_tag=src_tag,dst_tag=dst_tag))
			if res is None:
				raise ValueError("no value for {src_tag} -> {dst_tag}".format(src_tag=src_tag,dst_tag=dst_tag))
			elif len(res) <= 0:
				return 0.0
			else:
				return res[0][0]
		raise ValueError("Table: index out of range: {index}; max = {max}".format(index=index,max=len(self.P.miner.i_n)))