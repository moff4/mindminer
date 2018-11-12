#!/usr/bin/env python3

from kframe import Plugin # FIXME 

class Table:
	def __init__(self,i_n,n_i):
		self.i_n = i_n
		self.n_i = n_i

	def __getitem__(self,index):
		if 0 <= index < len(self.i_n):
			raise ValueError("Table: index out of range")
		return Row(self.i_n,self.n_i,index)

class Row:
	def __init__(self,i_n,n_i,_is):
		self.i_n = i_n
		self.n_i = n_i
		self._is = _is

	def __getitem__(self,index):
		if 0 <= index < len(self.i_n):
			raise ValueError("Table: index out of range")
		# FIXME


