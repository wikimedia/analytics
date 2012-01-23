# -*- coding: utf-8 -*-

'''
Copyright (C) 2012 by Diederik van Liere (dvanliere@wikimedia.org)
This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License version 2
as published by the Free Software Foundation.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details, at
http://www.fsf.org/licenses/gpl.html
'''

__author__ = '''\n'''.join(['Diederik van Liere (dvanliere@wikimedia.org)', ])
__email__ = 'dvanliere at wikimedia dot org'
__date__ = '2012-01-22'
__version__ = '0.1'


from abc import ABCMeta
import subprocess
import sys

class DataPipeline(object):	
	__metaclass__ = ABCMeta

	def decompress(self):
		'''
		Don't use the internal zlib library from Python, it's super slow. 
		This might make this script Linux/OSX only.
		'''
		p = subprocess.Popen(['gunzip','-c', self.filename], stdout=subprocess.PIPE, shell=False)
		for line in iter(p.stdout.readline, ""):
			yield line
		
	def extract(self):
		while True:
			line = sys.stdin.readline()
			if not line:
				break
			self.aggregate(line)
	
	
	def aggregate(self, obs):
		return

	def transform(self, obs):
		return
	
	def load(self):
		return
	
	def run(self):
		return
	
	def post_processing(self):
		return

	def pre_processing(self):
		return

	def _prepare_obs(self, obs):
		return



