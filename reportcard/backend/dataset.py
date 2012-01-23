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


class Observation(object):
	def __init__(self, *args):
		self.count = 0


class UserAgentObservation(object):

	def __init__(self, **kwargs):
		self.count = 0
		for key, value in kwargs.iteritems():
			setattr(self, key, value)
	
	def __str__(self):
		if self.device:
			return '%s observations using %s:%s in %s for %s%s on %s' % (self.count, self.device, self.geography, self.language_code, self.project, self.timestamp)
		else:
			return '%s observations in %s for %s%s on %s' % (self.count, self.geography, self.language_code, self.project, self.timestamp)

		
