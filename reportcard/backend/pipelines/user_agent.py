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

import gc
from datetime import datetime
from urlparse import urlparse
from wurfl import devices
from pywurfl.algorithms import TwoStepAnalysis
import GeoIP

from pipeline import DataPipeline

search_algorithm = TwoStepAnalysis(devices)

class Variable:
	'''
	This class defines the variables that we want to collect,
	which function we need to call to construct the variable and
	the position of the raw data in a line of data.
	'''
	def __init__(self, name, func_name, location, store=True):
		self.name = name 
		self.func_name = func_name
		self.location = location
		self.store = store

class UserAgentObservation:
	'''
	This class aggregates observations for a particular combination of
	project, language, browser, country, timestamp.
	'''
	def __init__(self, **kwargs):
		self.count = 0
		for key, value in kwargs.iteritems():
			setattr(self, key, value)
	
	def __str__(self):
		if self.device:
			return '%s observations using %s:%s in %s for %s%s on %s' % (self.count, self.device, self.geography, self.language_code, self.project, self.timestamp)
		else:
			return '%s observations in %s for %s%s on %s' % (self.count, self.geography, self.language_code, self.project, self.timestamp)


class UserAgentPipeline(DataPipeline):
	'''
	This class does all the heavy lifting of aggregating observations, writing output
	to a file, and do some simple logging 
	'''
	def __init__(self, observation_class, filename, process_id):
		self.start = datetime.now()
		self.skipped_records = 0
		self.http_valid_codes = {'200':1,'304':1, '302':1}
		self.observations = {} #weakref.WeakValueDictionary()
		self.http_invalid_codes = {}
		self.invalid_user_agents = {}
		self.keys = set()
		self.url = None
		self.filename = filename
		self.process_id = process_id
		self.number_of_fields = 14
		self.observation_class = observation_class
		self.gi = GeoIP.new(GeoIP.GEOIP_MEMORY_CACHE)
		self.error_vars = ['http_invalid_codes', 'invalid_user_agents', 'skipped_records']
		
		self.variables = {
					'language_code': ['language_code', '_determine_language_code', 8],
					'project': ['project', '_determine_project', 8],
					'geography': ['geography', '_determine_geography', 4],	#ip address
					'timestamp': ['timestamp', '_determine_date', 2],
					'user_agent': ['user_agent', '_convert_to_unicode', 13],
					}
		self.props=  {}
		for key, value in self.variables.iteritems():
			self.props[key]= Variable(*value)
	
	def _prepare_obs(self, obs):
		'''
		Obs is the raw untouched input from stdin. This function prepares it for 
		subsequent analysis.
		'''
		if type(obs) != type([]):
			return obs.strip().split(' ')
		else:
			return obs
	
	def _parse_url(self, url):
		self.url = urlparse(url)
	
	def _generate_key(self, props):
		value = '_'.join(props.values())
		return hash(value)
	
	def _determine_language_code(self, url):
		domain = self.url.netloc.split('.')
		if len(domain) != 2:
			return domain[0]
		else:
			return 'en'
	
	def _determine_project(self, url):
		try:
			return self.url.netloc.split('.')[-2]
		except IndexError:
			return 'unknown_project'
		
	def _determine_geography(self, ip):
		geography = self.gi.country_code_by_addr(ip)
		if not geography:
			return 'unknown_geography'
		else:
			return geography
	
	def _determine_date(self, timestamp):
		'''
		timestamp format: 2011-08-25T23:43:29.72
		'''
		return timestamp[:10]
	
	def _add_observation(self, key, props):
		'''
		props should be a dictionary and here the actual aggregation happens.
		'''
		if not isinstance(props, dict):
			raise Exception('You have to feed an instance of a Datamodel a dictionary.')

		obs = self.observations.get(key, UserAgentObservation(**props))
		obs.count +=1
		self.keys.add(key)
		self.observations[key] =obs

	def _convert_to_unicode(self, obs):
		return obs.decode('utf-8')
		
	def _total_pageviews(self):
		pageviews = 0
		for obs in self.observations.values():
			pageviews += obs.count
		return pageviews

	def _determine_http_code(self, line):
		#filter out all invalid http requests 
		status_code = line[5][-3:]
		res = self.http_valid_codes.get(status_code, None)
		if res:
			return line
		else:
			self.http_invalid_codes.setdefault(status_code, 0)
			self.http_invalid_codes[status_code] +=1
		return None
	
	def _free_memory(self):
		'''
		This function is no longer used. 
		'''
		for value in self.observations.values():
			del value
		self.observations = {}
		gc.collect()
		
	def _write_error_log(self):
		for prop_name in self.error_vars:
			fh = open('logs/diag-%s-%s.tsv' % (self.process_id, prop_name), 'w')
			prop = getattr(self, prop_name)
			if type(prop) == type({}):
				if prop != {}: #it doesn't make sense to write an empty dictionary to a file. 
					for key, value in prop.iteritems():
						fh.write('%s\t%s\n' %(key, value))
			else:
				fh.write('%s\t%s\n' % (prop_name, prop))
			fh.close()
	
	def _is_device_mobile(self, device):
		return device.is_wireless_device
	
	def transform(self, obs):
		props = {}
		for key in self.props.keys():
			try:
				func_name = getattr(self.props[key], 'func_name')
				location = getattr(self.props[key], 'location')
				if location == 8:
					res = self._parse_url(obs[location])
					if res == False:
						print obs
				if func_name:
					func = getattr(self, func_name)
				props[key] = func(obs[location])
			except Exception, e:
				print len(obs), location
				print func_name
				print obs
				print props
				raise Exception(e)
		return props
		
	def aggregate(self, obs):
		obs = self._prepare_obs(obs)
		props = self.transform(obs)
		key = self._generate_key(props)
		self._add_observation(key, props)
	
	def post_processing(self):
		for obs in self.observations.values():
			try:
				device = devices.select_ua(obs.user_agent, search=search_algorithm)
				obs.device = '%s:%s' % (device.brand_name, device.model_name)
				obs.mobile = self._is_device_mobile(device)
			except (ValueError, UnicodeDecodeError):
				obs.device = '%s:%s' % ('generic', 'unknown_device')
				obs.mobile = True 
				self.invalid_user_agents.setdefault(obs.user_agent,0)
				self.invalid_user_agents[obs.user_agent] +=1
	
	def pre_processing(self, line):
		line = self._prepare_obs(line)
		if len(line) == (self.number_of_fields):
			return self._determine_http_code(line)
		else:
			return None
	
	def load(self):
		fh = open('output/chunk-%s.tsv' % (self.process_id), 'w')
		observations = self.observations.values()
		for obs in observations:
			try:
				fh.write('%s\t%s\t%s\t%s\t%s\t%s\n' % (obs.count, obs.device, obs.geography, obs.language_code, obs.project, obs.timestamp))
			except:
				pass
		fh.close()
		
	def diagnostics(self):
		self._write_error_log()
		print 'Worker %s: Processing of %s took %s' % (self.process_id, self.filename, (datetime.now() - self.start))
		print 'Total number of skipped records: %s' % self.skipped_records
		print 'Total number of invalid http requests: %s' % sum(self.http_invalid_codes.values())
		print 'Total number of invalid user agents: %s' % sum(self.invalid_user_agents.values()) 

	def shutdown(self):
		print 'Total number of observation instances: %s' % (len(self.observations.keys()))
		print 'Total number of pageviews: %s' % self._total_pageviews()
		#self._free_memory()

	def run(self):
		for line in self.decompress():
			line = self.pre_processing(line)
			if line:
				self.aggregate(line)
			else:
				self.skipped_records +=1
	
		self.post_processing()
		self.load()
		self.diagnostics()
		self.shutdown()


def main(filename, process_id):
	pipeline = UserAgentPipeline(UserAgentObservation, filename, process_id)
	pipeline.run()


def debug():
	pipeline = UserAgentPipeline(UserAgentObservation, 'mobile.log-20110826.gz', 0)
	pipeline.run()
	
if __name__ == '__main__':
	#main()
	debug()
