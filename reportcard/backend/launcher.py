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


import os
from multiprocessing import Process, JoinableQueue, cpu_count
from time import sleep

import argparse

def _check_folder_exists(path):
	if os.path.exists(path):
		return path
	else:
		raise Exception('Please enter a valid source.')


def _prepare_target(pipeline):
	try:
		module = __import__('%s' % pipeline)
	except ImportError, e:
		raise Exception('It does not seem that %s has a function called main().\n The full error is: \n %s.' % (pipeline, e))
	func = getattr(module, 'main')
	return func

	
def launch_pipeline(target, filename, pid):
	return Process(target=target, args=[filename, pid])

	
def determine_number_of_files(args, ext):
	'''
	Count the total number of files that need to be processed.
	'''
	files = os.listdir(args.source)
	nr_files = 0
	for filename in files:
		if filename.endswith(ext):
			nr_files +=1
	return nr_files


def determine_number_of_processors(nr_files):
	'''
	Determine the maximum number of processes that can be launched.
	To prevent hammering of the server, it always leaves 1 core
	unused.
	'''
	if nr_files > cpu_count():
		return cpu_count() - 1
	else:
		return nr_files


def fill_queue(args, nr_processors, ext):
	'''
	Fill the queue with the full path to the files that need to be processed
	'''
	queue = JoinableQueue()
	files = os.listdir(args.source)
	
	count_files=0
	for filename in files:
		filename = os.path.join(args.source, filename)
		if filename.endswith(ext):
			print filename
			count_files+=1
			queue.put(filename)
	for x in xrange(nr_processors):
		print 'Inserting poison pill %s...' % x
		queue.put(None)

	return queue

	
def main(args):
	'''
	Main function that will fill the queue and launch the processes 
	'''
	ext= 'gz'
	target = _prepare_target(args.pipeline)
	pipelines = []
	pid=0
	nr_files = determine_number_of_files(args, ext)
	nr_processors = determine_number_of_processors(nr_files)
	queue= fill_queue(args, nr_processors, ext)
	while nr_files != 0:
		while sum(pipeline.is_alive() for pipeline in pipelines) != nr_processors:
			filename = queue.get()
			queue.task_done()
			if filename:
				p =launch_pipeline(target, filename, pid)
				p.start()
				pipelines.append(p)
				pid+=1
				nr_files -=1
				print 'There are %s files left in the queue' % (nr_files)
		sleep(30)


		
if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Generic DataPipeline Cruncher')

	parser.add_argument('--source', '-s', metavar='source', action='store', type=_check_folder_exists,
					   help='The full path where the input files are stored')
	
	parser.add_argument('--pipeline', '-p', metavar='pipeline', action='store',
					   help='The filename of the script that does the actual heavy lifting.')
	

	args = parser.parse_args()
	main(args)