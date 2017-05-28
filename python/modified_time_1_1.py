import os
import time

class Run(object):
	"""docstring for Run"""
	def __init__(self):
		self._ts = 0
		self._filename = '/home/data/scandata/EC172FE3B340/20170502'

	def watch(self):
		ts = os.stat(self._filename).st_mtime
		if ts != self._ts:
			print "filename: %s, ts: %s" % (self._filename, ts)
			self._ts = ts

if __name__ == '__main__':
	run = Run()
	while True:
		try:
			time.sleep(1)
			run.watch()
		except KeyboardInterrupt:
			print('\nDone')
			break