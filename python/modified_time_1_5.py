import os
import time
import datetime
from multiprocessing.dummy import Pool as Pool
import subprocess

HADOOPHOME = '/home/xujy/hadoop-2.6.5/bin/hadoop'
SRCDIR = '/home/data/scandata/'
HDFSDIR = 'hdfs://10.103.93.27:9000/realtime_data/'

if __name__ == '__main__':
	try:
		init_ts = 0
		pos = 0
		path = '/home/data/scandata/EC172FE3B340/20170509'
		hdfs_path = 'hdfs://10.103.93.27:9000/realtime_data/EC172FE3B340/20170509'
		ts = os.stat(path).st_mtime
		return_code = subprocess.call([HADOOPHOME, "fs", "-touchz", hdfs_path])
		if return_code != 0:
			print return_code
		while True:
			# with open(path, "r") as fr:
			fr = open(path, 'r')
			fr.seek(pos)
			print "before read: %s" % pos
			if pos == 0:
				offline_data = fr.readlines()
				print "offline_data: %s" % offline_data
				data = []
			else:
				data = fr.readlines()
				print "data: %s" % data
			pos = fr.tell()
			print "after read: %s" % pos
			print "len: %s" % len(data)
			if len(data) > 0:
				print "write data: %s to file: %s" % (data, hdfs_path)
				for line in data:
					child_0 = subprocess.Popen(["echo", line.strip()], stdout=subprocess.PIPE)
					child_1 = subprocess.Popen([HADOOPHOME, "fs", "-appendToFile", "-", hdfs_path], stdin=child_0.stdout, stdout=subprocess.PIPE)
					append_output = child_1.communicate()
					# print "stderr: %s" % append_output[1]
			fr.close()
	except Exception as e:
		raise e