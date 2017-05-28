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
		dir_list = os.listdir(SRCDIR)
		today_str = datetime.date.today().strftime("%Y%m%d")
		path = ''
		for item in dir_list:
			try:
				path = os.path.join(SRCDIR, item+"/"+today_str)
				print "path: %s" % path
				dest_dir = os.path.join(HDFSDIR, item)
				print "dest_dir: %s" % dest_dir
				return_code = subprocess.call([HADOOPHOME, "fs", "-touchz", os.path.join(dest_dir, today_str)])
				if return_code != 0:
					print return_code
					# return
			except Exception as e:
				raise e
				# return

			for item in dir_list:
				try:
					init_ts = 0
					pos = 0
					last_pos = -1
					ts = os.stat(path).st_mtime
					if ts != init_ts:
						init_ts = ts
						with open(path, "r") as fr:
							fr.seek(pos)
							print "before read: %s" % pos
							if pos == 0:
								offline_data = fr.readlines()
								data = []
							data = fr.readlines()
							pos = fr.tell()
							print "after read: %s" % pos
							print "len: %s" % len(data)
							if len(data) > 0:
								print "write data: %s to file: %s" % (data, os.path.join(dest_dir, today_str))
								for line in data:
									child_0 = subprocess.Popen(["echo", line.strip()], stdout=subprocess.PIPE)
									child_1 = subprocess.Popen([HADOOPHOME, "fs", "-appendToFile", "-", os.path.join(dest_dir, today_str)], stdin=child_0.stdout, stdout=subprocess.PIPE)
									append_output = child_1.communicate()
									print "stderr: %s" % append_output[1]
				except Exception as e:
					raise e
					# return
	except Exception as e:
		raise e