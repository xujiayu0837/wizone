import os
import time
from multiprocessing.dummy import Pool as Pool

SRCDIR = '/Users/xujiayu/python/mean'

def handle(item):
	try:
		with open(item, 'r') as fr:
			prev_ts = fr.readline().split("|")[1]
			for line in fr:
				ts = line.split("|")[1]
				ts_interval = int(ts) - int(prev_ts)
				prev_ts = ts
				if ts_interval == 0:
					print("0s, filename: %s, line: %s" % (item, line))
				else:
					mins = int(ts_interval / 60)
					if mins < 1:
						print("ts_interval: %ss, filename: %s, line: %s" % (ts_interval, item, line))
					else:
						print("more than %sm, ts_interval: %ss, filename: %s, line: %s" % (mins, ts_interval, item, line))
	except Exception as e:
		raise e

if __name__ == '__main__':
	try:
		start_time = time.time()
		path_list = []
		for dir in os.listdir(SRCDIR):
			dir_path = os.path.join(SRCDIR, dir)
			for filename in os.listdir(dir_path):
				path = os.path.join(dir_path, filename)
				path_list.append(path)
		# print("path_list: %s" % path_list)
		pool = Pool()
		pool.map(handle, path_list)
		pool.close()
		pool.join()
		print("It cost %ss" % (time.time() - start_time))
	except Exception as e:
		raise e