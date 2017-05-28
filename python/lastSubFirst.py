import os
import time
from multiprocessing.dummy import Pool as Pool

SRCDIR = '/Users/xujiayu/python/data_per_user'

def handle(item):
	try:
		ts_set = set()
		with open(item, 'r') as fr:
			first_line = fr.readline()
			first_ts = int(first_line.split("|")[1])
			ts_set.add(first_ts)
			for line in fr:
				ts = int(line.split("|")[1])
				ts_set.add(ts)
		if len(ts_set) > 1:
			last_line = line
			last_ts = int(last_line.split("|")[1])
			res = last_ts - first_ts
			hours = int(res / 3600)
			if hours >= 1:
				print("more than %sh: %ss" % (hours, res))
		# if len(ts_set) == 1:
		# 	print("1 record, filename: %s" % item)
		# else:
		# 	if line is None:
		# 		print("None line: %s" % line)
		# 	else:
		# 		last_line = line
		# 		last_ts = int(last_line.split("|")[1])
		# 		res = last_ts - first_ts
		# 		hours = int(res / 3600)
		# 		if hours < 1:
		# 			if res == 0:
		# 				print("0s, ts_set: %s, filename: %s" % (ts_set, item))
		# 			else:
		# 				print("%ss" % res)
		# 		else:
		# 			print("more than %sh: %ss" % (hours, res))
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