import os
import time
from multiprocessing.dummy import Pool as Pool

SRCDIR = '/Users/xujiayu/python/data_per_user'

def handle(item):
	try:
		error_list = []
		date = item.split("/")[-2]
		real_day = date[-2:]
		# print("real_day: %s" % real_day)
		try:
			with open(item, 'r') as fr:
				for line in fr:
					ts = int(line.split("|")[1])
					# print("ts: %s" % ts)
					# 1 -> 01, 2 -> 02, ...
					day = ''.join(['0', str(time.localtime(ts)[2])]) if time.localtime(ts)[2] < 10 else str(time.localtime(ts)[2])
					if day != real_day:
						hour = str(time.localtime(ts)[3])
						error_list.append([real_day, day, hour, line, item])
			if len(error_list) > 0:
				print("real_day: %s, day: %s, hour: %s, line: %s, filename: %s" % (real_day, day, hour, line, item))
		except Exception as e:
			raise e
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