import os
import time
from multiprocessing.dummy import Pool as Pool

SRCDIR = '/Users/xujiayu/python/mean'

def handle(item):
	try:
		lines_list_0 = []
		lines_list_1 = []
		with open(item, 'r') as fr:
			for line in fr:
				lst = line.split("|")
				AP = lst[-1].strip()
				line_list = [lst[0], lst[1], lst[2], AP]
				if AP == '14E4E6E186A4':
					lines_list_0.append(line_list)
				elif AP == 'EC172FE3B340':
					lines_list_1.append(line_list)
		if len(lines_list_0) <= 0:
			print("len_0: %s, filename: %s" % (len(lines_list_0), item))
		if len(lines_list_1) <= 0:
			print("len_1: %s, filename: %s" % (len(lines_list_1), item))
	except Exception as e:
		raise e

if __name__ == '__main__':
	try:
		start_time = time.time()
		path_list = []
		for dir in os.listdir(SRCDIR):
			dir_path = os.path.join(SRCDIR, dir)
			print("dir_path: %s" % dir_path)
			if not os.path.isdir(dir_path):
				continue
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