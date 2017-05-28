import os
import time
from multiprocessing.dummy import Pool as Pool

SRCDIR = '/Users/xujiayu/python/mean'

def handle(item):
	try:
		try:
			lines_list_0 = []
			lines_list_1 = []
			ts_list_0 = []
			ts_list_1 = []
			with open(item, 'r') as fr:
				for line in fr:
					line_list = line.split("|")
					AP = line_list[-1].strip()
					print("line_list: %s, filename: %s" % (line_list, item))
					line_list = [line_list[0], line_list[1], line_list[2], AP]
					if AP == '14E4E6E186A4':
						lines_list_0.append(line_list)
					elif AP == 'EC172FE3B340':
						lines_list_1.append(line_list)
		except Exception as e:
			raise e
		try:
			i = 1
			j = 1
			while i < (len(lines_list_0) - 1):
				if int(lines_list_0[i][2]) >= int(lines_list_0[i-1][2]) and int(lines_list_0[i][2]) >= int(lines_list_0[i+1][2]):
					print("rssi seq: %s, %s, %s" % (lines_list_0[i-1][2], lines_list_0[i][2], lines_list_0[i+1][2]))
					print("lines: %s, %s, %s" % ('|'.join(lines_list_0[i-1]), '|'.join(lines_list_0[i]), '|'.join(lines_list_0[i+1])))
					ts_list_0.append(lines_list_0[i][1])
				i += 1
			while j < (len(lines_list_1) - 1):
				if int(lines_list_1[j][2]) >= int(lines_list_1[j-1][2]) and int(lines_list_1[j][2]) >= int(lines_list_1[j+1][2]):
					print("rssi seq: %s, %s, %s" % (lines_list_1[j-1][2], lines_list_1[j][2], lines_list_1[j+1][2]))
					print("lines: %s, %s, %s" % ('|'.join(lines_list_1[j-1]), '|'.join(lines_list_1[j]), '|'.join(lines_list_1[j+1])))
					ts_list_1.append(lines_list_1[j][1])
				j += 1
			print("ts_list_0: %s, ts_list_1: %s" % (ts_list_0, ts_list_1))
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