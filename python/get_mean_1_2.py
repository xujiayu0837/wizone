import os
import time
import statistics
from multiprocessing.dummy import Pool as Pool
from util import get_AP_0
from util import get_date_0

SRCDIR = '/Users/xujiayu/python/scandata/20170502'
DESTDIR = '/Users/xujiayu/python/mean_1_2'
OUIPATH = '/Users/xujiayu/Downloads/oui_new.txt'
AP_list = ['0C8268F90E64', '0C8268C7D504', '14E6E4E1C510', '0C8268C7DD6C']

def get_oui_list():
	with open(OUIPATH, 'r') as fr:
		oui_list = [line.split("|")[0] for line in fr]
	return oui_list

def handle(item):
	try:
		i = 0
		lines_list = []
		try:
			oui_list = get_oui_list()
			# print("oui_list: %s, len: %s" % (oui_list, len(oui_list)))
		except Exception as e:
			raise e
		try:
			AP = get_AP_0(item)
			date = get_date_0(item)
			dir = os.path.join(DESTDIR, AP)
			if not os.path.exists(dir):
				os.mkdir(dir)
		except Exception as e:
			raise e
		try:
			fw = open(os.path.join(dir, date), 'a')
			with open(item, 'r', encoding='latin-1') as fr:
				lines_list = fr.readlines()
			length = len(lines_list)
			while i < length:
				first_line = lines_list[i]
				first_list = first_line.split("|")
				if len(first_list) < 3:
					print("first_list: %s, filename: %s" % (first_list, item))
					break
				first_user_id = first_list[0]
				first_rssi = int(first_list[1])
				j = i+1
				# print("i: %s, j: %s, length: %s" % (i, j, length))
				if first_user_id[0:6] in oui_list and first_rssi > -90:
					rssi_list = [first_rssi]
					first_ts = first_list[-1].strip()
					while j < length:
						line = lines_list[j]
						line_list = line.split("|")
						user_id = line_list[0]
						rssi = int(line_list[1])
						ts = line_list[-1].strip()
						if user_id != first_user_id or ts != first_ts:
							break
						else:
							j += 1
							if rssi <= -90:
								# print("invalid rssi: %s" % rssi)
								continue
							rssi_list.append(rssi)
					if len(rssi_list) > 0:
						mean_rssi = round(statistics.mean(rssi_list))
						print("rssi_list: %s, mean_rssi: %s" % (rssi_list, mean_rssi))
						# print("rssi_list: %s, user_id: %s, ts: %s, filename: %s" % (rssi_list, user_id, ts, item))
						line_to_write = '|'.join([first_user_id, str(mean_rssi), first_ts])+'\n'
						fw.write(line_to_write)
				i = j
			fw.close()
		except Exception as e:
			raise e
	except Exception as e:
		raise e

if __name__ == '__main__':
	try:
		start_time = time.time()
		path_list = []
		for dir in os.listdir(SRCDIR):
			if dir in AP_list:
				dir_path = os.path.join(SRCDIR, dir)
				for filename in os.listdir(dir_path):
					path = os.path.join(dir_path, filename)
					path_list.append(path)
		print("path_list: %s" % path_list)
		pool = Pool()
		pool.map(handle, path_list)
		pool.close()
		pool.join()
		print("It cost %ss" % (time.time() - start_time))
	except Exception as e:
		raise e