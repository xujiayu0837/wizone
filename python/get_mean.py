import os
import time
import statistics
from multiprocessing.dummy import Pool as Pool

SRCDIR = '/Users/xujiayu/python/data_per_user'
DESTDIR = '/Users/xujiayu/python/mean'

def get_dest_dir(dir, item):
	try:
		src_dir_list = item.split("/")
		date = src_dir_list[-2]
		dest_dir = os.path.join(dir, date)
		print("dest_dir: %s" % dest_dir)
		return dest_dir
	except Exception as e:
		raise e

def handle(item):
	try:
		# 需手动创建dest_dir. cd /Users/xujiayu/python, mkdir -p mean/date
		dest_dir = get_dest_dir(DESTDIR, item)
		user_id = item.split('/')[-1]
	except Exception as e:
		raise e
	try:
		with open(item, 'r') as fr:
			length = len(fr.readlines())
			# print("length: %s" % length)
		with open(item, 'r') as fr, open(os.path.join(dest_dir, user_id), 'a') as fw:
			rssi_list = []
			first_line = fr.readline()
			first_list = first_line.split("|")
			first_ts = first_list[1]
			first_rssi = float(first_list[2])
			first_AP = first_list[-1].strip()
			rssi_list.append(first_rssi)
			i = 1
			while i < length:
				i += 1
				line = fr.readline()
				lines = line.split("|")
				ts = lines[1]
				rssi = float(lines[2])
				AP = lines[-1].strip()
				if ts == first_ts and AP == first_AP:
					rssi_list.append(rssi)
				else:
					mean_rssi = round(statistics.mean(rssi_list))
					# mean_rssi = round(sum(rssi_list) / len(rssi_list))
					line_to_write = '|'.join([user_id, first_ts, str(mean_rssi), first_AP])+'\n'
					print("line_to_write: %s" % line_to_write)
					fw.write(line_to_write)
					first_ts = ts
					first_rssi = rssi
					first_AP = AP
					# print("rssi_list: %s" % rssi_list)
					rssi_list[:] = []
					rssi_list.append(first_rssi)
			mean_rssi = round(statistics.mean(rssi_list))
			# mean_rssi = round(sum(rssi_list) / len(rssi_list))
			line_to_write = '|'.join([user_id, first_ts, str(mean_rssi), first_AP])+'\n'
			print("line_to_write: %s" % line_to_write)
			fw.write(line_to_write)
			# print("rssi_list: %s" % rssi_list)
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