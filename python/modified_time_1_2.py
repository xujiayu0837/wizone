import os
import time
import datetime
from multiprocessing.dummy import Pool as Pool

def handle(sub_dir):
	realtime_dir = '/home/hadoop/realtime_data/'
	today_str = datetime.date.today().strftime("%Y%m%d")
	dest_dir = ''
	path = ''
	try:
		try:
			for filename in os.listdir(sub_dir):
				if filename == today_str:
					path = os.path.join(sub_dir, today_str)
				dest_dir = os.path.join(realtime_dir, os.path.basename(sub_dir))
				print "dest_dir: %s" % dest_dir
				if not os.path.exists(dest_dir):
					os.mkdir(dest_dir)
		except Exception as e:
			return
		f = open(path, "r")
		history_list = f.readlines()
		last_pos = f.tell()
		f.close()
		init_ts = 0
		while True:
			ts = os.stat(path).st_mtime
			if init_ts != ts:
				init_ts = ts
				print "dest_path: %s" % os.path.join(dest_dir, today_str)
				with open(path, 'r') as fr, open(os.path.join(dest_dir, today_str), 'a') as fw:
					print "last_pos: %s" % last_pos
					fr.seek(last_pos)
					data = fr.readlines()
					last_pos = fr.tell()
					if len(data) > 0:
						print "write data: %s to file: %s" % (data, os.path.join(dest_dir, today_str))
						fw.writelines(data)
	except Exception as e:
		raise e

if __name__ == '__main__':
	source_dir = '/home/data/scandata/'
	try:
		dir_list = os.listdir(source_dir)
		path_list = []
		for item in dir_list:
			path_list.append(os.path.join(source_dir, item))
		pool = Pool(40)
		pool.map(handle, path_list)
		pool.close()
		pool.join()
	except Exception as e:
		raise e