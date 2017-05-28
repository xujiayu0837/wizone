import os
import time
from multiprocessing.dummy import Pool as Pool
import shutil
from util import get_date, get_uid, get_dest_dir

SRCDIR = '/Users/xujiayu/python/mean'
DESTDIR = '/Users/xujiayu/python/24hours'

def handle(item):
	try:
		# 需手动创建dest_dir. cd /Users/xujiayu/python, mkdir -p 24hours/date
		date = get_date(item)
		user_id = get_uid(item)
		dest_dir = get_dest_dir(DESTDIR, item)
		print("date: %s, user_id: %s, dest_dir: %s" % (date, user_id, dest_dir))
	except Exception as e:
		raise e
	try:
		hour_set = set()
		with open(item, 'r') as fr:
			for line in fr:
				# user_id = line.split("|")[0]
				ts = int(line.split("|")[1])
				day = str(time.localtime(ts)[2])
				hour = str(time.localtime(ts)[3])
				hour_set.add(",".join([day, hour]))
		if len(hour_set) >= 24:
			shutil.move(item, os.path.join(dest_dir, user_id))
			print("hour_set: %s, user_id: %s, filename: %s" % (hour_set, user_id, item))
	except Exception as e:
		raise e

if __name__ == '__main__':
	try:
		start_time = time.time()
		path_list = []
		for dir in os.listdir(SRCDIR):
			dir_path = os.path.join(SRCDIR, dir)
			if os.path.isdir(dir_path):
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