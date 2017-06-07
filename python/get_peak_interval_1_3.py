import os
import time
from multiprocessing.dummy import Pool as Pool
import threading
import pandas as pd
from util import get_uid, get_date, convert_datetime_to_timestamp, get_time_index

SRCDIR = '/Users/xujiayu/python/uid_mean'
DATEFORMAT = '%Y%m%d'
DATETIMEFORMAT = '%Y-%m-%d %H:%M:%S'
time_index_nums = int(24 * 60 / 5)
res_list = [0] * time_index_nums
lock = threading.Lock()

def get_trajectory(item):
	try:
		try:
			user_id = get_uid(item)
			date = get_date(item)
			ts_0_0_0 = time.mktime(time.strptime(date, DATEFORMAT))
			lines_list_0 = []
			lines_list_1 = []
			ts_traj_map = {}
			sorted_map = {}
			traj_list = []
			inside_tup = ('1')

			come_cnt_list = [None] * time_index_nums
			go_cnt_list = [None] * time_index_nums
			stay_cnt_list = [None] * time_index_nums
		except Exception as e:
			raise e
		try:
			df = pd.read_csv(item, sep='|', names=['user_id', 'ts', 'rssi', 'AP'])
			df_0 = df[df['AP'] == '0C8268F90E64']
			df_1 = df[df['AP'] == '0C8268C7D504']
			df_2 = df[df['AP'] == '14E6E4E1C510']
			df_3 = df[df['AP'] == '0C8268C7DD6C']
			print("len: %s" % len(df))
		except Exception as e:
			raise e
	except Exception as e:
		raise e

if __name__ == '__main__':
	try:
		start_time = time.time()
		path_list = []
		for dir in os.listdir(SRCDIR):
			if dir == '20170502':
				dir_path = os.path.join(SRCDIR, dir)
				# print("dir_path: %s" % dir_path)
				if not os.path.isdir(dir_path):
					continue
				for filename in os.listdir(dir_path):
					path = os.path.join(dir_path, filename)
					path_list.append(path)
		pool = Pool()
		pool.map(get_trajectory, path_list)
		pool.close()
		pool.join()
		global res_list
		print("res_list: %s" % res_list)
		print("It cost %ss" % (time.time() - start_time))
	except Exception as e:
		raise e