import os
import time
from multiprocessing.dummy import Pool as Pool
import pandas as pd
from util import get_uid, get_date, convert_datetime_to_timestamp, get_time_index

SRCDIR = '/Users/xujiayu/python/uid_mean'
DATEFORMAT = '%Y%m%d'
DATETIMEFORMAT = '%Y-%m-%d %H:%M:%S'

def get_trajectory(item):
	try:
		try:
			user_id = get_uid(item)
			date = get_date(item)
			ts_0_0_0 = time.mktime(time.strptime(date, DATEFORMAT))
			lines_list_0 = []
			lines_list_1 = []
			lines_list_2 = []
			lines_list_3 = []
			ts_traj_map = {}
			sorted_map = {}
			traj_list = []
			inside_tup = ('1')
			AP_list = ['0C8268F90E64', '0C8268C7D504', '14E6E4E1C510', '0C8268C7DD6C']

			time_index_nums = int(24 * 60 / 5)
			come_cnt_list = [None] * time_index_nums
			go_cnt_list = [None] * time_index_nums
			stay_cnt_list = [None] * time_index_nums

		except Exception as e:
			raise e
		try:
			df = pd.read_csv(item, sep='|', names=['user_id', 'ts', 'rssi', 'AP'])
			with open(item, 'r') as fr:
				for line in fr:
					line_list = line.split("|")
					AP = line_list[-1].strip()
					# print("line_list: %s, filename: %s" % (line_list, item))
					line_list = [line_list[0], line_list[1], line_list[2], AP]
					if AP == AP_list[0]:
						lines_list_0.append(line_list)
					elif AP == AP_list[1]:
						lines_list_1.append(line_list)
					elif AP == AP_list[2]:
						lines_list_2.append(line_list)
					elif AP == AP_list[3]:
						lines_list_3.append(line_list)
		except Exception as e:
			raise e
		try:
			i = 1
			while i < (len(lines_list_0) - 1):
				rssi = int(lines_list_0[i][2])
				if rssi > int(lines_list_0[i-1][2]) and rssi > int(lines_list_0[i+1][2]):
					ts = time.strftime(DATETIMEFORMAT, time.localtime(int(lines_list_0[i][1])))
					if ts in ts_traj_map:
						traj = ts_traj_map[ts]
						ts_traj_map[ts] = [traj, '0']
						# ts_traj_map[ts] = [traj, ['0', rssi]]
						# print("outside AP: 0. dup ts. ts_traj_map[ts]: %s, ts: %s, user_id: %s" % (ts_traj_map[ts], ts, user_id))
						# ts_traj_map[ts] = '1'
					else:
						ts_traj_map[ts] = '0'
						# ts_traj_map[ts] = ['0', rssi]
				i += 1
			i = 1
			while i < (len(lines_list_1) - 1):
				rssi = int(lines_list_1[i][2])
				if rssi > int(lines_list_1[i-1][2]) and rssi > int(lines_list_1[i+1][2]):
					ts = time.strftime(DATETIMEFORMAT, time.localtime(int(lines_list_1[i][1])))
					if ts in ts_traj_map:
						traj = ts_traj_map[ts]
						ts_traj_map[ts] = [traj, '1']
						# ts_traj_map[ts] = [traj, ['1', rssi]]
						# print("inside AP: 1. dup ts. ts_traj_map[ts]: %s, ts: %s, user_id: %s" % (ts_traj_map[ts], ts, user_id))
						# continue
						# ts_traj_map[ts] = '1'
					else:
						ts_traj_map[ts] = '1'
						# ts_traj_map[ts] = ['1', rssi]
				i += 1
			i = 1
			while i < (len(lines_list_2) - 1):
				rssi = int(lines_list_2[i][2])
				if rssi > int(lines_list_2[i-1][2]) and rssi > int(lines_list_2[i+1][2]):
					ts = time.strftime(DATETIMEFORMAT, time.localtime(int(lines_list_2[i][1])))
					if ts in ts_traj_map:
						traj = ts_traj_map[ts]
						ts_traj_map[ts] = [traj, '2']
						# ts_traj_map[ts] = [traj, ['2', rssi]]
						# print("outside AP: 2. dup ts. ts_traj_map[ts]: %s, ts: %s, user_id: %s" % (ts_traj_map[ts], ts, user_id))
						# ts_traj_map[ts] = '1'
					else:
						ts_traj_map[ts] = '2'
						# ts_traj_map[ts] = ['2', rssi]
				i += 1
			i = 1
			while i < (len(lines_list_3) - 1):
				rssi = int(lines_list_3[i][2])
				if rssi > int(lines_list_3[i-1][2]) and rssi > int(lines_list_3[i+1][2]):
					ts = time.strftime(DATETIMEFORMAT, time.localtime(int(lines_list_3[i][1])))
					if ts in ts_traj_map:
						traj = ts_traj_map[ts]
						ts_traj_map[ts] = [traj, '3']
						# ts_traj_map[ts] = [traj, ['3', rssi]]
						# print("inside AP: 3. dup ts. ts_traj_map[ts]: %s, ts: %s, user_id: %s" % (ts_traj_map[ts], ts, user_id))
						# ts_traj_map[ts] = '1'
					else:
						ts_traj_map[ts] = '3'
						# ts_traj_map[ts] = ['3', rssi]
				i += 1
			length = len(ts_traj_map)
			if length > 1:
				sorted_map = sorted(ts_traj_map.items())
				for tup in sorted_map:
					if isinstance(tup[1], list):
						print("sorted_map: %s, user_id: %s" % (sorted_map, user_id))
						continue
				# print("sorted_map: %s, user_id: %s" % (sorted_map, user_id))
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
		# print("path_list: %s" % path_list)
		pool = Pool()
		pool.map(get_trajectory, path_list)
		pool.close()
		pool.join()
		print("It cost %ss" % (time.time() - start_time))
	except Exception as e:
		raise e