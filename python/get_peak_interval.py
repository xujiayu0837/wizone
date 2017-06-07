import os
import time
from multiprocessing.dummy import Pool as Pool
import threading
from util import get_uid, get_date, convert_datetime_to_timestamp, get_time_index

SRCDIR = '/Users/xujiayu/python/mean'
THRESHOLD = 1800
DATEFORMAT = '%Y%m%d'
DATETIMEFORMAT = '%Y-%m-%d %H:%M:%S'
TIME_INDEX_NUMS = int(24 * 60 / 5)
RES_LIST = [0] * TIME_INDEX_NUMS
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

			come_cnt_list = [None] * TIME_INDEX_NUMS
			go_cnt_list = [None] * TIME_INDEX_NUMS
			stay_cnt_list = [None] * TIME_INDEX_NUMS

		except Exception as e:
			raise e
		try:
			with open(item, 'r') as fr:
				for line in fr:
					line_list = line.split("|")
					AP = line_list[-1].strip()
					line_list = [line_list[0], line_list[1], line_list[2], AP]
					if AP == '14E4E6E186A4':
						lines_list_0.append(line_list)
					elif AP == 'EC172FE3B340':
						lines_list_1.append(line_list)
		except Exception as e:
			raise e
		try:
			i = 1
			while i < (len(lines_list_0) - 1):
				rssi = int(lines_list_0[i][2])
				if rssi > int(lines_list_0[i-1][2]) and rssi > int(lines_list_0[i+1][2]):
					ts = time.strftime(DATETIMEFORMAT, time.localtime(int(lines_list_0[i][1])))
					if ts in ts_traj_map:
						# traj = ts_traj_map[ts]
						# ts_traj_map[ts] = [traj, '0']
						# ts_traj_map[ts] = [traj, ['0', rssi]]
						print("outside AP. dup ts. ts_traj_map: %s, ts: %s, user_id: %s" % (ts_traj_map, ts, user_id))
						ts_traj_map[ts] = '1'
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
						# traj = ts_traj_map[ts]
						# ts_traj_map[ts] = [traj, '1']
						# ts_traj_map[ts] = [traj, ['1', rssi]]
						print("inside AP. dup ts. ts_traj_map: %s, ts: %s, user_id: %s" % (ts_traj_map, ts, user_id))
						# continue
						ts_traj_map[ts] = '1'
					else:
						ts_traj_map[ts] = '1'
						# ts_traj_map[ts] = ['1', rssi]
				i += 1
			length = len(ts_traj_map)
			if length > 1:
				sorted_map = sorted(ts_traj_map.items())
				# print("sorted_map: %s, user_id: %s" % (sorted_map, user_id))
				prev_ts = convert_datetime_to_timestamp(sorted_map[0][0], DATETIMEFORMAT)
				traj_list.append(sorted_map[0])
				i = 1
				while i < length:
					cur_ts = convert_datetime_to_timestamp(sorted_map[i][0], DATETIMEFORMAT)
					if cur_ts - prev_ts <= THRESHOLD:
						traj_list.append(sorted_map[i])
					else:
						come_cnt_list, go_cnt_list, stay_cnt_list = handle(traj_list, inside_tup, user_id, ts_0_0_0, come_cnt_list, go_cnt_list, stay_cnt_list)
						# print("traj_list: %s, user_id: %s" % (traj_list, user_id))
						traj_list[:] = []
						traj_list.append(sorted_map[i])
					i += 1
					prev_ts = cur_ts
				come_cnt_list, go_cnt_list, stay_cnt_list = handle(traj_list, inside_tup, user_id, ts_0_0_0, come_cnt_list, go_cnt_list, stay_cnt_list)
				# print("traj_list: %s, user_id: %s" % (traj_list, user_id))
			with lock:
				global RES_LIST
				i = 0
				while i < TIME_INDEX_NUMS:
					come_cnt = come_cnt_list[i] if come_cnt_list[i] else 0
					go_cnt = go_cnt_list[i] if go_cnt_list[i] else 0
					stay_cnt = stay_cnt_list[i] if stay_cnt_list[i] else 0
					RES_LIST[i] = RES_LIST[i] + come_cnt - go_cnt + stay_cnt
					i += 1
			# for cnt in come_cnt_list:
			# 	if cnt:
			# 		print("come_cnt_list: %s, filename: %s" % (come_cnt_list, item))
			# 		break
			# print("come_cnt_list: %s, filename: %s" % (come_cnt_list, item))
			# print("go_cnt_list: %s, filename: %s" % (go_cnt_list, item))
			# print("stay_cnt_list: %s, filename: %s" % (stay_cnt_list, item))
		except Exception as e:
			raise e
	except Exception as e:
		raise e

def handle(traj_list, inside_tup, user_id, ts_0_0_0, come_cnt_list, go_cnt_list, stay_cnt_list):
	try:
		flag_list = []
		for tup in traj_list:
			if isinstance(tup[1], list):
				print("dup ts. ts: %s, user_id: %s" % (tup[0], user_id))
				break
			flag_list.append((tup[0], 'i' if tup[1] in inside_tup else 'o'))
		# flag_list = [(tup[0], 'i' if tup[1] in inside_tup else 'o') for tup in traj_list]
		len_1 = len(flag_list)
		if len_1 <= 0:
			print("empty. flag_list: %s, user_id: %s, filename: %s" % (flag_list, user_id, item))
		elif len_1 == 1:
			print("passerby. flag_list: %s, user_id: %s" % (flag_list, user_id))
			# if flag_list[0][1] == 'o':
			# 	print("passerby. flag_list: %s, user_id: %s" % (flag_list, user_id))
			# elif flag_list[0][1] == 'i':
			# 	time_index = get_time_index(flag_list[0][0], ts_0_0_0, 300)
			# 	stay_cnt_list[time_index] = 1 if not stay_cnt_list[time_index] else stay_cnt_list[time_index]+1
			# 	print("stay. flag_list: %s, user_id: %s" % (flag_list, user_id))
		elif len_1 > 1:

			first_flag = flag_list[0][1]
			first_ts = convert_datetime_to_timestamp(flag_list[0][0], DATETIMEFORMAT)
			first_time_index = get_time_index(first_ts, ts_0_0_0, 300)
			last_flag = flag_list[len_1-1][1]
			last_ts = convert_datetime_to_timestamp(flag_list[len_1-1][0], DATETIMEFORMAT)
			last_time_index = get_time_index(last_ts, ts_0_0_0, 300)
			if first_time_index != last_time_index:
				print("datetimes: %s, %s, flag_list: %s, user_id: %s" % (flag_list[0][0], flag_list[len_1-1][0], flag_list, user_id))

			if len_1 == 2:
				if first_flag == 'o' and last_flag == 'o':
					print("passerby. flag_list: %s, user_id: %s" % (flag_list, user_id))
				elif first_flag == 'i' and last_flag == 'i':
					stay_cnt_list[first_time_index] = 1 if not stay_cnt_list[first_time_index] else stay_cnt_list[first_time_index]+1
					print("stay. flag_list: %s, user_id: %s" % (flag_list, user_id))
				elif first_flag == 'o' and last_flag == 'i':
					come_cnt_list[first_time_index] = 1 if not come_cnt_list[first_time_index] else come_cnt_list[first_time_index]+1
					print('come in. first_time_index: %s, flag_list: %s, user_id: %s' % (first_time_index, flag_list, user_id))
				elif first_flag == 'i' and last_flag == 'o':
					go_cnt_list[first_time_index] = 1 if not go_cnt_list[first_time_index] else go_cnt_list[first_time_index]+1
					print('go out. first_time_index: %s, flag_list: %s, user_id: %s' % (first_time_index, flag_list, user_id))
			else:

				# i = 0
				# while i < len_1:
				# 	j = i+1
				# 	while j < len_1:
				# 		if flag_list[i][1] != flag_list[j][1]:
				# 			if flag_list[i][1] == 'o':
				# 				print('come in. first_time_index: %s, flag_list: %s, user_id: %s' % (first_time_index, flag_list, user_id))
				# 			elif flag_list[i][1] == 'i':
				# 				print('go out. first_time_index: %s, flag_list: %s, user_id: %s' % (first_time_index, flag_list, user_id))
				# 		i = j

				if first_flag == last_flag:
					j = 1
					while j < len_1-1:
						if flag_list[j][1] != first_flag:
							stay_cnt_list[first_time_index] = 1 if not stay_cnt_list[first_time_index] else stay_cnt_list[first_time_index]+1
							print("stay. flag_list: %s, user_id: %s" % (flag_list, user_id))
							break
						j += 1
					if first_flag == 'o':
						print("passerby. flag_list: %s, user_id: %s" % (flag_list, user_id))
					elif first_flag == 'i':
						stay_cnt_list[first_time_index] = 1 if not stay_cnt_list[first_time_index] else stay_cnt_list[first_time_index]+1
						print("stay. flag_list: %s, user_id: %s" % (flag_list, user_id))
				else:
					j = 1
					init_flag = first_flag
					change_nums = 0
					while j < len_1-1:
						cur_flag = flag_list[j][1]
						if cur_flag != init_flag:
							change_nums += 1
							init_flag = cur_flag
						j += 1
					if change_nums <= 1:
						if first_flag == 'o':
							come_cnt_list[first_time_index] = 1 if not come_cnt_list[first_time_index] else come_cnt_list[first_time_index]+1
							print('come in. first_time_index: %s, flag_list: %s, user_id: %s' % (first_time_index, flag_list, user_id))
						elif first_flag == 'i':
							go_cnt_list[first_time_index] = 1 if not go_cnt_list[first_time_index] else go_cnt_list[first_time_index]+1
							print('go out. first_time_index: %s, flag_list: %s, user_id: %s' % (first_time_index, flag_list, user_id))
					else:
						stay_cnt_list[first_time_index] = 1 if not stay_cnt_list[first_time_index] else stay_cnt_list[first_time_index]+1
						print("stay. flag_list: %s, user_id: %s" % (flag_list, user_id))

		return come_cnt_list, go_cnt_list, stay_cnt_list
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
		global RES_LIST
		print("RES_LIST: %s" % RES_LIST)
		print("It cost %ss" % (time.time() - start_time))
	except Exception as e:
		raise e