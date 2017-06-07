import os
import time
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from util import get_paths_0, get_uid

SRCDIR = '/Users/xujiayu/python/mean'
DATETIMEFORMAT = '%Y-%m-%d %H:%M:%S'
BIG_LIST = []

def run():
	try:
		try:
			paths_list = get_paths_0('20170502')
			i = 0
		except Exception as e:
			raise e
		try:
			while i < len(paths_list):
				lines_list_0 = []
				lines_list_1 = []
				ts_traj_map = {}
				user_id = get_uid(paths_list[i])
				with open(paths_list[i], 'r') as fr:
					for line in fr:
						line_list = line.split("|")
						AP = line_list[-1].strip()
						# print("line_list: %s, filename: %s" % (line_list, item))
						line_list = [line_list[0], line_list[1], line_list[2], AP]
						if AP == '14E4E6E186A4':
							lines_list_0.append(line_list)
						elif AP == 'EC172FE3B340':
							lines_list_1.append(line_list)
				j = 1
				while j < len(lines_list_0)-1:
					rssi = int(lines_list_0[j][2])
					if rssi > int(lines_list_0[j-1][2]) and rssi > int(lines_list_0[j+1][2]):
						# ts = time.strftime(DATETIMEFORMAT, time.localtime(int(lines_list_0[j][1])))
						ts = int(lines_list_0[j][1])
						if ts in ts_traj_map:
							ts_traj_map[ts] = '1'
						else:
							ts_traj_map[ts] = '0'
					j += 1
				j = 1
				while j < len(lines_list_1)-1:
					rssi = int(lines_list_1[j][2])
					if rssi > int(lines_list_1[j-1][2]) and rssi > int(lines_list_1[j+1][2]):
						# ts = time.strftime(DATETIMEFORMAT, time.localtime(int(lines_list_1[j][1])))
						ts = int(lines_list_1[j][1])
						ts_traj_map[ts] = '1'
					j += 1
				length = len(ts_traj_map)
				if length > 1:
					sorted_map = sorted(ts_traj_map.items())
					# print("sorted_map: %s, user_id: %s" % (sorted_map, user_id))
					prev_ts = sorted_map[0][0]
					# print("prev_ts: %s, type: %s" % (prev_ts, type(prev_ts)))
					j = 1
					while j < len(sorted_map):
						cur_ts = sorted_map[j][0]
						peak_interval = cur_ts - prev_ts
						# print("peak_interval: %s" % peak_interval)
						BIG_LIST.append(peak_interval)
						prev_ts = cur_ts
						j += 1
				i += 1
			# print("BIG_LIST: %s, len: %s" % (BIG_LIST, len(BIG_LIST)))
			kmeans = KMeans(n_clusters=2).fit(np.array(BIG_LIST).reshape(-1, 1))
			center = kmeans.cluster_centers_
			print("center: %s" % center)
		except Exception as e:
			raise e
	except Exception as e:
		raise e

if __name__ == '__main__':
	try:
		start_time = time.time()
		run()
		print("It cost %ss" % (time.time() - start_time))
	except Exception as e:
		raise e