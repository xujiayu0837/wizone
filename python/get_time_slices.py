import os
import time
import statistics
from multiprocessing.dummy import Pool as Pool
import pandas as pd
# from matplotlib import pyplot as plt
# from basic_plot import get_df_with_index
from get_mean import get_dest_dir

SRCDIR = '/Users/xujiayu/python/mean'
DESTDIR = '/Users/xujiayu/python/timeslices'

def get_date(path):
	return path.split("/")[-2]

def handle(item):
	try:
		# 需手动创建dest_dir. cd /Users/xujiayu/python, mkdir -p timeslices/date
		dest_dir = get_dest_dir(DESTDIR, item)
		user_id = item.split('/')[-1]
	except Exception as e:
		raise e
	try:
		with open(item, 'r') as fr:
			length = len(fr.readlines())
			date = get_date(item)
		with open(item, 'r') as fr, open(os.path.join(dest_dir, user_id), 'a') as fw:
			time_slices_list = []
			prev_line = fr.readline()
			prev_list = prev_line.split("|")
			user_id = prev_list[0]
			prev_ts = int(prev_list[1])
			prev_rssi = int(prev_list[2])
			prev_AP = prev_list[-1].strip()
			time_slices_list.append((prev_ts, prev_rssi, prev_AP))
			i = 1
			while i < length:
				i += 1
				cur_line = fr.readline()
				cur_list = cur_line.split("|")
				cur_ts = int(cur_list[1])
				cur_rssi = int(cur_list[2])
				cur_AP = cur_list[-1].strip()
				if cur_ts - prev_ts <= 120:
					time_slices_list.append((cur_ts, cur_rssi, cur_AP))
				else:
					if time_slices_list[-1][0] - time_slices_list[0][0] < 6:
						print("passerby. user_id: %s, date: %s, time_slices_list: %s" % (user_id, date, time_slices_list))
					if len(time_slices_list) <= 1:
						print("1 timeslice. time_slices_list: %s, filename: %s" % (time_slices_list, item))
					else:
						rssi_list = [item[1] for item in time_slices_list]
						stdev = statistics.stdev(rssi_list)
						print("stdev: %s, filename: %s" % (stdev, item))
					time_slices_list.append(user_id)
					# fw.write(str(time_slices_list)+'\n')
					# df = pd.DataFrame(time_slices_list)
					# df = get_df_with_index(df, df[0])
					# df_0 = df[df[2] == '14E4E6E186A4']
					# df_1 = df[df[2] == 'EC172FE3B340']
					# print("df: %s, df_0: %s, df_1: %s, user_id: %s" % (df, df_0, df_1, user_id))
					# plt.plot(df_0['rssi'])
					# plt.plot(df_1['rssi'])
					# plt.show()
					time_slices_list[:] = []
					time_slices_list.append((cur_ts, cur_rssi, cur_AP))
				prev_ts = cur_ts
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