import os
import datetime
import pandas as pd
from matplotlib import pyplot as plt
from util import get_date
from util import get_uid
from util import get_paths

SRCDIR = '/Users/xujiayu/python/mean'

def get_df_with_index(df, timestamp):
	df['dt'] = pd.to_datetime(timestamp, unit='s')+datetime.timedelta(hours=8)
	df = df.set_index('dt')
	# df = df.drop('ts', axis=1)
	return df

def run():
	try:
		paths_list = get_paths('20170501')
		i = 0
		while i < 100:
			# df = pd.read_csv(paths_list[i], sep='|', names=['user_id', 'ts', 'rssi', 'AP'])
			# df = get_df_with_index(df, df['ts'])
			# df_0 = df[df['AP'] == '14E4E6E186A4']
			# df_1 = df[df['AP'] == 'EC172FE3B340']
			# print("df: %s, df_0: %s, df_1: %s" % (df, df_0, df_1))
			# i += 1
			# plt.plot(df_0['rssi'])
			# plt.plot(df_1['rssi'])
			# plt.xlabel('time')
			# plt.ylabel('rssi')
			# plt.show()

			with open(paths_list[i], 'r') as fr:
				length = len(fr.readlines())
				date = get_date(paths_list[i])
				user_id = get_uid(paths_list[i])
			with open(paths_list[i], 'r') as fr:
				time_slices_list = []
				prev_line = fr.readline()
				prev_list = prev_line.split("|")
				user_id = prev_list[0]
				prev_ts = int(prev_list[1])
				prev_rssi = int(prev_list[2])
				prev_AP = prev_list[-1].strip()
				time_slices_list.append((prev_ts, prev_rssi, prev_AP))
				i += 1
				j = 1
				while j < length:
					j += 1
					cur_line = fr.readline()
					cur_list = cur_line.split("|")
					cur_ts = int(cur_list[1])
					cur_rssi = int(cur_list[2])
					cur_AP = cur_list[-1].strip()
					if cur_ts - prev_ts <= 120:
						time_slices_list.append((cur_ts, cur_rssi, cur_AP))
					else:
						df = pd.DataFrame(time_slices_list)
						df = get_df_with_index(df, df[0])
						df_0 = df[df[2] == '14E4E6E186A4']
						df_1 = df[df[2] == 'EC172FE3B340']
						print("df: %s, df_0: %s, df_1: %s, user_id: %s" % (df, df_0, df_1, user_id))
						plt.plot(df_0[1])
						plt.plot(df_1[1])
						plt.title(user_id)
						plt.xlabel('time')
						plt.ylabel('rssi')
						plt.show()
						time_slices_list[:] = []
						time_slices_list.append((cur_ts, cur_rssi, cur_AP))
					prev_ts = cur_ts
	except Exception as e:
		raise e

if __name__ == '__main__':
	run()