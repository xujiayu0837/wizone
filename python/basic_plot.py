import os
import datetime
import pandas as pd
from matplotlib import pyplot as plt

SRCDIR = '/Users/xujiayu/python/mean'

def get_paths():
	dir_list = []
	paths_list = []
	for date in os.listdir(SRCDIR):
		dir_path = os.path.join(SRCDIR, date)
		if os.path.isdir(dir_path):
			dir_list.append(os.path.join(SRCDIR, date))
	for dir in dir_list:
		for user_id in os.listdir(dir):
			paths_list.append(os.path.join(dir, user_id))
	return paths_list

def get_df_with_index(df, timestamp):
	df['dt'] = pd.to_datetime(timestamp, unit='s')+datetime.timedelta(hours=8)
	df = df.set_index('dt')
	# df = df.drop('ts', axis=1)
	return df

def run():
	try:
		paths_list = get_paths()
		i = 0
		while i < 100:
			df = pd.read_csv(paths_list[i], sep='|', names=['user_id', 'ts', 'rssi', 'AP'])
			df = get_df_with_index(df, df['ts'])
			df_0 = df[df['AP'] == '14E4E6E186A4']
			df_1 = df[df['AP'] == 'EC172FE3B340']
			print("df: %s, df_0: %s, df_1: %s" % (df, df_0, df_1))
			i += 1
			plt.plot(df_0['rssi'])
			plt.plot(df_1['rssi'])
			plt.xlabel('time')
			plt.ylabel('rssi')
			plt.show()
	except Exception as e:
		raise e

if __name__ == '__main__':
	run()