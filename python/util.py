import os
import datetime
import time

def get_AP_0(path):
	return path.split("/")[-2]

def get_date_0(path):
	return path.split("/")[-1]

def get_date(path):
	return path.split("/")[-2]

def get_uid(path):
	return path.split("/")[-1]

def get_dest_dir(dir, item):
	try:
		date = get_date(item)
		dest_dir = os.path.join(dir, date)
		return dest_dir
	except Exception as e:
		raise e

def mkdir(start_date, days):
	PWD = '/Users/xujiayu/python'
	subdir = 'uid_mean'
	dir_str = os.path.join(PWD, subdir)
	date_str = start_date
	for i in range(days):
		if not os.path.exists(os.path.join(dir_str, date_str)):
			os.makedirs(os.path.join(dir_str, date_str))
			date_obj = datetime.datetime.strptime(date_str, '%Y%m%d') + datetime.timedelta(days=1)
			date_str = date_obj.strftime('%Y%m%d')

def get_paths(skip_dates):
	try:
		SRCDIR = '/Users/xujiayu/python/mean'
		dir_list = []
		paths_list = []
		for date in os.listdir(SRCDIR):
			if skip_dates:
				if isinstance(skip_dates, str):
					if date == skip_dates:
						continue
				elif isinstance(skip_dates, list):
					for item in skip_dates:
						if date == item:
							continue
			# if date == '20170501':
			# 	continue
			dir_path = os.path.join(SRCDIR, date)
			if os.path.isdir(dir_path):
				dir_list.append(dir_path)
		# print("dir_list: %s" % dir_list)
		for dir in dir_list:
			for user_id in os.listdir(dir):
				paths_list.append(os.path.join(dir, user_id))
		return paths_list
	except Exception as e:
		raise e

def get_paths_0(dates):
	try:
		SRCDIR = '/Users/xujiayu/python/mean'
		dir_list = []
		paths_list = []
		for dir in os.listdir(SRCDIR):
			if dir == dates:
				dir_path = os.path.join(SRCDIR, dir)
				# print("dir_path: %s" % dir_path)
				if not os.path.isdir(dir_path):
					continue
				dir_list.append(dir_path)
		for dir in dir_list:
			for filename in os.listdir(dir_path):
				path = os.path.join(dir_path, filename)
				paths_list.append(path)
		# print("paths_list: %s" % paths_list)
		return paths_list
	except Exception as e:
		raise e

def convert_datetime_to_timestamp(dt, format):
	try:
		time_odj = time.strptime(dt, format)
		ts = time.mktime(time_odj)
		return ts
	except Exception as e:
		raise e

def get_time_index(ts, init_ts, time_interval):
	try:
		time_index = int((ts - init_ts) / time_interval)
		return time_index
	except Exception as e:
		raise e

if __name__ == '__main__':
	# start_date = '20170502'
	# days = 1
	# mkdir(start_date, days)
	get_paths('20170501')
	get_paths_0('20170502')
