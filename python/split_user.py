import os
import time
from multiprocessing.dummy import Pool as Pool

SRCDIR = "/Users/xujiayu/python/scandata/"

def handle(item):
	try:
		path_list = item.split('/')
		AP = path_list[-2]
		date = path_list[-1]
		dest_dir = '/'.join(['/Users/xujiayu/python/data_per_user', date])
		try:
			if not os.path.exists(dest_dir):
				# print("*"*20)
				os.makedirs(dest_dir)
		except Exception as e:
			# return
			raise e
		try:
			with open(item, 'r') as fr:
				for line in fr:
					line_list = line.split('|')
					user_id = line_list[0]
					rssi = line_list[1]
					ts = line_list[2].strip()
					with open('/'.join([dest_dir, user_id]), 'a') as fw:
						line_to_write = '|'.join([user_id, ts, rssi, AP])
						fw.write(line_to_write + '\n')
		except Exception as e:
			# return
			raise e
	except Exception as e:
		# return
		raise e

if __name__ == '__main__':
	try:
		start_time = time.time()
		dir_list = []
		filename_list = []
		for sub_dir in os.listdir(SRCDIR):
			dir_path = os.path.join(SRCDIR, sub_dir)
			print("dir_path: %s" % dir_path)
			for filename in os.listdir(dir_path):
				filename_list.append(os.path.join(dir_path, filename))
		# print("filename_list: %s" % filename_list)
		pool = Pool()
		pool.map(handle, filename_list)
		pool.close()
		pool.join()
		print("it cost: %s" % (time.time() - start_time))
	except Exception as e:
		raise e