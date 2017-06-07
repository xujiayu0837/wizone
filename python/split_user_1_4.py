import os
import time
from util import get_AP_0
from util import get_date_0

SRCDIR = '/Users/xujiayu/python/mean_1_2'
DESTDIR = '/Users/xujiayu/python/uid_mean'

def run():
	try:
		try:
			dir_list = []
			paths_list = []
			for dir in os.listdir(SRCDIR):
				dir_path = os.path.join(SRCDIR, dir)
				if os.path.isdir(dir_path):
					dir_list.append(dir_path)
			for item in dir_list:
				for filename in os.listdir(item):
					paths_list.append(os.path.join(item, filename))
		except Exception as e:
			raise e
		try:
			i = 0
			uid_lines_map = {}
			while i < len(paths_list):
				AP = get_AP_0(paths_list[i])
				date = get_date_0(paths_list[i])
				dest_dir = os.path.join(DESTDIR, date)
				if not os.path.exists(dest_dir):
					os.mkdir(dest_dir)
				print("AP: %s, date: %s, dest_dir: %s" % (AP, date, dest_dir))
				with open(paths_list[i], 'r') as fr:
					for line in fr:
						line_list = line.split("|")
						user_id = line_list[0]
						if user_id in uid_lines_map:
							uid_lines_map[user_id].append([user_id, line_list[-1].strip(), line_list[1], AP])
						else:
							line_per_user = [user_id, line_list[-1].strip(), line_list[1], AP]
							uid_lines_map[user_id] = [line_per_user]
				i += 1
			for k, v in uid_lines_map.items():
				sorted_list = sorted(v, key=lambda x: x[1])
				with open(os.path.join(dest_dir, k), 'a') as fw:
					for item in sorted_list:
						line_to_write = '|'.join(item)
						print("line_to_write: %s" % line_to_write)
						fw.write(line_to_write+'\n')
		except Exception as e:
			raise e
	except Exception as e:
		raise e
	
if __name__ == '__main__':
	start_time = time.time()
	run()
	print("it cost: %s seconds" % (time.time() - start_time))