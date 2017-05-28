import os
import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--date")
args = parser.parse_args()

def get_AP(path):
	return path.split('/')[-2]

def run():
	try:
		path_list = []
		dir_list = ['/Users/xujiayu/python/scandata/14E4E6E186A4', '/Users/xujiayu/python/scandata/EC172FE3B340']
		for dir in dir_list:
			path_list.append(os.path.join(dir, args.date))
		dest_dir = os.path.join('/Users/xujiayu/python/data_per_user', args.date)
	except Exception as e:
		raise e
	try:
		if not os.path.exists(dest_dir):
			os.makedirs(dest_dir)
	except Exception as e:
		raise e
	try:
		AP_0 = get_AP(path_list[0])
		AP_1 = get_AP(path_list[1])
		print("AP_0: %s, AP_1: %s" % (AP_0, AP_1))
	except Exception as e:
		raise e
	try:
		if not (os.path.exists(path_list[0]) and os.path.exists(path_list[1])):
			return
		index_0 = 0
		index_1 = 0
		with open(path_list[0], 'r') as fr_0, open(path_list[1], 'r') as fr_1:
			lines_list_0 = fr_0.readlines()
			lines_list_1 = fr_1.readlines()
			while index_0 < len(lines_list_0) and index_1 < len(lines_list_1):
				ts_0 = lines_list_0[index_0].split('|')[-1].strip()
				ts_1 = lines_list_1[index_1].split('|')[-1].strip()
				print("index_0: %s, index_1: %s" % (index_0, index_1))
				print("ts_0: %s, ts_1: %s" % (ts_0, ts_1))
				if ts_0 <= ts_1:
					user_id = lines_list_0[index_0].split('|')[0]
					rssi = lines_list_0[index_0].split('|')[1]
					index_0 += 1
					if float(rssi) <= -90:
						continue
					print("index_0: %s, len: %s" % (index_0, len(lines_list_0)))
					line_to_write = '|'.join([user_id, ts_0, rssi, AP_0])
					with open(os.path.join(dest_dir, user_id), 'a') as fw:
						fw.write(line_to_write+'\n')
				else:
					user_id = lines_list_1[index_1].split('|')[0]
					rssi = lines_list_1[index_1].split('|')[1]
					index_1 += 1
					if float(rssi) <= -90:
						continue
					print("index_1: %s, len: %s" % (index_1, len(lines_list_1)))
					line_to_write = '|'.join([user_id, ts_1, rssi, AP_1])
					with open(os.path.join(dest_dir, user_id), 'a') as fw:
						fw.write(line_to_write+'\n')
			while index_0 < len(lines_list_0):
				user_id = lines_list_0[index_0].split('|')[0]
				rssi = lines_list_0[index_0].split('|')[1]
				index_0 += 1
				if float(rssi) <= -90:
						continue
				print("index_0: %s, len: %s" % (index_0, len(lines_list_0)))
				line_to_write = '|'.join([user_id, ts_0, rssi, AP_0])
				with open(os.path.join(dest_dir, user_id), 'a') as fw:
					fw.write(line_to_write+'\n')
			while index_1 < len(lines_list_1):
				user_id = lines_list_1[index_1].split('|')[0]
				rssi = lines_list_1[index_1].split('|')[1]
				index_1 += 1
				if float(rssi) <= -90:
						continue
				print("index_1: %s, len: %s" % (index_1, len(lines_list_1)))
				line_to_write = '|'.join([user_id, ts_1, rssi, AP_1])
				with open(os.path.join(dest_dir, user_id), 'a') as fw:
					fw.write(line_to_write+'\n')
	except Exception as e:
		raise e
	
if __name__ == '__main__':
	start_time = time.time()
	run()
	print("it cost: %s seconds" % (time.time() - start_time))