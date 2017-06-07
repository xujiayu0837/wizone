import os
import time
from multiprocessing.dummy import Pool as Pool

SRCDIR_0 = '/Users/xujiayu/python/scandata/14E4E6E186A4'
SRCDIR_1 = '/Users/xujiayu/python/scandata/EC172FE3B340'
DESTDIR = '/Users/xujiayu/python/data_per_user'
OUIPATH = '/Users/xujiayu/Downloads/oui_new.txt'

def get_oui_list():
	with open(OUIPATH, 'r') as fr:
		oui_list = [line.split("|")[0] for line in fr]
	return oui_list

def get_AP(path):
	return path.split('/')[-1]

def run():
	try:
		oui_list = get_oui_list()
		print("oui_list: %s" % oui_list)
		AP_0 = get_AP(SRCDIR_0)
		AP_1 = get_AP(SRCDIR_1)
		print("AP_0: %s, AP_1: %s" % (AP_0, AP_1))
	except Exception as e:
		raise e
	try:
		for filename in os.listdir(SRCDIR_0):
			if filename in os.listdir(SRCDIR_1):
				src_path_0 = os.path.join(SRCDIR_0, filename)
				src_path_1 = os.path.join(SRCDIR_1 , filename)
				print("src_path_0: %s, src_path_1: %s" % (src_path_0, src_path_1))
				try:
					if not os.path.exists(os.path.join(DESTDIR, filename)):
						os.makedirs(os.path.join(DESTDIR, filename))
				except Exception as e:
					raise e
				try:
					index_0 = 0
					index_1 = 0
					with open(src_path_0, 'r') as fr_0, open(src_path_1, 'r') as fr_1:
						lines_list_0 = fr_0.readlines()
						lines_list_1 = fr_1.readlines()
						while index_0 < len(lines_list_0) and index_1 < len(lines_list_1):
							lst_0 = lines_list_0[index_0].split('|')
							ts_0 = lst_0[-1].strip()
							lst_1 = lines_list_1[index_1].split('|')
							ts_1 = lst_1[-1].strip()
							print("ts_0: %s, ts_1: %s" % (ts_0, ts_1))
							if ts_0 <= ts_1:
								user_id = lst_0[0]
								rssi = lst_0[1]
								index_0 += 1
								if not (user_id[0:6] in oui_list and float(rssi) > -90):
									continue
								print("index_0: %s, len: %s" % (index_0, len(lines_list_0)))
								line_to_write = '|'.join([user_id, ts_0, rssi, AP_0])
								dest_path = os.path.join(os.path.join(DESTDIR, filename), user_id)
								with open(dest_path, 'a') as fw:
									fw.write(line_to_write+'\n')
							else:
								user_id = lst_0[0]
								rssi = lst_0[1]
								index_1 += 1
								if not (user_id[0:6] in oui_list and float(rssi) > -90):
									continue
								print("index_1: %s, len: %s" % (index_1, len(lines_list_1)))
								line_to_write = '|'.join([user_id, ts_1, rssi, AP_1])
								dest_path = os.path.join(os.path.join(DESTDIR, filename), user_id)
								with open(dest_path, 'a') as fw:
									fw.write(line_to_write+'\n')
						while index_0 < len(lines_list_0):
							user_id = lst_0[0]
							rssi = lst_0[1]
							index_0 += 1
							if not (user_id[0:6] in oui_list and float(rssi) > -90):
								continue
							print("index_0: %s, len: %s" % (index_0, len(lines_list_0)))
							line_to_write = '|'.join([user_id, ts_0, rssi, AP_0])
							dest_path = os.path.join(os.path.join(DESTDIR, filename), user_id)
							with open(dest_path, 'a') as fw:
								fw.write(line_to_write+'\n')
						while index_1 < len(lines_list_1):
							user_id = lst_0[0]
							rssi = lst_0[1]
							index_1 += 1
							if not (user_id[0:6] in oui_list and float(rssi) > -90):
								continue
							print("index_1: %s, len: %s" % (index_1, len(lines_list_1)))
							line_to_write = '|'.join([user_id, ts_1, rssi, AP_1])
							dest_path = os.path.join(os.path.join(DESTDIR, filename), user_id)
							with open(dest_path, 'a') as fw:
								fw.write(line_to_write+'\n')
				except Exception as e:
					raise e
	except Exception as e:
		raise e
	
if __name__ == '__main__':
	start_time = time.time()
	run()
	print("it cost: %s seconds" % (time.time() - start_time))