import os
import time
import pandas as pd
from matplotlib import pyplot as plt
from util import get_paths
from util import get_uid

SRCDIR = '/Users/xujiayu/python/mean'

def run():
	try:
		try:
			paths_list = get_paths('20170501')
			i = 0
		except Exception as e:
			raise e
		try:
			while i < 100:
				lines_list_0 = []
				lines_list_1 = []
				rssi_list = []
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
				user_id = get_uid(paths_list[i])
				# print("before rssi_list: %s, user_id: %s" % (rssi_list, user_id))
				j = 1
				while j < len(lines_list_0)-1:
					rssi = int(lines_list_0[j][2])
					if rssi > int(lines_list_0[j-1][2]) and rssi > int(lines_list_0[j+1][2]):
						print(rssi)
						rssi_list.append(rssi)
					j += 1
				j = 1
				while j < len(lines_list_1)-1:
					rssi = int(lines_list_1[j][2])
					if rssi > int(lines_list_1[j-1][2]) and rssi > int(lines_list_1[j+1][2]):
						print(rssi)
						rssi_list.append(rssi)
					j += 1
				print("rssi_list: %s, user_id: %s" % (rssi_list, user_id))
				plot_hist(rssi_list, user_id)
				i += 1
			print("len: %s" % len(rssi_list))
		except Exception as e:
			raise e
		try:
			# plot_hist(rssi_list)
			pass
		except Exception as e:
			raise e
	except Exception as e:
		raise e

def plot_hist(rssi_list, user_id):
	try:
		plt.hist(rssi_list)
		plt.title(user_id)
		plt.xlabel('rssi')
		plt.ylabel('count')
		plt.show()
	except Exception as e:
		raise e

if __name__ == '__main__':
	try:
		start_time = time.time()
		run()
		print("It cost %ss" % (time.time() - start_time))
	except Exception as e:
		raise e