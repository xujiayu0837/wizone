# -*- coding: utf-8 -*-
import os
import time
import datetime
from multiprocessing.dummy import Pool as Pool
from kafka import KafkaProducer

SRCDIR = '/home/data/scandata/'
SERVER = '10.103.93.27:9092'
TOPIC = 'scandata'

def get_AP(dir):
	return dir.split('/')[-1]

def write_to_kafka(producer, data_with_AP):
	try:
		for item in data_with_AP:
			bytes_to_write = item.encode('utf-8')
			producer.send(TOPIC, bytes_to_write)
	except Exception as e:
		raise e

def handle(sub_dir):
	try:
		try:
			today_str = datetime.date.today().strftime("%Y%m%d")
			path = ''
			for filename in os.listdir(sub_dir):
				if filename == today_str:
					path = os.path.join(sub_dir, today_str)
			AP = get_AP(sub_dir)
			# print "AP: %s" % AP
			producer = KafkaProducer(bootstrap_servers=SERVER)
		except Exception as e:
			raise e
		try:
			if path == '':
				return
			f = open(path, "r")
			history_list = f.readlines()
			last_pos = f.tell()
			f.close()
			init_ts = 0
			while True:
				ts = os.stat(path).st_mtime
				if init_ts != ts:
					init_ts = ts
					with open(path, 'r') as fr:
						# print "last_pos: %s" % last_pos
						fr.seek(last_pos)
						data = fr.readlines()
						last_pos = fr.tell()
						if len(data) > 0:
							data_with_AP = ['|'.join([item.strip(), AP]) for item in data]
							print "write data: %s to kafka. server: %s, topic: %s" % (data_with_AP, SERVER, TOPIC)
							write_to_kafka(producer, data_with_AP)
							time.sleep(1)
				else:
					time.sleep(1)
		except Exception as e:
			raise e
	except Exception as e:
		raise e

if __name__ == '__main__':
	try:
		dir_list = os.listdir(SRCDIR)
		path_list = []
		for item in dir_list:
			path_list.append(os.path.join(SRCDIR, item))
		pool = Pool(40)
		pool.map(handle, path_list)
		pool.close()
		pool.join()
	except Exception as e:
		raise e