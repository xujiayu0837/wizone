import math

SRCFILE = '/Users/xujiayu/Desktop/wicloud/新wifi探针.csv'
DESTFILE = '/Users/xujiayu/python/group.csv'

def read_file(path):
	try:
		group_id_map = {}
		loc_list = []
	except Exception as e:
		raise e
	try:
		with open(path, 'rb') as fr:
			lines_list = fr.readlines()
		for item in lines_list[2:]:
			loc_str = str(item).split(',')[0].strip("b'")
			# print("loc_str: %s" % loc_str)
			if loc_str:
				loc_list.append(loc_str)
		return loc_list

	except Exception as e:
		raise e

def get_bin_len(length):
	try:
		for i in range(1,10):
			if math.pow(2, i) >= length:
				return i
	except Exception as e:
		raise e

def get_bin_code(item, bin_len):
	try:
		bin_code = bin(item)[2:].zfill(bin_len)
		return bin_code
	except Exception as e:
		raise e

def write_csv(path, group_id_map):
	try:
		with open(path, 'a') as fw:
			for k, v in group_id_map.items():
				line_to_write = ','.join([k, v])+'\n'
				fw.write(line_to_write)
	except Exception as e:
		raise e

if __name__ == '__main__':
	try:
		group_id_map = {}
		loc_list = read_file(SRCFILE)
		length = len(loc_list)
		bin_len = get_bin_len(length)
		for i, item in enumerate(loc_list):
			with open(DESTFILE, 'a') as fw:
				bin_code = get_bin_code(i+1, bin_len)
				line_to_write = ','.join([item, bin_code])+'\n'
				fw.write(line_to_write)
			print("loc_str: %s, index: %s, bin_code: %s" % (item, i, bin_code))
	except Exception as e:
		raise e