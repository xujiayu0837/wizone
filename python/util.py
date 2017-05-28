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