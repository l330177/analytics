import os
import glob

class FileManager :
	input_folder = os.path.dirname(__file__)+'/input/'
	def __init__(self):
		pass

	def latest(self):
		newest = max(glob.iglob(os.path.join(self.input_folder, '*.csv')), key=os.path.getctime)
		return newest;

def test():
	x = FileManager();
	print x.latest();

if __name__ == '__main__':
	test();