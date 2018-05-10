import os
import glob
import time

class FileManager :
    input_folder = os.path.dirname(__file__)+'/input/'
    def __init__(self, input_dir) :
        pass
    def latest(self):
        eDate = time.strftime("%Y-%m-%d")
        #eDate = time.strftime("2017-03-21")
        return 'C:\QlikDataFiles\out\{}validationRst.csv'.format(eDate);
#newest = max(glob.iglob(os.path.join(self.input_folder, '*validat*.csv')), key=os.path.getctime)
#return newest;

def test():
    x = FileManager();
    print x.latest();

if __name__ == '__main__':
    test();
