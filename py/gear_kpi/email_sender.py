import os
import json
import csv
class Emailraw:

	config_path = os.path.dirname(__file__)+'\\config\\';
	def __init__(self):
		pass;

	def _setHtml(self,html):
		path = self.config_path+'tmp.json';
		data = None;
		with open(self.config_path+'gear_msg.json') as data_file:    
			data = json.load(data_file);
			#data['Body']['Html']['Data']=html;
			data['attachment']=csv;
		with open(path,'w') as f:
			json.dump(data, f)
		return path;
	#def send(self,html):
		#msg = self._setHtml(html);
		#dst = self.config_path+'destination_json.txt'
		#cmd='''
#--from notifications@samsungknox.com 
#--destination "file://{dst}" 
#--message "file://{msg}"
#'''.replace('\n','').strip().format(dst=dst, msg=msg);
		#print cmd;
		#os.system(cmd);

def test():
	x = Emailraw();
	x.send("<h1>Hello</h1>");

if __name__ == '__main__':
	test();

