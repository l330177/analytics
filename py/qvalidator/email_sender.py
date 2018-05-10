import os
import json
class EmailSender:

	config_path = os.path.dirname(__file__)+'/config/';
	def __init__(self):
		pass;

	def _setHtml(self,html):
		path = self.config_path+'tmp.json';
		data = None;
		with open('./config/message_json.txt') as data_file:    
			data = json.load(data_file);
			data['Body']['Html']['Data']=html;
		with open(path,'w') as f:
			json.dump(data, f)
		return path;
	def send(self,html):
		msg = self._setHtml(html);
		dst = self.config_path+'destination_json.txt'
		cmd='''
"aws ses send-email 
--from notifications@samsungknox.com 
--destination {dst} 
--message {msg}
"'''.replace('\n','').strip().format(dst=dst, msg=msg);
		print cmd;
		os.system(cmd);

def test():
	x = EmailSender();
	x.send("<h1>Hello</h1>");

if __name__ == '__main__':
	test();

