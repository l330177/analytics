from csv_parser import CsvParser
from html import Html
from file_manager import FileManager
from email_sender import EmailSender
import os

def main():
	m = EmailSender();
	f = FileManager();
	c = CsvParser();
	data = c.load(f.latest())

	x = Html();
	isValid = True;
	#[kpi,qlik,expected,delta,isFail]
	for (k,q,e,d,f) in data:
		if (f=="True"): # if failed
			isValid = False;
		x.addRow(k,q,e,d,f);

	x.addNotice(isValid);
	x.save(os.path.dirname(__file__)+'/output/latest.html');
	m.send(x.html());

if __name__ == '__main__':
	main();