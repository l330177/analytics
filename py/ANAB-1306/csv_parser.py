class CsvParser :

	def __init__(self):
		pass

	def load(self,file_name):
		path = file_name;
		data = [];
		skip_header = True;
		with open(path,'r') as f:
			for l in f:
				if skip_header:
					skip_header=False;
					continue
				(kpi,fname,percent_diff,qlik,expected,isFail,failLimit) = l.replace('\r\n','').split(',');
				delta = 0;
				if (qlik and expected):
					delta = str(float(qlik)-float(expected));
					delta += ' ('+str(failLimit)+')';
				if (not kpi):
					kpi='Not Updating Check'
				if (isFail=='False'):
					isFail='True';
				else:
					isFail='False';
				data.append([kpi,qlik,expected,delta,isFail]);
		return data;

def test():
	import os
	fileName = os.path.dirname(__file__)+'/input/'+'2017-01-03validationRst.csv';
	x = CsvParser();
	data = x.load(fileName)
	print(data);

if __name__=='__main__':
	test();