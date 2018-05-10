import os
class Html:
	template_folder = os.path.dirname(__file__)+'/templates/'

	templates = {
	'base':'base.template.html',
	'notice-valid':'notice-valid.partial.html',
	'notice-invalid':'notice-invalid.partial.html',
	'row':'row.partial.html',
	}
	template_placeholders = {
	'notice':'<!-- notice-placeholder -->',
	'row':'<!-- row-placeholder -->',
	}

	row_template_placeholder = {
	'kpi':'<!-- kpi -->',
	'qlik':'<!-- qlik -->',
	'expected':'<!-- expected -->',
	'delta':'<!-- delta -->',
	'pass':'<!-- pass -->',
	}

	row_html = '';
	notice_html = '';
	def __init__(self):
		pass

	def addRow(self,k,q,e,d,p):
		path = self.template_folder+self.templates['row'];
		data = None;
		with open(path, 'r') as f:
			data=f.read().replace('\n', '');
			data=data.replace(self.row_template_placeholder['kpi'],str(k));
			data=data.replace(self.row_template_placeholder['qlik'],str(q));
			data=data.replace(self.row_template_placeholder['expected'],str(e));
			data=data.replace(self.row_template_placeholder['delta'],str(d));
			data=data.replace(self.row_template_placeholder['pass'],str(p));
		self.row_html += data;

	def addNotice(self,isValid):
		data = None;
		path = self.template_folder;
		if isValid:
			path += self.templates['notice-valid'];
		else:
			path += self.templates['notice-invalid'];
		with open(path, 'r') as f:
			data=f.read().replace('\n', '');
		self.notice_html = data;

	def save(self,outputDest):
		path = self.template_folder+self.templates['base'];
		data = None;
		with open(path, 'r') as f:
			data=f.read().replace('\n', '');
			data=data.replace(self.template_placeholders['notice'],str(self.notice_html));
			data=data.replace(self.template_placeholders['row'],str(self.row_html));
		with open(outputDest, 'w') as f:
			f.write(data);
	def html(self):
		path = self.template_folder+self.templates['base'];
		data = None;
		with open(path, 'r') as f:
			data=f.read().replace('\n', '');
			data=data.replace(self.template_placeholders['notice'],str(self.notice_html));
			data=data.replace(self.template_placeholders['row'],str(self.row_html));
		return data;

def test():
	x = Html();
	x.addNotice(False);
	x.addRow('ru',100,100,0,"False");
	x.save(os.path.dirname(__file__)+'/output/test.html');

if __name__=='__main__':
	test();
