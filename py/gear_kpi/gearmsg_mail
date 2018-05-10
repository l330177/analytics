import os
import sys
import email
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
to_emails = "arthur.l@samsung.com, arthur.l@samsung.com"
msg = MIMEMultipart()
msg.preamble = ''
msg['Subject'] = 'Gear KPI'
msg['From'] = 'notifications@samsungknox.com'
msg['To'] = to_emails
fname = "Gear KPI.csv"
msgfile = open("gearmsg.json",'w')
part = MIMEText('Attached is an important CSV')
msg.attach(part)
part = MIMEApplication(open(fname, 'rb').read())
part.add_header('Content-Disposition', 'attachment', filename=fname)
msg.attach(part)
msgfile.write(msg.as_string())
