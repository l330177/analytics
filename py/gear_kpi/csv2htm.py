import sys
import csv
if len(sys.argv) < 3:
    print "Usage: csvToTable.py csv_file html_file"
    exit(1)
reader = csv.reader(open(sys.argv[1]))
htmlfile = open(sys.argv[2],"w")
rownum = 0
htmlfile.write('<table>')
for row in reader:
   if rownum == 0:
      htmlfile.write('<tr>')
      for column in row:
          htmlfile.write('<th>' + column + '</th>')
      htmlfile.write('</tr>')
   else:
      htmlfile.write('<tr>')    
      for column in row:
          htmlfile.write('<td>' + column + '</td>')
      htmlfile.write('</tr>')
   rownum += 1
   htmlfile.write('</table>')
   print "Created " + str(rownum) + " row table."
exit(0)
