import csv
import json

csvfile = open('Gear KPI.csv', 'r')
jsonfile = open('Gear KPI.csv'.replace('.csv', '.json'), 'w')

jsonfile.write('{"' + 'Gear KPI.csv'.replace('.csv', '') + '": [\n') # Write JSON parent of data list
fieldnames = csvfile.readline().replace('\n','').split(',')        # Get fieldnames from first line of csv
num_lines = sum(1 for line in open('Gear KPI.csv')) - 1              # Count total lines in csv minus header row

reader = csv.DictReader(csvfile, fieldnames)
i = 0
for row in reader:
  i += 1
  json.dump(row, jsonfile)
  if i < num_lines:
    jsonfile.write(',')
  jsonfile.write('\n')
jsonfile.write(']}')
