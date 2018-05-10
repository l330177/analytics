#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 15 17:06:22 2017

appending one-day data each day
"""

### Change the datapath to the path the file is in
datapath = ''
kpifile = datapath+'/Canada_KPI_reporting_queries.xlsx'
#%%
thedate = str(pd.to_datetime(time.strftime("%Y-%m-%d")) - pd.Timedelta('1 days'))
#%%
outfile = datapath+'/cumulative_daily_report.xlsx'


replacedate = '2016-12-20'

import sys
sys.path.append(datapath)
import pandas as pd
import genKPI as gk

kpi = gk.genKPI('RS')

def genKPIforSheet(sheetname, withgroup, groupname):
    querydf = pd.read_excel(kpifile,sheetname = sheetname)
    if withgroup:
        result = kpi.genKPIWithGroup(querydf,'Query','KPI name',groupname, thedate, thedate, replacedate)
    else:
        result = kpi.genKPINoGroup(querydf,'Query','KPI name', thedate, thedate, replacedate)
    return result
     
#without group, append new data to cumulative_daily_report
writer = pd.ExcelWriter(outfile)
sheetlist = ['Basic KPI', 'User Status of MAU', 'Response of Card Registration']
for sheetname in sheetlist:
    print (sheetname)
    result = genKPIforSheet(sheetname, False, '')
    pre_result = pd.read_excel(outfile,sheetname = sheetname)
    result = pd.concat([pre_result,result])
    result.to_excel(writer, sheet_name=sheetname)
writer.save()


#with group, generate a file for daily data
sheetlist = ['KPI by app version', 'KPI by device model', 'KPI by card name']
grouplist = ['app_ver', 'device_model', 'card_name']
for i in list(range(0, 3)):
    result = genKPIforSheet(sheetlist[i], True, grouplist[i])
    writer = pd.ExcelWriter(datapath+'/'+thedate+'_'+sheetlist[i]+'.xlsx')
    result.to_excel(writer, sheet_name=sheetlist[i])
    writer.save()
