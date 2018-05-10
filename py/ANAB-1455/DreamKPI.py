#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 18 17:27:05 2017

"""

import pandas as pd
import genKPI as gk
import time
import datetime

datapath = 'C:\QlikDataFiles\ANAB-1455i
kpifile = datapath+'/Dream_KPI_v3.xlsx'
replacedate = '2017-04-18'

#startdate = '2017-04-25'
#enddate = '2017-04-25'

startdate = enddate = (datetime.datetime.now() - datetime.timedelta(days = 1)).strftime("%Y-%m-%d")

append = True

kpi = gk.genKPI('RS')

outfile = datapath+'/Dream_KPI_breakdown.csv'
querydf = pd.read_excel(kpifile,sheetname = 'KPI')
result = kpi.genKPINoGroup(querydf,'Queries','KPI Name', startdate, enddate, replacedate).transpose()

if append:
    pre_result = pd.read_csv(outfile, index_col = 0)
    result = pd.concat([pre_result,result],axis = 1)
result.to_csv(outfile)

"""
outfile = datapath+'/Dream_KPI_breakdown.csv'
querydf = pd.read_excel(kpifile,sheetname = 'KPIBreakDown')
result = kpi.genKPIWithGroup(querydf,'Queries','KPI Name','device', startdate, enddate, replacedate)

if append:
    pre_result = pd.read_csv(outfile, index_col = [0, 1])
    result = pd.concat([pre_result,result])
      
        
result.to_csv(outfile)
"""
                                                                                                                                                   

