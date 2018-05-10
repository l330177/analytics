#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  7 18:49:09 2017

genKPIs with group by
genKPIs without group by
dailys based
"""


from sqlalchemy import create_engine
import pandas as pd
### Modify the credential
###'redshift+psycopg2://USRNAME:PASSWORD@localhost:45439/trsanalytics'
CREDENTIAL = ''
class genKPI:
    def __init__(self, enginename):
        self.engine = create_engine(CREDENTIAL)
        self.with_group = False



    def runSQL(self, thedate,sql,replacedDate):
        thesql = sql.replace(replacedDate,str(thedate+pd.DateOffset(1))[:10]) 
        print(thedate)
        engine = create_engine(CREDENTIAL)
        result = pd.read_sql_query(thesql,engine)
        if self.with_group:
            result['date']=[str(thedate)[:10]]*len(result)
        else:
            result.index=[str(thedate)[:10]]
        return (result)
    
    
    def getOneKPI(self, sql,startdate,enddate,originaldate):    
        alldates = pd.Series(pd.date_range(startdate,enddate))    
        result = pd.DataFrame()
        for i in alldates:
            result = pd.concat([result,self.runSQL(i,sql,originaldate)])
        if self.with_group:
            result = result.reset_index(drop=True)
        return result

    """The query does not contain group by. One-day query returns one row. There could return multipal columns for one raw. """
    def genKPINoGroup(self, querydf,queryfield, namefield,startdate,enddate,originaldate):
        self.with_group = False
        result = pd.DataFrame()
        for i in querydf.index:
            oneresult = self.getOneKPI(querydf.iloc[i][queryfield].replace('\n',' ').replace('\t',' '),startdate,enddate,originaldate)
            columnnames = list(oneresult.columns)
            if len(columnnames) == 1:
                oneresult.columns = [querydf.iloc[i][namefield]]
            else:
                oneresult.columns = [(querydf.iloc[i][namefield]+'_'+j) for j in columnnames]
            result = pd.concat([result,oneresult],axis = 1)
        return result
    
    """The query contains group by. One-day query returns multiple rows. One row can only have one value (count). """
    def genKPIWithGroup(self, querydf,queryfield, namefield,groupname,startdate,enddate,originaldate):
        self.with_group = True
        result = pd.DataFrame()
        for i in querydf.index:
            oneresult = self.getOneKPI(querydf.iloc[i][queryfield].replace('\n',' ').replace('\t',' '),startdate,enddate,originaldate)
            oneresult[groupname] = oneresult[groupname].fillna('NULL')
            oneresult.index=pd.MultiIndex.from_tuples(list(zip(*[list(oneresult['date']),list(oneresult[groupname])])), names=['date', groupname])
            oneresult = pd.DataFrame(oneresult['count'])
            oneresult.columns = [querydf.iloc[i][namefield]]
            result = pd.concat([result,oneresult],axis = 1)
        return result
