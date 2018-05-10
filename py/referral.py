# -*- coding: utf-8 -*-
"""
Created on Thu Aug 25 14:26:08 2016

@author: a.seshadri
"""

import pandas as pd
from sqlalchemy import create_engine
import csv
import datetime

def get_p0(date):
    red_engine = create_engine('redshift+psycopg2://qlik:qlikiest@redshift-cluster.czlcstchuzge.us-east-1.redshift.amazonaws.com:45439/analytics')
    tu_query = """
    select distinct userid
    from fact_ws_notification w inner join dim_device m on w.dimdeviceid = m.id
    where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null) and m.countryCode ='US' and convert_timezone('UTC', 'EST', w.createdat) < '""" + date + "';"
    cru_query = """
    select distinct muserid
    from fact_token_history t inner join dim_device m on t.dimdeviceid = m.id
    where t.tokenid is not null and m.countryCode = 'US' and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + "';"
    lau_query = """
    select dimuserid
     from fact_ws_notification w inner join dim_device m on w.dimdeviceid = m.id
     where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null) and m.countryCode ='US' and convert_timezone('UTC', 'EST', w.createdat) < '""" + date + """'
     group by dimuserid
     having count(*) < 6;
     """
    hau_query = """
    select dimuserid
     from fact_ws_notification w inner join dim_device m on w.dimdeviceid = m.id
     where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null) and m.countryCode ='US' and convert_timezone('UTC', 'EST', w.createdat) < '""" + date + """'
     group by dimuserid
     having count(*) >= 6;
     """
    tu = pd.read_sql_query(tu_query,red_engine)
    cru = pd.read_sql_query(cru_query,red_engine)
    lau = pd.read_sql_query(lau_query,red_engine)
    hau = pd.read_sql_query(hau_query,red_engine)
    
    
    promo_engine = create_engine('mysql://SMPSCMNQLIQ:zAj7jbPV~N@db3-mys-us1a-prd.internal:43306/promotion')
    sr_query = """
    select distinct referrer_id 
    from prmt_ref
    where referrer_claim_time IS NOT NULL and referrer_claim_time < '"""+ date + "';"

    sr = pd.read_sql_query(sr_query, promo_engine)
    
    data = [date]
    data.append(len(set(sr.referrer_id).difference(set(cru.dimuserid))))
    data.append(len(set(sr.referrer_id).difference(set(tu.dimuserid))))
    data.append(len(set(sr.referrer_id).intersection(set(lau.dimuserid))))
    data.append(len(set(sr.referrer_id).intersection(set(hau.dimuserid))))
    
    with open('referral_p0.csv','ab') as fp:
        wr = csv.writer(fp,delimiter=',')
        wr.writerow(data)



if __name__ == "__main__":
    fmt = '%Y-%m-%d %H:%M:%S'
    date = datetime.datetime.now()
    date = date.strftime(fmt)
    get_p0(date)
