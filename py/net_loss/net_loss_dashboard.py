# -*- coding: utf-8 -*-
"""
Created on Tue Feb 14 11:39:28 2017

@author: abl
"""

import pandas as pd
from sqlalchemy import create_engine
import csv
import datetime

#Engine to connect to redshift
red_engine = create_engine('redshift+psycopg2://admin:uhuhuhuh@redshift-cluster.czlcstchuzge.us-east-1.redshift.amazonaws.com:45439/analytics')

#get the daily net change
def daily_net_change(dt):
    fmt = '%Y-%m-%d %H:%M:%S'
    date = dt.strftime(fmt)
    
    net_cru_start_query = """
        select distinct trsdimuserid
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US'
          and status != 'DISPOSED' and t.createdat = (select max(t2.createdat) from trs_fact_token_history t2 where t2.tokenid = t.tokenid and convert_timezone('UTC', 'EST', t2.createdat) < '""" + date + "');"

    net_cru_end_query = """
        select distinct trsdimuserid
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US'
          and status != 'DISPOSED' and t.createdat = (select max(t2.createdat) from trs_fact_token_history t2 where t2.tokenid = t.tokenid and convert_timezone('UTC', 'EST', t2.createdat) < ('""" + date + "'::timestamp + interval '1 day'));"
          
    cum_cru_start_query = """
        select distinct trsdimuserid
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US'
        and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + "';"          
              
    net_cru_start = pd.read_sql_query(net_cru_start_query, red_engine)
    net_cru_end = pd.read_sql_query(net_cru_end_query, red_engine)
    cum_cru_start = pd.read_sql_query(cum_cru_start_query, red_engine)
    net_cru_added_users = set(net_cru_end.trsdimuserid).difference(set(net_cru_start.trsdimuserid))
    net_cru_add = len(set(net_cru_end.trsdimuserid).difference(set(net_cru_start.trsdimuserid)))
    new_net_cru_add = len(net_cru_added_users.difference(set(cum_cru_start.trsdimuserid)))
    return_net_cru_add = len(net_cru_added_users.intersection(set(cum_cru_start.trsdimuserid)))
    net_cru_delete = len(set(net_cru_start.trsdimuserid).difference(set(net_cru_end.trsdimuserid)))
    net_cru_deleted_users = list(set(net_cru_start.trsdimuserid).difference(set(net_cru_end.trsdimuserid)))
    net_change = net_cru_add - net_cru_delete
    
    return [date[:10], len(net_cru_start.index), len(net_cru_end.index), net_cru_add, new_net_cru_add, return_net_cru_add, net_cru_delete, net_change], net_cru_deleted_users

#get the profile of lost users    
def loss_profile(dt, users):
    fmt = '%Y-%m-%d %H:%M:%S'
    date = dt.strftime(fmt)
        
    time_dist_query = """
        select min_date, count(distinct trsdimuserid)
        from
        (select trsdimuserid, trunc(min(t.createdat)) as min_date
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + "' and trsdimuserid in " + str(users) + """
        group by trsdimuserid) 
        group by min_date;"""
        
    time_dist = pd.read_sql_query(time_dist_query,red_engine)
    
    return [date[:10], time_dist]
    
#get factors that could be possible reasons    
def loss_factors(dt, users):
    fmt = '%Y-%m-%d %H:%M:%S'
    date = dt.strftime(fmt)
    
    loss_payment_network_query = """
        select tr, count(distinct trsdimuserid) as count_users
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countrycode = 'US' and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + "' and trsdimuserid in " + str(users) + """
        group by tr
        order by count_users;"""
        
    loss_card_issuer_query = """
        select alias, count(distinct trsdimuserid) as count_users
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id inner join trs_dim_card_issuer i on t.trsdimcardissuerid = i.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countrycode = 'US' and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + "' and trsdimuserid in " + str(users) + """
        group by alias
        order by count_users desc; """

    loss_active_card_query = """
        select count(distinct trsdimuserid) as count_users
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id inner join trs_dim_card_issuer i on t.trsdimcardissuerid = i.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and status = 'ACTIVE' and m.countrycode = 'US' and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + "' and trsdimuserid in " + str(users) + ";"
    
    loss_payment_network = pd.read_sql_query(loss_payment_network_query, red_engine)
    loss_card_issuer = pd.read_sql_query(loss_card_issuer_query, red_engine)
    loss_active_card = pd.read_sql_query(loss_active_card_query, red_engine)
    
    return [date[:10], loss_payment_network, loss_card_issuer, loss_active_card]
    
    
if __name__ == "__main__":
#    end_date = datetime.datetime(2017,2,14,0,0,0)
    end_date = datetime.datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond=0)
    start_date = end_date + datetime.timedelta(days=-1)
    
    fmt = '%Y-%m-%d'
    dt = start_date
    while dt < end_date:
        #query the database
        net_change, users = daily_net_change(dt)
        users = tuple([str(user) for user in users])
        profile = loss_profile(dt, users)
        factors = loss_factors(dt, users)
        print "done", dt        
        dt = dt+datetime.timedelta(days=1)
        
        #processing
        days = profile[1]
        del_date = dt.date()
        days['num_days'] = days.apply(lambda row: (del_date - row['min_date']).days, axis = 1)
        days_dist = [profile[0],sum(days[days['num_days'] <= 30]['count'].values),sum(days[(days['num_days']<=60) & (days['num_days'] > 30)]['count'].values),sum(days[(days['num_days']<=90) & (days['num_days'] > 60)]['count'].values),sum(days[days['num_days']>90]['count'].values)]
        
        payment_network = factors[1]
        card_issuer = factors[2]
        card_issuer.insert(0,'date',factors[0])
        active_card = factors[3]
        pay_net_split = [factors[0], payment_network[payment_network['tr'] == 'VI']['count_users'].values[0],payment_network[payment_network['tr'] == 'MC']['count_users'].values[0],payment_network[payment_network['tr'] == 'AX']['count_users'].values[0]]
        card_iss_split = card_issuer.head(20).values.tolist()
        act_card_split = [factors[0], active_card.iat[0,0], net_change[6] - active_card.iat[0,0]]
        
        for iss in card_iss_split[1:]:
            iss[1] = iss[1].encode('utf-8')
            
        
#        write to files
        with open('net_change.csv','ab') as fp:
            writer = csv.writer(fp,delimiter = ',')
            writer.writerow(net_change)
                    
        with open('days_dist.csv','ab') as fp:
            writer = csv.writer(fp,delimiter = ',')
            writer.writerow(days_dist)
            
        with open('pay_net_split.csv','ab') as fp:
            writer = csv.writer(fp,delimiter = ',')
            writer.writerow(pay_net_split)

        with open('card_iss_split.csv','ab') as fp:
            writer = csv.writer(fp,delimiter = ',')
            writer.writerows(card_iss_split)

        with open('act_card_split.csv','ab') as fp:
            writer = csv.writer(fp,delimiter = ',')
            writer.writerow(act_card_split)
            
