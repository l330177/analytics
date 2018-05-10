# -*- coding: utf-8 -*-
"""
Created on Tue Feb 14 11:39:28 2017

@author: a.seshadri
"""

import pandas as pd
from sqlalchemy import create_engine
import datetime

#Engine to connect to redshift
red_engine = create_engine('redshift+psycopg2://user:bust@localhost:45438/analytics')

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
    
    return [(date[:10], len(net_cru_start.index), len(net_cru_end.index), net_cru_add, new_net_cru_add, return_net_cru_add, net_cru_delete, net_change)], net_cru_deleted_users

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
# To retrieve data for a specific date. The data retrieved will be for a day before the end_date. Uncomment this line for backfilling.	
#    end_date = datetime.datetime(2017,4,23,0,0,0)			#Change this date to required date
#    start_date = end_date + datetime.timedelta(days=-1)    

# To retrieve data for a specific date range. The data retrieved will be for a day before the end_date. Uncomment this line for backfilling.	
#    end_date = datetime.datetime(2017,5,2,0,0,0)		#Change this date to required end date.
#    start_date = datetime.datetime(2017,4,23,0,0,0)		#Change this date to required start date


# To retrive the data for the previous day. Comment next 2 lines if using specific date for backfilling.
    end_date = datetime.datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond=0)
    start_date = end_date + datetime.timedelta(days=-1)
    
#	Always loops from start_date to end_date - 1

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
        days_dist = [(profile[0],sum(days[days['num_days'] <= 30]['count'].values),sum(days[(days['num_days']<=60) & (days['num_days'] > 30)]['count'].values),sum(days[(days['num_days']<=90) & (days['num_days'] > 60)]['count'].values),sum(days[days['num_days']>90]['count'].values))]
        
        payment_network = factors[1]
        card_issuer = factors[2]
        card_issuer.insert(0,'date',factors[0])
        active_card = factors[3]
        pay_net_split = [(factors[0], payment_network[payment_network['tr'] == 'VI']['count_users'].values[0],payment_network[payment_network['tr'] == 'MC']['count_users'].values[0],payment_network[payment_network['tr'] == 'AX']['count_users'].values[0])]
        card_iss_split = card_issuer.head(20).values.tolist()        
        act_card_split = [(factors[0], active_card.iat[0,0], net_change[0][6] - active_card.iat[0,0])]
        
        for iss in card_iss_split:
            iss[1] = iss[1].encode('utf-8')
        
        card_iss_split = [tuple(iss) for iss in card_iss_split]
        
        #update existing data
        existing_netchange = pd.read_csv('net_change.csv',header = 0)
        new_netchange = pd.DataFrame.from_records(net_change,columns = existing_netchange.columns)
        updated_netchange = existing_netchange.loc[existing_netchange['Date'] != new_netchange.Date.unique().tolist()[0]]
        updated_netchange = updated_netchange.append(new_netchange,ignore_index = True)
        updated_netchange.sort_values(by='Date',inplace=True)        
        
        existing_daysdist = pd.read_csv('days_dist.csv',header = 0)
        new_daysdist = pd.DataFrame.from_records(days_dist,columns = existing_daysdist.columns)
        updated_daysdist = existing_daysdist.loc[existing_daysdist['Date'] != new_daysdist.Date.unique().tolist()[0]]
        updated_daysdist = updated_daysdist.append(new_daysdist,ignore_index = True)
        updated_daysdist.sort_values(by='Date',inplace=True)        

        existing_paynetsplit = pd.read_csv('pay_net_split.csv',header = 0)
        new_paynetsplit = pd.DataFrame.from_records(pay_net_split,columns = existing_paynetsplit.columns)
        updated_paynetsplit = existing_paynetsplit.loc[existing_paynetsplit['Date'] != new_paynetsplit.Date.unique().tolist()[0]]
        updated_paynetsplit = updated_paynetsplit.append(new_paynetsplit,ignore_index = True)
        updated_paynetsplit.sort_values(by='Date',inplace=True)
        
        existing_cardisssplit = pd.read_csv('card_iss_split.csv',header = 0)
        new_cardisssplit = pd.DataFrame.from_records(card_iss_split,columns = existing_cardisssplit.columns)
        updated_cardisssplit = existing_cardisssplit.loc[existing_cardisssplit['Date'] != new_cardisssplit.Date.unique().tolist()[0]]
        updated_cardisssplit = updated_cardisssplit.append(new_cardisssplit,ignore_index = True)
        updated_cardisssplit.sort_values(by='Date',inplace=True)  
        
        existing_actcardsplit = pd.read_csv('act_card_split.csv',header = 0)
        new_actcardsplit = pd.DataFrame.from_records(act_card_split,columns = existing_actcardsplit.columns)
        updated_actcardsplit = existing_actcardsplit.loc[existing_actcardsplit['Date'] != new_actcardsplit.Date.unique().tolist()[0]]
        updated_actcardsplit = updated_actcardsplit.append(new_actcardsplit,ignore_index = True)
        updated_actcardsplit.sort_values(by='Date',inplace=True)  
        
#        write to files
        updated_netchange.to_csv('net_change.csv',index = False)        
        updated_daysdist.to_csv('days_dist.csv',index = False)
        updated_paynetsplit.to_csv('pay_net_split.csv',index = False)
        updated_cardisssplit.to_csv('card_iss_split.csv',index = False)
        updated_actcardsplit.to_csv('act_card_split.csv',index = False)
            
