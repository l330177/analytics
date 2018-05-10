# -*- coding: utf-8 -*-
"""
Created on Mon Oct 31 10:54:12 2016

"""

import pandas as pd
from sqlalchemy import create_engine
import csv
import datetime

#Engine to connect to redshift
red_engine = create_engine('redshift+psycopg2://admin:hahahaa@redshift-cluster.czlcstchuzge.us-east-1.redshift.amazonaws.com:45439/analytics')

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
    
    card_dist_query = """
        select num_cards, count(distinct trsdimuserid)
        from
        (select trsdimuserid, count(distinct tokenid) as num_cards
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + "' and trsdimuserid in " + str(users) + """
        group by trsdimuserid) 
        group by num_cards;"""
        
    txn_dist_query = """
        select num_txn, count(distinct trsdimuserid)
        from
        (select trsdimuserid, count(*) as num_txn
        from trs_fact_ws_notification w inner join trs_dim_device m on w.trsdimdeviceid = m.id
        where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null) and m.countryCode = 'US' and convert_timezone('UTC', 'EST', w.createdat) < '""" + date + "' and trsdimuserid in " + str(users) + """  
        group by trsdimuserid)
        group by num_txn;"""
        
    time_dist_query = """
        select min_date, count(distinct trsdimuserid)
        from
        (select trsdimuserid, trunc(min(t.createdat)) as min_date
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + "' and trsdimuserid in " + str(users) + """
        group by trsdimuserid) 
        group by min_date;"""
        
    card_dist = pd.read_sql_query(card_dist_query, red_engine)
    txn_dist = pd.read_sql_query(txn_dist_query, red_engine)
    time_dist = pd.read_sql_query(time_dist_query,red_engine)
    
    return [date[:10], card_dist, txn_dist, time_dist]
    
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

    loss_credit_debit_query = """    
        select cardtype, count(distinct trsdimuserid) as count_users
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id inner join trs_dim_card_product p on t.trsdimcardproductid = p.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countrycode = 'US' and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + "' and trsdimuserid in " + str(users) + """
        group by cardtype
        order by count_users desc; """

    loss_device_model_query = """
        select friendlyname, count(distinct trsdimuserid) as count_users
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id inner join trs_dim_device_model d on t.trsdimdevicemodelid = d.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countrycode = 'US' and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + "' and trsdimuserid in " + str(users) + """
        group by friendlyname
        order by friendlyname; """

    loss_active_card_query = """
        select count(distinct trsdimuserid) as count_users
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id inner join trs_dim_card_issuer i on t.trsdimcardissuerid = i.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and status = 'ACTIVE' and m.countrycode = 'US' and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + "' and trsdimuserid in " + str(users) + ";"
    
    loss_payment_network = pd.read_sql_query(loss_payment_network_query, red_engine)
    loss_card_issuer = pd.read_sql_query(loss_card_issuer_query, red_engine)
    loss_credit_debit = pd.read_sql_query(loss_credit_debit_query, red_engine)
    loss_device_model = pd.read_sql_query(loss_device_model_query, red_engine)
    loss_active_card = pd.read_sql_query(loss_active_card_query, red_engine)
    
    return [date[:10], loss_payment_network, loss_card_issuer, loss_credit_debit, loss_device_model, loss_active_card]
    
    
if __name__ == "__main__":
    end_date = datetime.datetime.now().replace(hour=0,minute=0,second=0,microsecond=0)    
    start_date = end_date + datetime.timedelta(days = -1)
    
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
        cards = profile[1]
        txn = profile[2]
        days = profile[3]
        del_date = dt.date()
        days['num_days'] = days.apply(lambda row: (del_date - row['min_date']).days, axis = 1)
        card_dist = [profile[0],cards[cards['num_cards'] == 1]['count'].values[0],sum(cards[(cards['num_cards']<=5) & (cards['num_cards'] > 1)]['count'].values),sum(cards[(cards['num_cards']<=10) & (cards['num_cards'] > 5)]['count'].values),sum(cards[cards['num_cards']>10]['count'].values)]
        txn_dist = [profile[0],sum(txn[(txn['num_txn']<=5) & (txn['num_txn'] > 0)]['count'].values),sum(txn[(txn['num_txn']<=10) & (txn['num_txn'] > 5)]['count'].values),sum(txn[txn['num_txn']>10]['count'].values)]
        txn_dist.insert(1,net_change[6]-(txn_dist[1]+txn_dist[2]+txn_dist[3]))
        days_dist = [profile[0],sum(days[days['num_days'] <= 30]['count'].values),sum(days[(days['num_days']<=60) & (days['num_days'] > 30)]['count'].values),sum(days[(days['num_days']<=90) & (days['num_days'] > 60)]['count'].values),sum(days[days['num_days']>90]['count'].values)]
        
        payment_network = factors[1]
        card_issuer = factors[2]
        credit_debit = factors[3]
        device_model = factors[4]
        active_card = factors[5]
        pay_net_split = [factors[0], payment_network[payment_network['tr'] == 'VI']['count_users'].values[0],payment_network[payment_network['tr'] == 'MC']['count_users'].values[0],payment_network[payment_network['tr'] == 'AX']['count_users'].values[0]]
        card_iss_split = [factors[0]] + card_issuer.head(20).values.tolist()
        cred_deb_split = [factors[0], credit_debit[credit_debit['cardtype'] == 'CREDIT']['count_users'].values[0], credit_debit[credit_debit['cardtype'] == 'DEBIT']['count_users'].values[0]]
        dev_mod_split = [factors[0]] + device_model.values.tolist()
        act_card_split = [factors[0], active_card.iat[0,0], net_change[6] - active_card.iat[0,0]]
        
        for iss in card_iss_split[1:]:
            iss[0] = iss[0].encode('utf-8')
            
        
#        write to files
        with open('net_change.csv','ab') as fp:
            writer = csv.writer(fp,delimiter = ',')
            writer.writerow(net_change)
            
        with open('card_dist.csv','ab') as fp:
            writer = csv.writer(fp,delimiter = ',')
            writer.writerow(card_dist)
            
        with open('txn_dist.csv','ab') as fp:
            writer = csv.writer(fp,delimiter = ',')
            writer.writerow(txn_dist)
        
        with open('days_dist.csv','ab') as fp:
            writer = csv.writer(fp,delimiter = ',')
            writer.writerow(days_dist)
            
        with open('pay_net_split.csv','ab') as fp:
            writer = csv.writer(fp,delimiter = ',')
            writer.writerow(pay_net_split)

        with open('card_iss_split.csv','ab') as fp:
            writer = csv.writer(fp,delimiter = ',')
            writer.writerows(card_iss_split)

        with open('cred_deb_split.csv','ab') as fp:
            writer = csv.writer(fp,delimiter = ',')
            writer.writerow(cred_deb_split)

        with open('dev_mod_split.csv','ab') as fp:
            writer = csv.writer(fp,delimiter = ',')
            writer.writerows(dev_mod_split)

        with open('act_card_split.csv','ab') as fp:
            writer = csv.writer(fp,delimiter = ',')
            writer.writerow(act_card_split)
            
