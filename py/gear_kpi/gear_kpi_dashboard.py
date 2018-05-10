# -*- coding: utf-8 -*-
"""
Created on Tue Feb 14 14:47:17 2017

@author: a.seshadri
"""

import pandas as pd
from sqlalchemy import create_engine
import csv
import datetime

red_engine = create_engine('redshift+psycopg2://a.seshadri:9wmRCMHYSG@localhost:45438/trsanalytics')

def gear_kpi(dt):
    fmt = '%Y-%m-%d %H:%M:%S'
    date = dt.strftime(fmt)
    
    ru_cum_query = """
        select count(distinct usr.master_id)
        from ws.tcmn_usr as usr inner join ws.tcmn_usr_dvc as usrdvc on usr.master_id = usrdvc.master_id
        where upper(cntry_2_cd) = 'US' and dvc_name in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC','EST',usrdvc.reg_dt) < '""" + date + "';"

    ru_s2_query = """ 
        select count(distinct usr.master_id)
        from ws.tcmn_usr as usr inner join ws.tcmn_usr_dvc as usrdvc on usr.master_id = usrdvc.master_id
        where upper(cntry_2_cd) = 'US' and dvc_name in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V') 
        and convert_timezone('UTC','EST',usrdvc.reg_dt) < '""" + date + "';"
        
    ru_s2_bt_query = """
        select count(distinct usr.master_id)
        from ws.tcmn_usr as usr inner join ws.tcmn_usr_dvc as usrdvc on usr.master_id = usrdvc.master_id
        where upper(cntry_2_cd) = 'US' and dvc_name in ('SM-R720','SM-R732') and convert_timezone('UTC','EST',usrdvc.reg_dt) < '""" + date + "';"

    ru_s2_3g_query = """
        select count(distinct usr.master_id)
        from ws.tcmn_usr as usr inner join ws.tcmn_usr_dvc as usrdvc on usr.master_id = usrdvc.master_id
        where upper(cntry_2_cd) = 'US' and dvc_name in ('SM-R730A','SM-R730T','SM-R730V','SM-R735A','SM-R735T','SM-R735V') 
        and convert_timezone('UTC','EST',usrdvc.reg_dt) < '""" + date + "';"
        
    ru_s3_query = """
        select count(distinct usr.master_id)
        from ws.tcmn_usr as usr inner join ws.tcmn_usr_dvc as usrdvc on usr.master_id = usrdvc.master_id
        where upper(cntry_2_cd) = 'US' and dvc_name in ('SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC','EST',usrdvc.reg_dt) < '""" + date + "';"
        
    gross_cru_cum_query = """
        select count(distinct trsdimuserid)
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where t.tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' 
        and trsdimdevicemodelid in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + "';"

    gross_cru_s2_query = """
        select count(distinct trsdimuserid)
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where t.tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' 
        and trsdimdevicemodelid in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V') 
        and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + "';"

    gross_cru_s2_bt_query = """
        select count(distinct trsdimuserid)
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where t.tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' 
        and trsdimdevicemodelid in ('SM-R720','SM-R732') and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + "';"

    gross_cru_s2_3g_query = """
        select count(distinct trsdimuserid)
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where t.tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' 
        and trsdimdevicemodelid in ('SM-R730A','SM-R730T','SM-R730V','SM-R735A','SM-R735T','SM-R735V') 
        and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + "';"

    gross_cru_s3_query = """
        select count(distinct trsdimuserid)
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where t.tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' 
        and trsdimdevicemodelid in ('SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + "';"
        
    net_cru_cum_query = """
        select count(distinct trsdimuserid)
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' 
        and trsdimdevicemodelid in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and status != 'DISPOSED' and t.createdat = (select max(t2.createdat) from trs_fact_token_history t2 where t2.tokenid = t.tokenid and convert_timezone('UTC', 'EST', t2.createdat) < '""" + date + "');"

    net_cru_s2_query = """    
        select count(distinct trsdimuserid)
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' 
        and trsdimdevicemodelid in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V') 
        and status != 'DISPOSED' and t.createdat = (select max(t2.createdat) from trs_fact_token_history t2 where t2.tokenid = t.tokenid and convert_timezone('UTC', 'EST', t2.createdat) < '""" + date + "');"

    net_cru_s2_bt_query = """    
        select count(distinct trsdimuserid)
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' 
        and trsdimdevicemodelid in ('SM-R720','SM-R732') and status != 'DISPOSED' 
        and t.createdat = (select max(t2.createdat) from trs_fact_token_history t2 where t2.tokenid = t.tokenid and convert_timezone('UTC', 'EST', t2.createdat) < '""" + date + "');"

    net_cru_s2_3g_query = """    
        select count(distinct trsdimuserid)
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' 
        and trsdimdevicemodelid in ('SM-R730A','SM-R730T','SM-R730V','SM-R735A','SM-R735T','SM-R735V') and status != 'DISPOSED' 
        and t.createdat = (select max(t2.createdat) from trs_fact_token_history t2 where t2.tokenid = t.tokenid and convert_timezone('UTC', 'EST', t2.createdat) < '""" + date + "');"

    net_cru_s3_query = """
        select count(distinct trsdimuserid)
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' 
        and trsdimdevicemodelid in ('SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and status != 'DISPOSED' and t.createdat = (select max(t2.createdat) from trs_fact_token_history t2 where t2.tokenid = t.tokenid and convert_timezone('UTC', 'EST', t2.createdat) < '""" + date + "');"

    mau_query = """
        select count(distinct trsdimuserid)
        from trs_fact_ws_notification w inner join trs_dim_device m on w.trsdimdeviceid = m.id
        where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null) 
        and trsdimdevicemodelid in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC', 'EST', w.createdat) >= ('""" + date + "'::timestamp - interval '30 days') and convert_timezone('UTC', 'EST', w.createdat) < '" + date + "' and m.countryCode ='US';"

    mau_bt_query = """
        select count(distinct trsdimuserid)
        from trs_fact_ws_notification w inner join trs_dim_device m on w.trsdimdeviceid = m.id
        where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null) 
        and trsdimdevicemodelid in ('SM-R720','SM-R732') and convert_timezone('UTC', 'EST', w.createdat) >= ('""" + date + """'::timestamp - interval '30 days') 
        and convert_timezone('UTC', 'EST', w.createdat) < '""" + date + "' and m.countryCode ='US';"

    mau_3g_query = """
        select count(distinct trsdimuserid)
        from trs_fact_ws_notification w inner join trs_dim_device m on w.trsdimdeviceid = m.id
        where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null) 
        and trsdimdevicemodelid in ('SM-R730A','SM-R730T','SM-R730V','SM-R735A','SM-R735T','SM-R735V') and convert_timezone('UTC', 'EST', w.createdat) >= ('""" + date + """'::timestamp - interval '30 days') 
        and convert_timezone('UTC', 'EST', w.createdat) < '""" + date + "' and m.countryCode ='US';"

    mau_s3_query = """
        select count(distinct trsdimuserid)
        from trs_fact_ws_notification w inner join trs_dim_device m on w.trsdimdeviceid = m.id
        where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null) 
        and trsdimdevicemodelid in ('SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') and convert_timezone('UTC', 'EST', w.createdat) >= ('""" + date + """'::timestamp - interval '30 days') 
        and convert_timezone('UTC', 'EST', w.createdat) < '""" + date + "' and m.countryCode ='US';"
        
    wau_query = """
        select count(distinct trsdimuserid)
        from trs_fact_ws_notification w inner join trs_dim_device m on w.trsdimdeviceid = m.id
        where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null) 
        and trsdimdevicemodelid in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC', 'EST', w.createdat) >= ('""" + date + "'::timestamp - interval '7 days') and convert_timezone('UTC', 'EST', w.createdat) < '" + date + "' and m.countryCode ='US';"

    dau_query = """
        select count(distinct trsdimuserid)
        from trs_fact_ws_notification w inner join trs_dim_device m on w.trsdimdeviceid = m.id
        where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null) 
        and trsdimdevicemodelid in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC', 'EST', w.createdat) >= ('""" + date + "'::timestamp - interval '1 day') and convert_timezone('UTC', 'EST', w.createdat) < '" + date + "' and m.countryCode ='US';"

    txn_query = """
        select count(w.id)
        from trs_fact_ws_notification w inner join trs_dim_device m on w.trsdimdeviceid = m.id
        where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null) 
        and trsdimdevicemodelid in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC', 'EST', w.createdat) >= ('""" + date + "'::timestamp - interval '1 day') and convert_timezone('UTC', 'EST', w.createdat) < '""" + date + "' and m.countryCode ='US';"
        

    ru_cum = pd.read_sql_query(ru_cum_query,red_engine)
    ru_s2 = pd.read_sql_query(ru_s2_query,red_engine)
    ru_s2_bt = pd.read_sql_query(ru_s2_bt_query,red_engine)
    ru_s2_3g = pd.read_sql_query(ru_s2_3g_query,red_engine)
    ru_s3 = pd.read_sql_query(ru_s3_query,red_engine)
    
    gross_cru_cum = pd.read_sql_query(gross_cru_cum_query,red_engine)
    gross_cru_s2 = pd.read_sql_query(gross_cru_s2_query,red_engine)
    gross_cru_s2_bt = pd.read_sql_query(gross_cru_s2_bt_query,red_engine)
    gross_cru_s2_3g = pd.read_sql_query(gross_cru_s2_3g_query,red_engine)
    gross_cru_s3 = pd.read_sql_query(gross_cru_s3_query,red_engine)

    net_cru_cum = pd.read_sql_query(net_cru_cum_query,red_engine)
    net_cru_s2 = pd.read_sql_query(net_cru_s2_query,red_engine)
    net_cru_s2_bt = pd.read_sql_query(net_cru_s2_bt_query,red_engine)
    net_cru_s2_3g = pd.read_sql_query(net_cru_s2_3g_query,red_engine)    
    net_cru_s3 = pd.read_sql_query(net_cru_s3_query,red_engine)

    mau = pd.read_sql_query(mau_query,red_engine)
    mau_bt = pd.read_sql_query(mau_bt_query,red_engine)    
    mau_3g = pd.read_sql_query(mau_3g_query,red_engine)        
    mau_s3 = pd.read_sql_query(mau_s3_query,red_engine)
    
    wau = pd.read_sql_query(wau_query,red_engine)
    dau = pd.read_sql_query(dau_query,red_engine)
    txn = pd.read_sql_query(txn_query,red_engine)
    
    dat = (dt + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
    return [dat, ru_cum.iat[0,0], ru_s2.iat[0,0], ru_s2_bt.iat[0,0], ru_s2_3g.iat[0,0], ru_s3.iat[0,0], gross_cru_cum.iat[0,0], gross_cru_s2.iat[0,0], gross_cru_s2_bt.iat[0,0], gross_cru_s2_3g.iat[0,0], gross_cru_s3.iat[0,0], net_cru_cum.iat[0,0], net_cru_s2.iat[0,0], net_cru_s2_bt.iat[0,0], net_cru_s2_3g.iat[0,0], net_cru_s3.iat[0,0]], [mau.iat[0,0], mau_bt.iat[0,0], mau_3g.iat[0,0], mau_s3.iat[0,0], wau.iat[0,0], dau.iat[0,0], txn.iat[0,0]]

def daily_gear_kpi(dt):
    fmt = '%Y-%m-%d %H:%M:%S'
    date = dt.strftime(fmt)
    ru_daily_query = """
        select count(*) from
        (select distinct usr.master_id
        from ws.tcmn_usr as usr inner join ws.tcmn_usr_dvc as usrdvc on usr.master_id = usrdvc.master_id
        where upper(cntry_2_cd) = 'US' and dvc_name in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC','EST',usrdvc.reg_dt) < '""" + date + """'
        except
        select distinct usr.master_id
        from ws.tcmn_usr as usr inner join ws.tcmn_usr_dvc as usrdvc on usr.master_id = usrdvc.master_id
        where upper(cntry_2_cd) = 'US' and dvc_name in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC','EST',usrdvc.reg_dt) < ('""" + date + "'::timestamp - interval '1 day'));"

    gross_cru_cum_query = """
        select distinct trsdimuserid
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where t.tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' 
        and trsdimdevicemodelid in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + "';"
        
    gross_cru_cum_prev_query = """
        select distinct trsdimuserid
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where t.tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' 
        and trsdimdevicemodelid in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC', 'EST', t.createdat) < ('""" + date + "'::timestamp - interval '1 day');"
        
    net_cru_cum_query = """
        select distinct trsdimuserid
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' 
        and trsdimdevicemodelid in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') and status != 'DISPOSED' 
        and t.createdat = (select max(t2.createdat) from trs_fact_token_history t2 where t2.tokenid = t.tokenid and convert_timezone('UTC', 'EST', t2.createdat) < '""" + date + "');"
        
    active_user_daily_query = """
        select count(*) from
        (select distinct trsdimuserid
        from trs_fact_ws_notification w inner join trs_dim_device m on w.trsdimdeviceid = m.id
        where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null)
        and trsdimdevicemodelid in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC', 'EST', w.createdat) < '""" + date + """'
        except
        select distinct trsdimuserid
        from trs_fact_ws_notification w inner join trs_dim_device m on w.trsdimdeviceid = m.id
        where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null)
        and trsdimdevicemodelid in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC', 'EST', w.createdat) < ('""" + date + "'::timestamp - interval '1 day'));"    
        
    reg_cards_daily_query = """
        select count(*) from
        (select distinct tokenid
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where t.tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' 
        and trsdimdevicemodelid in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + """'
        except
        select distinct tokenid
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where t.tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' 
        and trsdimdevicemodelid in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC', 'EST', t.createdat) < ('""" + date + "'::timestamp - interval '1 day'));"
        
    active_cards_daily_query = """
        select count(*) from
        (select distinct tokenid
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where t.tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' and status = 'ACTIVE'
        and trsdimdevicemodelid in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC', 'EST', t.createdat) < '""" + date + """'
        except
        select distinct tokenid
        from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
        where t.tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' and status = 'ACTIVE'
        and trsdimdevicemodelid in ('SM-R720','SM-R730A','SM-R730T','SM-R730V','SM-R732','SM-R735A','SM-R735T','SM-R735V','SM-R760','SM-R765A','SM-R765IFA','SM-R765T','SM-R765V','SM-R770','SM-R770IFA') 
        and convert_timezone('UTC', 'EST', t.createdat) < ('""" + date + "'::timestamp - interval '1 day'));"
    
    ru_daily = pd.read_sql_query(ru_daily_query,red_engine)
    gross_cru_cum = pd.read_sql_query(gross_cru_cum_query,red_engine)            
    gross_cru_cum_prev = pd.read_sql_query(gross_cru_cum_prev_query,red_engine)
    net_cru_cum = pd.read_sql_query(net_cru_cum_query,red_engine)
    active_user_daily = pd.read_sql_query(active_user_daily_query,red_engine)
    reg_cards_daily = pd.read_sql_query(reg_cards_daily_query,red_engine)
    active_cards_daily = pd.read_sql_query(active_cards_daily_query,red_engine)    
    
    new_gross = set(gross_cru_cum.trsdimuserid).difference(set(gross_cru_cum_prev.trsdimuserid))
    new_net = new_gross.intersection(set(net_cru_cum.trsdimuserid))

    
    return [ru_daily.iat[0,0], len(new_gross), len(new_net), active_user_daily.iat[0,0]], [reg_cards_daily.iat[0,0],active_cards_daily.iat[0,0]]

if __name__ == "__main__":
    gear_numbers = []
#    dt =  datetime.datetime(2017,2,14,0,0,0)
    dt = datetime.datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond=0)  
    cumulative1, cumulative2 = gear_kpi(dt)
    daily1, daily2 = daily_gear_kpi(dt)
    gear_numbers.append(cumulative1+daily1+cumulative2+daily2)

            
    with open('Gear KPI.csv','ab') as fp:
        wr = csv.writer(fp,delimiter=',')
        wr.writerows(gear_numbers)        
            