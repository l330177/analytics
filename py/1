import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import time
red_engine = create_engine('redshift+psycopg2://admin:woohoo@redshift-cluster.czlcstchuzge.us-east-1.redshift.amazonaws.com:45439/analytics')
promo_engine = create_engine('mysql://dangon:xxwellnow~N@db3-mys-us1a-prd.internal:43306/prime')
SMP_engine = create_engine('mysql://samol:giveit~N@db3-mys-us1a-prd.internal:43306/smaller')
eDate = time.strftime("%Y-%m-%d")
#Change the pahts
PATH = 'C:\QlikDataFiles\Verification'
output_path = 'C:\QlikDataFiles\out\{}redshiftData.csv'.format(eDate)
#%%
ruQ = """
SELECT COUNT(distinct usr.MASTER_ID) 
FROM ws.tcmn_usr AS usr LEFT JOIN ws.tcmn_usr_dvc AS usrdvc ON usr.master_id = usrdvc.master_id 
WHERE upper(usrdvc.cntry_2_cd) = 'US' AND usr.join_dt < 'END_DATE 05:00:00';
"""


gcruQ = """
select count(*)
from
(select trsdimuserid
from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
where t.tokenid is not null and m.countryCode = 'US' and convert_timezone('UTC', 'EST', t.createdat) < 'END_DATE'
union
select userid
from spigc.usercard
where convert_timezone('UTC', 'EST', created) < 'END_DATE');
"""

ncruQ = """
select count(*)
from
(select trsdimuserid
from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
where tokenid is not null and m.countryCode = 'US' and status != 'DISPOSED' and t.createdat = (select max(createdat) from trs_fact_token_history t2 where t2.tokenid = t.tokenid and convert_timezone('UTC', 'EST', t2.createdat) < 'END_DATE')
union
select p.userid
from spigc.txnlog t right join spigc.purchase p on t.txnid = p.id
where (t.txntype = 'PURCHASE' or t.txntype = 'PURCHASE_FOR_OTHERS') and t.txnapiname = 'CREATE_ACCOUNT' and t.response = 'SUCCESS' and convert_timezone('UTC', 'EST', t.created) >= '2015-10-28 00:00:00' and convert_timezone('UTC', 'EST', t.created) < 'END_DATE'
union
select l.userid
from spigc.txnlog t right join spigc.loadexisting2 l on t.txnid = l.id
where t.txntype = 'LOAD_EXISTING' and t.txnapiname = 'VERIFY_ACCOUNT' and t.response = 'SUCCESS' and convert_timezone('UTC', 'EST', t.created) >= '2015-10-28 00:00:00' and convert_timezone('UTC', 'EST', t.created) < 'END_DATE'
union
select r.userid
from spigc.txnlog t right join spigc.received r on t.giftcardid = r.giftcardid
where (t.txntype = 'BATCH_PROMOTION' or txntype = 'HERO_PROMOTION') and t.txnapiname = 'CREATE_ACCOUNT' and t.response = 'SUCCESS' and convert_timezone('UTC', 'EST', t.created) >= '2015-10-28 00:00:00' and convert_timezone('UTC', 'EST', t.created) < 'END_DATE');
"""

gmauQ = """
select count(*) from
(
/*CD MAU*/
select distinct trsdimuserid
from trs_fact_ws_notification w inner join trs_dim_device m on w.trsdimdeviceid = m.id  
where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null) 
     and convert_timezone('UTC', 'EST', w.createdat) >= ('END_DATE 00:00:00'::timestamp - interval '30 days')
     and convert_timezone('UTC', 'EST', w.createdat) < 'END_DATE 00:00:00'
     and m.countryCode ='US'
union
/* traditional membership and gc MAU*/
select distinct trsdimuserid
from trs_fact_txn_postattempt p inner join trs_dim_device m on p.trsdimdeviceid = m.id   
where convert_timezone('UTC', 'EST', p.createdat) >= ('END_DATE 00:00:00'::timestamp - interval '30 days')
     and convert_timezone('UTC', 'EST', p.createdat) < 'END_DATE 00:00:00'
     and p.tokenid is not null and m.countryCode ='US' and (p.tr = 'LC' or p.tokenid = 'GIFT')
union
select distinct trsdimuserid
from trs_fact_app_event e inner join trs_dim_device m on e.trsdimdeviceid = m.id
where (
     (eventcategory = 'activity_launch' and fieldkey = 'activity' and fieldvalue in ('screen_us_deals_simplepay_screen', 'screen_us_deals_detailed')) /* deal MAU via simple pay or view detail page*/
     or (eventcategory = 'deal_redeem_attempt' and fieldkey = 'redeemed_from') /*deal MAU who clicked redeem button*/
     or (eventcategory in ('loyalty_view_card_details', 'loyalty_launch_simple_pay'))  /*membership card detail page view*/
     or (eventcategory = 'reward_redeem' and fieldkey = 'action' and fieldValue = 'reward_attempted')  /*reward redemption*/
     or (eventcategory = 'transaction_attempt' and fieldkey='card_type' and fieldvalue in ('membership_card','card_category_gift_card')) /*membership and gc attempt from event app table*/
     )
     and m.countrycode = 'US'
     and convert_timezone('UTC', 'EST', e.createdat) >= ('END_DATE 00:00:00'::timestamp - interval '30 days')
     and convert_timezone('UTC', 'EST', e.createdat) < 'END_DATE 00:00:00'
union
select distinct trsdimuserid 
from trs_fact_app_user_event e inner join trs_dim_device m on e.trsdimdeviceid = m.id 
where convert_timezone('UTC', 'EST', e.createdat) >= ('END_DATE 00:00:00'::timestamp - interval '30 days')
    and convert_timezone('UTC', 'EST', e.createdat) < 'END_DATE 00:00:00' 
    and m.countrycode = 'US' 
    and eventcategory = 'activity_launch' 
    and fieldkey in ('activity', 'screen_id') 
    and fieldvalue in ('screen_us_deals_simplepay_screen', 'screen_us_deals_detailed') /* deal MAU via simple pay and view deal detail page*/
);
"""
gwauQ = """
select count(*) from
(
/*CD WAU*/
select distinct trsdimuserid
from trs_fact_ws_notification w inner join trs_dim_device m on w.trsdimdeviceid = m.id  
where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null) 
     and convert_timezone('UTC', 'EST', w.createdat) >= ('END_DATE 00:00:00'::timestamp - interval '7 days')
     and convert_timezone('UTC', 'EST', w.createdat) < 'END_DATE 00:00:00'
     and m.countryCode ='US'
union
/* traditional membership and gc WAU*/
select distinct trsdimuserid
from trs_fact_txn_postattempt p inner join trs_dim_device m on p.trsdimdeviceid = m.id   
where convert_timezone('UTC', 'EST', p.createdat) >= ('END_DATE 00:00:00'::timestamp - interval '7 days')
     and convert_timezone('UTC', 'EST', p.createdat) < 'END_DATE 00:00:00'
     and p.tokenid is not null and m.countryCode ='US' and (p.tr = 'LC' or p.tokenid = 'GIFT')
union
select distinct trsdimuserid
from trs_fact_app_event e inner join trs_dim_device m on e.trsdimdeviceid = m.id
where (
     (eventcategory = 'activity_launch' and fieldkey = 'activity' and fieldvalue in ('screen_us_deals_simplepay_screen', 'screen_us_deals_detailed')) /* deal WAU via simple pay or view detail page*/
     or (eventcategory = 'deal_redeem_attempt' and fieldkey = 'redeemed_from') /*deal WAU who clicked redeem button*/
     or (eventcategory in ('loyalty_view_card_details', 'loyalty_launch_simple_pay'))  /*membership card detail page view*/
     or (eventcategory = 'reward_redeem' and fieldkey = 'action' and fieldValue = 'reward_attempted')  /*reward redemption*/
     or (eventcategory = 'transaction_attempt' and fieldkey='card_type' and fieldvalue in ('membership_card','card_category_gift_card')) /*membership and gc attempt from event app table*/
     )
     and m.countrycode = 'US'
     and convert_timezone('UTC', 'EST', e.createdat) >= ('END_DATE 00:00:00'::timestamp - interval '7 days')
     and convert_timezone('UTC', 'EST', e.createdat) < 'END_DATE 00:00:00'
union
select distinct trsdimuserid 
from trs_fact_app_user_event e inner join trs_dim_device m on e.trsdimdeviceid = m.id 
where convert_timezone('UTC', 'EST', e.createdat) >= ('END_DATE 00:00:00'::timestamp - interval '7 days')
    and convert_timezone('UTC', 'EST', e.createdat) < 'END_DATE 00:00:00' 
    and m.countrycode = 'US' 
    and eventcategory = 'activity_launch' 
    and fieldkey in ('activity', 'screen_id') 
    and fieldvalue in ('screen_us_deals_simplepay_screen', 'screen_us_deals_detailed') /* deal WAU via simple pay and view deal detail page*/
);
"""

gdauQ = """
select count(*) from
(
/*CD DAU*/
select distinct trsdimuserid
from trs_fact_ws_notification w inner join trs_dim_device m on w.trsdimdeviceid = m.id  
where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null) 
     and convert_timezone('UTC', 'EST', w.createdat) >= ('END_DATE 00:00:00'::timestamp - interval '1 days')
     and convert_timezone('UTC', 'EST', w.createdat) < 'END_DATE 00:00:00'
     and m.countryCode ='US'
union
/* traditional membership and gc DAU*/
select distinct trsdimuserid
from trs_fact_txn_postattempt p inner join trs_dim_device m on p.trsdimdeviceid = m.id   
where convert_timezone('UTC', 'EST', p.createdat) >= ('END_DATE 00:00:00'::timestamp - interval '1 days')
     and convert_timezone('UTC', 'EST', p.createdat) < 'END_DATE 00:00:00'
     and p.tokenid is not null and m.countryCode ='US' and (p.tr = 'LC' or p.tokenid = 'GIFT')
union
select distinct trsdimuserid
from trs_fact_app_event e inner join trs_dim_device m on e.trsdimdeviceid = m.id
where (
     (eventcategory = 'activity_launch' and fieldkey = 'activity' and fieldvalue in ('screen_us_deals_simplepay_screen', 'screen_us_deals_detailed')) /* deal DAU via simple pay or view detail page*/
     or (eventcategory = 'deal_redeem_attempt' and fieldkey = 'redeemed_from') /*deal DAU who clicked redeem button*/
     or (eventcategory in ('loyalty_view_card_details', 'loyalty_launch_simple_pay'))  /*membership card detail page view*/
     or (eventcategory = 'reward_redeem' and fieldkey = 'action' and fieldValue = 'reward_attempted')  /*reward redemption*/
     or (eventcategory = 'transaction_attempt' and fieldkey='card_type' and fieldvalue in ('membership_card','card_category_gift_card')) /*membership and gc attempt from event app table*/
     )
     and m.countrycode = 'US'
     and convert_timezone('UTC', 'EST', e.createdat) >= ('END_DATE 00:00:00'::timestamp - interval '1 days')
     and convert_timezone('UTC', 'EST', e.createdat) < 'END_DATE 00:00:00'
union
select distinct trsdimuserid 
from trs_fact_app_user_event e inner join trs_dim_device m on e.trsdimdeviceid = m.id 
where convert_timezone('UTC', 'EST', e.createdat) >= ('END_DATE 00:00:00'::timestamp - interval '1 days')
    and convert_timezone('UTC', 'EST', e.createdat) < 'END_DATE 00:00:00' 
    and m.countrycode = 'US' 
    and eventcategory = 'activity_launch' 
    and fieldkey in ('activity', 'screen_id') 
    and fieldvalue in ('screen_us_deals_simplepay_screen', 'screen_us_deals_detailed') /* deal DAU via simple pay and view deal detail page*/
);
"""

ru_daily_diffQ = """
SELECT COUNT(distinct usr.master_id) 
FROM ws.tcmn_usr AS usr LEFT JOIN ws.tcmn_usr_dvc AS usrdvc ON usr.master_id = usrdvc.master_id 
WHERE upper(usrdvc.cntry_2_cd)= 'US' AND usr.join_dt >= ('END_DATE 05:00:00'::timestamp - interval '1 days') AND usr.join_dt < 'END_DATE 05:00:00';
"""

new_CRU_dailyQ = """
select count(*)
from
(
(select trsdimuserid
from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
where t.tokenid is not null and m.countryCode = 'US' and convert_timezone('UTC', 'EST', t.createdat) < 'END_DATE 00:00:00'
union
select userid
from spigc.usercard
where convert_timezone('UTC', 'EST', created) < 'END_DATE 00:00:00')
except
(select trsdimuserid
from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
where t.tokenid is not null and m.countryCode = 'US' and convert_timezone('UTC', 'EST', t.createdat) < ('END_DATE 00:00:00'::timestamp - interval '1 day') 
union
select userid
from spigc.usercard
where convert_timezone('UTC', 'EST', created) < ('END_DATE 00:00:00'::timestamp - interval '1 day'))
);
"""

net_new_cru_for_yesterdayQ = """
select count(*)
from
(
/*new daily CRU */
(
(select trsdimuserid
from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
where t.tokenid is not null and m.countryCode = 'US' and convert_timezone('UTC', 'EST', t.createdat) < 'END_DATE 00:00:00'
union
select userid
from spigc.usercard
where convert_timezone('UTC', 'EST', created) < 'END_DATE 00:00:00')
except
(select trsdimuserid
from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
where t.tokenid is not null and m.countryCode = 'US' and convert_timezone('UTC', 'EST', t.createdat) < ('END_DATE 00:00:00'::timestamp - interval '1 day') 
union
select userid
from spigc.usercard
where convert_timezone('UTC', 'EST', created) < ('END_DATE 00:00:00'::timestamp - interval '1 day')))
intersect
/*net CRU on that day*/
(select trsdimuserid
from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
where tokenid is not null and m.countryCode = 'US' and status != 'DISPOSED' and t.createdat = (select max(createdat) from trs_fact_token_history t2 where t2.tokenid = t.tokenid and convert_timezone('UTC', 'EST', t2.createdat) < 'END_DATE 00:00:00')
union
select p.userid
from spigc.txnlog t right join spigc.purchase p on t.txnid = p.id
where (t.txntype = 'PURCHASE' or t.txntype = 'PURCHASE_FOR_OTHERS') and t.txnapiname = 'CREATE_ACCOUNT' and t.response = 'SUCCESS' and convert_timezone('UTC', 'EST', t.created) >= '2015-10-28 00:00:00' and convert_timezone('UTC', 'EST', t.created) < 'END_DATE 00:00:00'
union
select l.userid
from spigc.txnlog t right join spigc.loadexisting2 l on t.txnid = l.id
where t.txntype = 'LOAD_EXISTING' and t.txnapiname = 'VERIFY_ACCOUNT' and t.response = 'SUCCESS' and convert_timezone('UTC', 'EST', t.created) >= '2015-10-28 00:00:00' and convert_timezone('UTC', 'EST', t.created) < 'END_DATE 00:00:00'
union
select r.userid
from spigc.txnlog t right join spigc.received r on t.giftcardid = r.giftcardid
where (t.txntype = 'BATCH_PROMOTION' or txntype = 'HERO_PROMOTION') and t.txnapiname = 'CREATE_ACCOUNT' and t.response = 'SUCCESS' and convert_timezone('UTC', 'EST', t.created) >= '2015-10-28 00:00:00' and convert_timezone('UTC', 'EST', t.created) < 'END_DATE 00:00:00')
);

"""

gcru_only_credit_debitQ = """
select count(distinct trsdimuserid)
from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
where t.tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' and convert_timezone('UTC', 'EST', t.createdat) < 'END_DATE';
"""

gcru_only_membershipQ = """
select count(distinct trsdimuserid)
from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
where t.tokenid is not null and trsdimpaymentnetworkid = 'LO' and m.countryCode = 'US' and convert_timezone('UTC', 'EST', t.createdat) < 'END_DATE';
"""

gcru_only_giftcardQ = """
select count(distinct userid)
from spigc.usercard
where convert_timezone('UTC', 'EST', created) < 'END_DATE';
"""

ncru_only_credit_debitQ = """
select count(distinct trsdimuserid)
from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
where tokenid is not null and trsdimpaymentnetworkid != 'LO' and m.countryCode = 'US' and status != 'DISPOSED' and t.createdat = (select max(t2.createdat) from trs_fact_token_history t2 where t2.tokenid = t.tokenid and convert_timezone('UTC', 'EST', t2.createdat) < 'END_DATE');
"""

ncru_only_membershipQ = """
select count(distinct trsdimuserid)
from trs_fact_token_history t inner join trs_dim_device m on t.trsdimdeviceid = m.id
where tokenid is not null and trsdimpaymentnetworkid = 'LO' and m.countryCode = 'US' and status != 'DISPOSED' and t.createdat = (select max(t2.createdat) from trs_fact_token_history t2 where t2.tokenid = t.tokenid and convert_timezone('UTC', 'EST', t2.createdat) < 'END_DATE');
"""

ncru_only_giftcardQ = """
select count(distinct userid)
from spigc.usercard
where convert_timezone('UTC', 'EST', created) < 'END_DATE' and deleted = 'false';
"""

gmau_only_credit_debitQ = """
select count(distinct trsdimuserid)
from trs_fact_ws_notification w inner join trs_dim_device m on w.trsdimdeviceid = m.id 
where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null)
     and convert_timezone('UTC', 'EST', w.createdat) >= ('END_DATE'::timestamp - interval '30 days')
     and convert_timezone('UTC', 'EST', w.createdat) < 'END_DATE'
     and m.countryCode ='US';
"""
gmau_only_membershipQ = """
select count(*)
from (
select distinct trsdimuserid
from trs_fact_txn_postattempt p inner join trs_dim_device m on p.trsdimdeviceid = m.id  
where convert_timezone('UTC', 'EST', p.createdat) >= ('END_DATE'::timestamp - interval '30 days')
     and convert_timezone('UTC', 'EST', p.createdat) < 'END_DATE'
     and p.tokenid is not null and m.countryCode ='US' and p.tr = 'LC'
union
select distinct trsdimuserid
from trs_fact_app_event e inner join trs_dim_device m on e.trsdimdeviceid = m.id
where (eventcategory in ('loyalty_view_card_details', 'loyalty_launch_simple_pay')
     or (eventcategory = 'transaction_attempt' and fieldkey='card_type' and fieldvalue ='membership_card'))
     and m.countrycode = 'US'
     and convert_timezone('UTC', 'EST', e.createdat) >= ('END_DATE'::timestamp - interval '30 days')
     and convert_timezone('UTC', 'EST', e.createdat) < 'END_DATE'
);
"""

gmau_only_dealsQ = """
select count(*) from(
select distinct trsdimuserid 
from trs_fact_app_event e inner join trs_dim_device m on e.trsdimdeviceid = m.id
where (
     (eventcategory = 'activity_launch' and fieldkey = 'activity' and fieldvalue in ('screen_us_deals_simplepay_screen', 'screen_us_deals_detailed')) /* deal DAU via simple pay or view detail page*/
     or (eventcategory = 'deal_redeem_attempt' and fieldkey = 'redeemed_from') /*deal MAU who clicked redeem button*/
     )
     and m.countrycode = 'US'
     and convert_timezone('UTC', 'EST', e.createdat) >= ('END_DATE 00:00:00'::timestamp - interval '30 days')
     and convert_timezone('UTC', 'EST', e.createdat) < 'END_DATE 00:00:00'
union
select distinct trsdimuserid 
from trs_fact_app_user_event e inner join trs_dim_device m on e.trsdimdeviceid = m.id 
where convert_timezone('UTC', 'EST', e.createdat) >= ('END_DATE 00:00:00'::timestamp - interval '30 days')
    and convert_timezone('UTC', 'EST', e.createdat) < 'END_DATE 00:00:00' 
    and m.countrycode = 'US' 
    and eventcategory = 'activity_launch' 
    and fieldkey in ('activity', 'screen_id') 
    and fieldvalue in ('screen_us_deals_simplepay_screen', 'screen_us_deals_detailed') /* deal DAU via simple pay and view deal detail page*/
 );
"""

gmau_only_giftcardQ = """
select count(*) from
(
select distinct trsdimuserid
from trs_fact_txn_postattempt p inner join trs_dim_device m on p.trsdimdeviceid = m.id  
where convert_timezone('UTC', 'EST', p.createdat) >= ('END_DATE'::timestamp - interval '30 days')
     and convert_timezone('UTC', 'EST', p.createdat) < 'END_DATE'
     and p.tokenid is not null and m.countryCode ='US' and p.tokenid = 'GIFT'
union
select distinct trsdimuserid
from trs_fact_app_event e inner join trs_dim_device m on e.trsdimdeviceid = m.id
where eventcategory = 'transaction_attempt' and fieldkey='card_type' and fieldvalue ='card_category_gift_card'
     and m.countrycode = 'US'
     and convert_timezone('UTC', 'EST', e.createdat) >= ('END_DATE'::timestamp - interval '30 days')
     and convert_timezone('UTC', 'EST', e.createdat) < 'END_DATE'
);
"""


gmau_onlyrewardsQ = """
select count(distinct trsdimuserid)
from trs_fact_app_event e inner join trs_dim_device m on e.trsdimdeviceid = m.id
where eventcategory = 'reward_redeem' and fieldkey = 'action' and fieldValue = 'reward_attempted'  /*reward redemption*/
     and m.countrycode = 'US'
     and convert_timezone('UTC', 'EST', e.createdat) >= ('END_DATE'::timestamp - interval '30 days')
     and convert_timezone('UTC', 'EST', e.createdat) < 'END_DATE';
"""

daily_new_active_users_grossQ = """
select count(*) from
(
(
/*CD AU*/
select distinct trsdimuserid
from trs_fact_ws_notification w inner join trs_dim_device m on w.trsdimdeviceid = m.id  
where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null) 
     and convert_timezone('UTC', 'EST', w.createdat) < 'END_DATE'
     and m.countryCode ='US'
union
/* traditional membership and gc AU*/
select distinct trsdimuserid
from trs_fact_txn_postattempt p inner join trs_dim_device m on p.trsdimdeviceid = m.id   
where  convert_timezone('UTC', 'EST', p.createdat) < 'END_DATE'
     and p.tokenid is not null and m.countryCode ='US' and (p.tr = 'LC' or p.tokenid = 'GIFT')
union
select distinct trsdimuserid
from trs_fact_app_event e inner join trs_dim_device m on e.trsdimdeviceid = m.id
where (
     (eventcategory = 'activity_launch' and fieldkey = 'activity' and fieldvalue = 'screen_us_deals_simplepay_screen') /* deal AU via simple pay*/
     or (fieldvalue = 'screen_us_deals_detailed') /*deal AU who see any deal detail page*/
     or (eventcategory = 'deal_redeem_attempt' and fieldkey = 'redeemed_from') /*deal AU who clicked redeem button*/
     or (eventcategory in ('loyalty_view_card_details', 'loyalty_launch_simple_pay'))  /*membership card detail page view*/
     or (eventcategory = 'reward_redeem' and fieldkey = 'action' and fieldValue = 'reward_attempted')  /*reward redemption*/
     or (eventcategory = 'transaction_attempt' and fieldkey='card_type' and fieldvalue in ('membership_card','card_category_gift_card'))
     )
     and m.countrycode = 'US'
     and convert_timezone('UTC', 'EST', e.createdat) < 'END_DATE'
)
except 
(
/*CD AU*/
select distinct trsdimuserid
from trs_fact_ws_notification w inner join trs_dim_device m on w.trsdimdeviceid = m.id  
where event = 'TRANSACTION' and (transactionOob = false or transactionOob is null) 
     and convert_timezone('UTC', 'EST', w.createdat) < ('END_DATE'::timestamp - interval '1 day') 
     and m.countryCode ='US'
union
/* traditional membership and gc AU*/
select distinct trsdimuserid
from trs_fact_txn_postattempt p inner join trs_dim_device m on p.trsdimdeviceid = m.id   
where  convert_timezone('UTC', 'EST', p.createdat) < ('END_DATE'::timestamp - interval '1 day') 
     and p.tokenid is not null and m.countryCode ='US' and (p.tr = 'LC' or p.tokenid = 'GIFT')
union
select distinct trsdimuserid
from trs_fact_app_event e inner join trs_dim_device m on e.trsdimdeviceid = m.id
where (
     (eventcategory = 'activity_launch' and fieldkey = 'activity' and fieldvalue = 'screen_us_deals_simplepay_screen') /* deal AU via simple pay*/
     or (fieldvalue = 'screen_us_deals_detailed') /*deal AU who see any deal detail page*/
     or (eventcategory = 'deal_redeem_attempt' and fieldkey = 'redeemed_from') /*deal AU who clicked redeem button*/
     or (eventcategory in ('loyalty_view_card_details', 'loyalty_launch_simple_pay'))  /*membership card detail page view*/
     or (eventcategory = 'reward_redeem' and fieldkey = 'action' and fieldValue = 'reward_attempted')  /*reward redemption*/
     or (eventcategory = 'transaction_attempt' and fieldkey='card_type' and fieldvalue in ('membership_card','card_category_gift_card'))
     )
     and m.countrycode = 'US'
     and convert_timezone('UTC', 'EST', e.createdat) < ('END_DATE'::timestamp - interval '1 day') 
)
);
"""





#%%

queryList = [ruQ, gcruQ, ncruQ, gmauQ, gwauQ, gdauQ, ru_daily_diffQ, new_CRU_dailyQ, net_new_cru_for_yesterdayQ, gcru_only_credit_debitQ, gcru_only_membershipQ, gcru_only_giftcardQ, ncru_only_credit_debitQ, ncru_only_membershipQ, ncru_only_giftcardQ, gmau_only_credit_debitQ, gmau_only_membershipQ, gmau_only_dealsQ, gmau_only_giftcardQ, gmau_onlyrewardsQ, daily_new_active_users_grossQ]
#%%
df = pd.DataFrame({'Name':['ru','gcru','ncru', 'gmau', 'gwau', 'gdau', 'ru_daily_diff', 'new_CRU_daily','net_new_cru_for_yesterday', 'gcru_only_credit_debit', 'gcru_only_membership', 'gcru_only_giftcard', 'ncru_only_credit_debit', 'ncru_only_membership', 'ncru_only_giftcard', 'gmau_only_credit_debit', 'gmau_only_membership', '001_deals_only_MAU', 'gmau_only_giftcard', 'gmau_onlyrewards', 'daily_new_active_users_gross']})
#%%
df['KPI'] = ['Registered User(Gross)', 'Gross CRU (Credit/Debit+Membership+GC)', 'Gross Net CRU (Credit/Debit+Membership+GC', 'Gross MAU (Credit+Debit+Membership+GC)', 'Gross WAU (Credit+Debit+Membership+GC)', 'Gross DAU (Credit+Debit+Membership+GC)', 'Daily new RU', 'New Daily CRU', 'Net New CRU', 'C/D only Gross CRU', 'Membership only Gross CRU', 'Gift Card only Gross CRU', 'C/D only Net CRU', 'Membership only Net CRU', 'Gift Card only Net CRU', 'C/D only Gross MAU', 'Membership only Gross MAU', 'Deals and offers only MAU', 'Gift Card only Gross MAU', 'Rewards MAU', 'Daily New Active Users (Gross)']


#%%
def getWSQueryRst(q1):
    q1 = q1.replace('END_DATE', eDate)
    rst = pd.read_sql_query(q1, SMP_engine)
    rst.columns = ['count']
    return rst['count'][0]
    
def getRedQueryRst(q1):
    q1 = q1.replace('END_DATE', eDate)
    rst = pd.read_sql_query(q1, red_engine)
    rst.columns = ['count']
    return rst['count'][0]
    
def getLatestTimeStamp(table):
    q1 = """select  convert_timezone('UTC', 'EST', max(createdat)) as latestTimestamp from TABLE_NAME;"""
    q1 = q1.replace('END_DATE', eDate)
    q1 = q1.replace('TABLE_NAME', table)
    rst = pd.read_sql_query(q1, red_engine)
    return rst['latesttimestamp'][0]
    

#%%
redshiftNumList = []
for q in queryList:
        redshiftNumList.append(getRedQueryRst(q))
        
#%%

df['Redshift Count'] = redshiftNumList

#%%
tableList = ['trs_fact_token_history', 'trs_fact_txn_postattempt', 'trs_fact_ws_notification']
df['NotUpdating'] = False
for table in tableList:
    latestTS = getLatestTimeStamp(table)
    if latestTS < pd.to_datetime(eDate):
        df['NotUpdating'] = True
        break



#%%
df.to_csv(output_path, index = False)
