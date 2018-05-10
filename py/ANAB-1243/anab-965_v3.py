from email_sender import EmailSender
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import time
red_engine = create_engine('redshift+psycopg2://admin:ibawsome@redshift-cluster.czlcstchuzge.us-east-1.redshift.amazonaws.com:45439/analytics')
filename = 'InApp.csv'
#%%
eDate = time.strftime("%Y-%m-%d")
#%%
def getRst():
    query = """
select 'END_DATE' as date, inapppid as Product_ID,
(case inapppid when '36bd54b9a8064f7e9b20bd' then 'Raise alpha'
when '4c8f6393d2c34b8b909476' then 'Raise discount gift cards'
when '96a58afcc881451b905d2f' then 'Fancy'
when '18f60d1e8d5840e7be3b6b' then 'IBM Canada'
when '19549d7c8df8468bb21647' then 'Hello Vino, Inc'
when '310929e16bf64ec4a71ebd' then 'smps_inapp100'
when '8796cd648af041b7b43659' then 'Adyen BV'
when '88399fbce50544e79ded9d' then 'smps_inapp100'
when '88a19ac6a22342c4a190c1' then 'Wish'
when '9ec48528b3cb4f618d298f' then 'SRA'
when 'de3a6ce3f758481bac1309' then 'SEA Ecomm Division'
when '0107b85b52b44f8ead626b' then 'IBM Canada'
else 'Samsung Electronics' end) as Merchant,
count(*) as transaction_num, count(distinct trsdimdeviceid) as unique_device
from trs_fact_txn_postattempt p inner join trs_dim_device m on p.trsdimdeviceid = m.id
where p.createdat>=convert_timezone('EST', 'UTC', 'END_DATE'::timestamp - interval '1 day')
and p.createdat < convert_timezone('EST', 'UTC', ('END_DATE'::timestamp))
and inapppid in
('36bd54b9a8064f7e9b20bd',
'4c8f6393d2c34b8b909476',
'96a58afcc881451b905d2f',
'18f60d1e8d5840e7be3b6b',
'19549d7c8df8468bb21647',
'310929e16bf64ec4a71ebd',
'8796cd648af041b7b43659',
'88399fbce50544e79ded9d',
'88a19ac6a22342c4a190c1',
'9ec48528b3cb4f618d298f',
'de3a6ce3f758481bac1309',
'0107b85b52b44f8ead626b',
'Dqz68EBXSx6Mv9jsaZxzaA')
and  p.tokenid is not null and m.countryCode ='US'
group by 1,2,3 order by 1,2,3;
    """
    query = query.replace('END_DATE', eDate)
    tmp = pd.read_sql_query(query, red_engine)
    return tmp
    
#%%
pre_result = pd.read_csv(filename)
rst = getRst()
result = pd.concat([pre_result,rst])
result.to_csv(filename,index = False)
#rst.to_excel(eDate + '_anab-965.xlsx', index = False)
#html_string = rst.to_html(index = False)
#EmailSender().send(html_string);
