import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import time
import math
import datetime
#%%
# Change this to your redshift credential
red_engine = create_engine('')
eDate = time.strftime("%Y-%m-%d")

#%%

def execute(q, endDate, engine):
    q = q.replace('END_DATE', endDate)
    tmp = pd.read_sql_query(q, engine)
    return tmp
    
etlQuery = """
select to_char(t.createdat,'YYYY-MM-DD:HH24'), count(t.id)
from trs_fact_token_history t join trs_dim_device m on t.trsdimdeviceid = m.id
where t.createdat>=convert_timezone('EST', 'UTC', ('END_DATE'::timestamp - interval '1 days'))
and t.createdat<convert_timezone('EST', 'UTC', ('END_DATE'::timestamp))
group by 1 order by 1;
"""

df = execute(etlQuery, eDate, red_engine)

df.columns = ['time', 'count']
#%%
def addChangeRate(df):
    df2 = {'change_rate':[]}
    df2['change_rate'].append(0)
    for i in range(1, df.shape[0]):
        countList = df['count']
        rate = (1.00 * (countList[i] - countList[i - 1])) / countList[i - 1]
        df2['change_rate'].append(rate)
    df2 = pd.DataFrame(df2)
    df['change_rate'] = df2

addChangeRate(df)

df['Flag'] = ((abs(df['change_rate']) < 0.4) )


#%%
eDate = '{dt.year}-{dt.month}-{dt.day}'.format(dt = datetime.datetime.now())
df.to_csv(eDate + 'ETLHourlyStatus.csv', index = False)