import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import time
red_engine = create_engine('redshift+psycopg2://admin:tWV173AMspc8EHSXZbfk@trs-redshift-cluster.czlcstchuzge.us-east-1.redshift.amazonaws.com:45439/trsanalytics')
promo_engine = create_engine('mysql://SMPSCMNQLIQ:zAj7jbPV~N@db3-mys-us1a-prd.internal:43306/promotion')
SMP_engine = create_engine('mysql://SMPSCMNQLIQ:zAj7jbPV~N@db3-mys-us1a-prd.internal:43306/SMPSCMN')

eDate = time.strftime("%Y-%m-%d")
#Change the pahts
PATH = 'C:\QlikDataFiles\Verification'
redshift_csv_input_path = 'C:\QlikDataFiles\out\{}redshiftData.csv'.format(eDate)
qlik_csv_input_path = 'C:\QlikDataFiles\out\kpi_{}.csv'.format(eDate)
output_path = 'C:\QlikDataFiles\out\{}validationRst.csv'.format(eDate)
#%%
df = pd.read_csv(redshift_csv_input_path)

#%%
Qlikdf = pd.read_csv(qlik_csv_input_path, sep = '\t')
#%%
df['Qlik Count'] = Qlikdf['Qlik Count']

#%%
df['Percentage Difference'] = abs(df['Qlik Count'] - df['Redshift Count'])/df['Redshift Count']
#%%
isNotUpdating = df.iloc[0]['NotUpdating']
#%%
threshold = [20,20,20,100,120,150,20,20,20,20,20,20,20,20,20,20,40,200,20,50,50]
df['threshold'] = threshold
df['isAboveThreshold'] = abs(df['Qlik Count'] - df['Redshift Count']) > df['threshold'] 
tmpdf = pd.DataFrame({'Name': ['NotUpdating'], 'isAboveThreshold':[isNotUpdating]})
df = df.append(tmpdf)
df = df.drop('NotUpdating', 1)
#%%

df.to_csv(output_path, index = False)
