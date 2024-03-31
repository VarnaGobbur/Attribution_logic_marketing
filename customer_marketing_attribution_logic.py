import pandas as pd
import numpy as np
import sys
import json
import pymysql
from datetime import date,timedelta,datetime
from pandas import ExcelWriter
import os
import time
from tabulate import tabulate
import smtplib
import sqlalchemy
from sqlalchemy import create_engine
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
import shutil
import pandasql as ps

 
start_time = time.time()

today = date.today()
yesterday = today - timedelta(days=1)
current_week = date.today().isocalendar()[1]
now = datetime.now()- timedelta(days=0)
next_week = current_week + 1
print("current week is ",current_week)
print("next week is ",next_week)
result = pd.DataFrame()

# creating my sql connections

def openConnection(host1,port1,user1,pswd1,db1):
    global con
    try:
        con= pymysql.connect(host=host1,port=int(port1), user=user1, password=pswd1,database=db1)
    except:
        print("ERROR: Unexpected error: Could not connect to MySql instance.")
        sys.exit()

#pull mysql data

with open('config_file') as con_file:
    cons=json.load(con_file)
    df_compliance=pd.DataFrame()
    for p in cons:
        #print(json.dumps(p,indent=0))
        try:
            openConnection(p["SERVER"],p["port"],p["uid"],p["pswd"],p["database"])
            print("Fetching data from ",p["campaign"])
            with con.cursor() as cur:
                sql = f"""
                SELECT
                customerid,
                campaign_name,
                date_of_campaign,
                conversion_probability
                FROM targeting_data 
                where campaign_name = {p["campaign"]}
                and date_of_campaign >= curdate() - interval 4 day

"""
            
                
                df_targets = pd.read_sql(sql,con)
                
                print(df_targets.shape)
                df_final_targets=df_compliance.append(df_targets)
                print(df_final_targets.shape)

                cur.close()
                con.close()
        except Exception as e:
            print(e)
        finally:
            print('Query Successful')
    print(df_final_targets.head())
    print(df_final_targets.shape)



# pulling data from redshift
def visits_data():
    
    print('extracting data from redshift')
    engine = create_engine('postgresql://user:pswd@databaseconnection:port/dbname')
    con = engine.connect()
    sql = f"""
   SELECT
   website,
   visit_date_time,
   customerid,
   widget,
   page_TYPE,
   from_page
   FROM clicks_table
   where visit_date_time BETWEEN DATEADD('DAY',-10,CURRENT_DATE) and CURRENT_DATE
    """

    print("parsing into dataframe")
    result2 = pd.read_sql(sql,engine)
    print(result2)
    return result2
 

def date_range_diff(row):
  difference = df_attribution_start['visit_date_time'] - df_attribution_start['date_of_campaign']
  if difference <= 10:
    attribution = 1
  else:
    attribution = 0
  return attribution

df_visits_data = visits_data()

df_attribution_start = df_visits_data.merge(df_visits_data,how='inner',left_on=[custome_id],right_on=[customer_id])

#taking visits only after campaign date. eliminating prior visits.
df_attribution_start =  df_attribution_start[df_attribution_start['visit_date_time'] >= df_attribution_start['date_of_campaign']]

#calculating attribution
#taking attributed to campaign if visit is within 10 days of campaign send
df_attribution_start['attribution'] = df_attribution_start.apply(lanbda row:date_range_diff(row),axis= 1)

df_attributed_cust = df_attribution_start[df_attribution_start['attribution']==1]

output_filepath = f"{os.getcwd()}/attribution_output_file.csv"

df_attributed_cust.to_csv(output_filepath, index=False)

upload_to_s3(output_filepath,s3_bucket,s3_filename)
