import argparse
import json
import os
import mysql.connector
import logging
import traceback
import pytz
from datetime import datetime
from datetime import timedelta
import numpy as np
import csv

from config_cred import DATABASE_CONFIG
from config_cred import EMAIL_ALERT_TEMPLATE_1, EMAIL_ALERT_TEMPLATE_2
from email_pusher import send_email


COMMON_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


MYSQL_HOST = DATABASE_CONFIG['MYSQL_HOST']
MYSQL_USER = DATABASE_CONFIG['MYSQL_USER']
MYSQL_PASSWORD = DATABASE_CONFIG['MYSQL_PASSWORD']
MYSQL_DB = DATABASE_CONFIG['MYSQL_DB']


def utc_to_sl(utc_dt):
    sl_timezone = pytz.timezone('Asia/Colombo')
    #print(sl_timezone)
    #print(utc_dt.replace(tzinfo=pytz.utc).astimezone(tz=sl_timezone))
    return utc_dt.replace(tzinfo=pytz.utc).astimezone(tz=sl_timezone)


def get_leecom_precipitation():

    sql = "SELECT station, end_date FROM curw_obs.run where (variable='10')"
    cursor.execute(sql, )
    check_result = cursor.fetchall()
    # print(check_result)
    return check_result

def downloaddata(cursordb,strtime,endtime):
    cursordb.callproc('get24hrCumRf',(str(strtime),str(endtime)))
    #print(stid)
    #print(mycursor)

    if mycursor.rowcount==0:
        print("No data returned")
    else:
        rows = []
        #rawfetch=mycursor.fetchall()
        for row in mycursor.stored_results():
        #print(row.fetchall())
            
            rows.append(row.fetchall())
            #print(type(rows))
            rows = np.array(rows)
            rows=np.vstack(rows)
            
            if not rows.any():
                print("No data returned")
                df=[]
            else:
                df=rows
    return df


if __name__ == "__main__":

    logging.info("--- Connecting to MySQL Database ---")
    mydb = mysql.connector.connect(host=MYSQL_HOST, database=MYSQL_DB, user=MYSQL_USER, passwd=MYSQL_PASSWORD)

    #now_date_obj = datetime.now()
    now_date_obj = datetime.now(pytz.timezone('Asia/Colombo'))
    todayst=now_date_obj.strftime('%Y-%m-%d %H:%M:%S')
    print(todayst)
    day1bfr=now_date_obj-timedelta(days=1)
    day1bfrst=day1bfr.strftime('%Y-%m-%d %H:%M:%S')
    day1hrbfr=now_date_obj-timedelta(hours=1)
    day1hrbfrst=day1hrbfr.strftime('%Y-%m-%d %H:%M:%S')
    print(day1bfrst)

    print(now_date_obj)
    #datetime.strptime(start_datetime_tally, '%Y, %m, %d, %H')
    #now_datetime = datetime.strptime(now_date_obj, COMMON_DATE_FORMAT)

    print("Successfully connected to %s database at curw_iot_platform cloud (%s)." % (MYSQL_DB, MYSQL_HOST))

    with open('previous.csv', 'r') as csvfile:
        pr_rec = csv.reader(csvfile)
        pr_rec = list(csv.reader(csvfile))
        pr_rec = np.array(pr_rec)
        pr_rec=np.vstack(pr_rec)

    with open('previoushr.csv', 'r') as csvfile:
        pr_rechr = csv.reader(csvfile)
        pr_rechr = list(csv.reader(csvfile))
        pr_rechr = np.array(pr_rechr)
        pr_rechr=np.vstack(pr_rechr)

    try:
        stationname = []
        variables = []

        mycursor = mydb.cursor()

        #cursor = mydb.cursor()
        # print(datetime.now())

        #check if the reported end date is upto date
        results = downloaddata(mycursor,day1bfrst,todayst)
        resultshr = downloaddata(mycursor,day1hrbfrst,todayst)
        #print(results)

        eml_bdy=""
        eml_bdyhr=""

        max_rf=max(results[:,2])
        max_rfhr=max(resultshr[:,2])
        #print(max_rf)
        number_of_steps=int(max_rf//50)
        number_of_stepshr=int(max_rfhr//50)
        #print(number_of_steps)

        ### for 24 hr checks ###

        for step in range(1,number_of_steps+1):
            header_chk=0
            #print("step=",step)
            for result in results:
                #print("station="+result[0])
                chck_prv_val=float(pr_rec[pr_rec[:,0]==result[0],2][0])
                if int(chck_prv_val) < int(result[2]):
                    trend="\u2191\n"
                elif int(chck_prv_val) > int(result[2]):
                    trend = "\u2193\n"
                else:
                    trend = "\u2194\n"
                if (result[2]< 50*(step+1)) and (result[2]>50*(step)):
                    if header_chk==1:
                        
                        eml_bdy=eml_bdy+result[0]+": "+str(result[2])+"mm "+ trend
                    else:
                        eml_bdy=eml_bdy+"\nRainfall higher than "+str(50*step)+" mm \n"+result[0]+": "+str(result[2])+"mm"+trend
                        header_chk=1
        #print(eml_bdy)

        #if eml_bdy!="":
        #   send_email(msg=EMAIL_ALERT_TEMPLATE_1 % (eml_bdy))

        fp = open('previous.csv', 'w')
        myFile = csv.writer(fp)
        myFile.writerows(results)
        fp.close()
        
### for 1 hr checks ###


        for step in range(1,number_of_stepshr+1):
            header_chk=0
            #print("step=",step)
            for result in resultshr:
                #print("station="+result[0])
                chck_prv_val=float(pr_rechr[pr_rechr[:,0]==result[0],2][0])
                if int(chck_prv_val) < int(result[2]):
                    trend="\u2191\n"
                elif int(chck_prv_val) > int(result[2]):
                    trend = "\u2193\n"
                else:
                    trend = "\u2194\n"
                if (result[2]< 50*(step+1)) and (result[2]>50*(step)):
                    if header_chk==1:
                        
                        eml_bdyhr=eml_bdyhr+result[0]+": "+str(result[2])+"mm "+ trend
                    else:
                        eml_bdyhr=eml_bdyhr+"\nRainfall higher than "+str(50*step)+" mm \n"+result[0]+": "+str(result[2])+"mm"+trend
                        header_chk=1
        #print(eml_bdy)

        if eml_bdy!="" and eml_bdyhr!="":
            send_email(msg=EMAIL_ALERT_TEMPLATE_1 % (eml_bdy+"\n\n\nExcessive rainfalls for past 1 hour are as follows.\n"+eml_bdyhr))
        elif eml_bdy!="":
            send_email(msg=EMAIL_ALERT_TEMPLATE_1 % (eml_bdy))


        fp = open('previoushr.csv', 'w')
        myFile = csv.writer(fp)
        myFile.writerows(resultshr)
        fp.close()
        





    except Exception as e:
        logging.warning("--- Connecting to MySQL failed--- %s", e)

    finally:
        # Close connection.
        mydb.close()