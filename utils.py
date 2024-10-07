import argparse
import json
import os
import mysql.connector
import logging
import traceback
import pytz
from datetime import datetime
from datetime import timedelta

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

    sql = "SELECT station, end_date FROM curw_iot.run_view where (name like 'Leecom' and variable='Precipitation')"
    cursor.execute(sql, )
    check_result = cursor.fetchall()
    # print(check_result)
    return check_result


if __name__ == "__main__":

    logging.info("--- Connecting to MySQL Database ---")
    mydb = mysql.connector.connect(host=MYSQL_HOST, database=MYSQL_DB, user=MYSQL_USER, passwd=MYSQL_PASSWORD)

    #now_date_obj = datetime.now()
    now_date_obj = utc_to_sl(datetime.now()).replace(tzinfo=None)

    print(now_date_obj)
    #datetime.strptime(start_datetime_tally, '%Y, %m, %d, %H')
    #now_datetime = datetime.strptime(now_date_obj, COMMON_DATE_FORMAT)

    print("Successfully connected to %s database at curw_iot_platform cloud (%s)." % (MYSQL_DB, MYSQL_HOST))

    try:
        stationname = []
        variables = []

        cursor = mydb.cursor()

        #cursor = mydb.cursor()
        # print(datetime.now())

        #check if the reported end date is upto date
        results = get_leecom_precipitation()
        #print(results)

        for result in results:

            station_name = result[0]
            lastupdated_datetime = result[1]

            print("%s station last updated at %s" % (station_name, lastupdated_datetime))


            #check the difference of last updated time and the current time

            gap_time = now_date_obj - lastupdated_datetime
            gap_time_in_s = gap_time.total_seconds()
            gap_time_min = divmod(gap_time_in_s, 60)[0]

            print("Time duration between the last updated time and current time of %s : %s" % (station_name, gap_time_min))

            print(gap_time)
            print(gap_time_min)
            print(station_name)

            if not ((station_name == 'Leecom Test') | (station_name == 'Nawala') | (station_name == 'Ragama')):
                if 0 < gap_time_min < 30:

                    #check in the json whether the station has come online
                    # if so, notify via an email that the station has come online
                    # Also remove the station from the json

                    if not os.stat('offlinestations.json').st_size == 0:
                        with open('offlinestations.json', 'r') as openfile:
                            json_object = json.load(openfile)


                            for i in range(len(json_object)):
                                if json_object[i]['StationName'] == station_name:
                                #if any(objectjs['StationName'] == station_name for objectjs in json_object):

                                    print("%s was removed from the json as it has started to respond at %s" % (
                                        station_name, lastupdated_datetime))

                                    send_email(msg=EMAIL_ALERT_TEMPLATE_2 % (station_name, lastupdated_datetime))

                                    print("%s has started to report back" % station_name)
                                    del json_object[i]
                                    break


                            open('offlinestations.json', "w").write(
                                json.dumps(json_object, sort_keys=True, indent=4, separators=(',', ': '))
                            )

                    if station_name in stationname:
                        for i, j in enumerate(stationname):
                            if j == station_name:
                                print(i)
                                variables.pop(i)

                        stationname.remove(station_name)


                if gap_time_min >= 30:
                    print(station_name)
                    print(gap_time_min)
                    #check if the json is empty

                    if os.stat('offlinestations.json').st_size == 0:
                        print('Json file is empty!')
                        print("%s station has stopped responding since %s" % (station_name, lastupdated_datetime))

                        stationname.append(station_name)
                        variables.append("Precipitation")

                        output = [{"StationName": sn, "Variable": var} for sn, var in zip(stationname, variables)]

                        outputjs = json.dumps(output, indent=4)
                        with open("offlinestations.json", "w") as outfile:
                            outfile.write(outputjs)

                        send_email(msg=EMAIL_ALERT_TEMPLATE_1 % (station_name, lastupdated_datetime, station_name))

                    #check if the station is already exist in the json
                    else:
                        with open('offlinestations.json', 'r') as openfile:
                            json_object = json.load(openfile)

                            #if station does not exist in the json, insert to the json file
                            #Also notify relevant parties via an email

                            if not any(objectjs['StationName'] == station_name for objectjs in json_object):

                                print("%s station has stopped responding since %s" % (station_name, lastupdated_datetime))

                                stationname.append(station_name)
                                variables.append("Precipitation")

                                output = [{"StationName": sn, "Variable": var} for sn, var in
                                          zip(stationname, variables)]

                                outputjs = json.dumps(output, indent=4)

                                with open("offlinestations.json", "w") as outfile:
                                    outfile.write(outputjs)

                                send_email(msg=EMAIL_ALERT_TEMPLATE_1 % (station_name, lastupdated_datetime, station_name))
                            else:
                                print("%s Station is already reported as a not working location" % station_name)


    except Exception as e:
        logging.warning("--- Connecting to MySQL failed--- %s", e)

    finally:
        # Close connection.
        mydb.close()