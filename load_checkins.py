#!/usr/bin/env python

#     FILE INFO       ##################################################
#
#        Mahitha Valluru                March 17, 2017
#
#        YELP Data Science Project
#
########################################################################


#   ALL IMPORTS   ######################################################
# import pandas as pd
import json
# import os
from pprint import pprint
import mysql.connector
import re
#######################################################################


path = 'D:\Mahitha\Yelp\Data\Challenge Round 9 Data\yelp_academic_dataset_'
checkin_file = 'checkin.json'
filepath = path + checkin_file

connection = mysql.connector.connect(user='root', password='SunnyBablu$999',
                              host='localhost',port='3306',
                              database='yelp')
cursor = connection.cursor()

cursor.execute('DROP TABLE `yelp`.`checkin`')


sql_checkin_time_init = """CREATE TABLE checkin
                        (business_id VARCHAR(30) ,
                        checkin_type VARCHAR(30) ,
                        checkin_day VARCHAR(30) ,
                        checkin_hour VARCHAR(30) ,
                        checkin_count VARCHAR(30))"""

cursor.execute(sql_checkin_time_init)


with open(filepath, encoding="utf8") as f:
    data = f.readlines()
    i = 0
    for line in data:
        dataPoint = json.loads(line)
        # pprint(dataPoint)

        bus_id = dataPoint['business_id']
        type = dataPoint['type']
        timeArray = dataPoint['time']

        # get the time array and split into Day, Hour, Number of Checkins
        j = 0
        while j < len(timeArray):
            time_str = timeArray[j]
            arrTime = re.split('-|:', time_str)
            day_id = arrTime[0]
            hour_id = arrTime[1]
            checkin_count = arrTime[2]
            cursor.execute('''INSERT into yelp.checkin (business_id, checkin_type, checkin_day, checkin_hour, checkin_count)
                                      values (%s, %s, %s, %s, %s )''',
                           (bus_id, type, day_id, hour_id, checkin_count))
            j += 1

            connection.commit()
        i += 1


cursor.close()

connection.commit()
connection.close()


