import requests
import time
from dateutil.parser import parse 
import collections
import sqlite3 as lite
from pandas.io.json import json_normalize
import matplotlib.pyplot as plt
import pandas as pd

# access data on citibikes from website in json format
r = requests.get('http://www.citibikenyc.com/stations/json')

# format json
df = json_normalize(r.json()['stationBeanList'])

# create unique list of keys for each station listing
key_list = [] 
for station in r.json()['stationBeanList']:
    for k in station.keys():
        if k not in key_list:
            key_list.append(k)

# create database and connect
con = lite.connect('citi_bike.db')
cur = con.cursor()

# create a table to store information about each citibike station
with con:
    cur.execute('DROP TABLE IF EXISTS citibike_reference')
    cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT )')
	
# prepare a SQL statement to execute over and over again in the loop
sql = "INSERT INTO citibike_reference (id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

# for loop to populate values in the database
with con:
    for station in r.json()['stationBeanList']:
        #id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location)
        cur.execute(sql,(station['id'],station['totalDocks'],station['city'],station['altitude'],station['stAddress2'],station['longitude'],station['postalCode'],station['testStation'],station['stAddress1'],station['stationName'],station['landMark'],station['latitude'],station['location']))
		
# extract the column from the DataFrame and put them into a list
station_ids = df['id'].tolist() 

# add '_' to the beginning of station name and add the data type for SQLite
station_ids = ['_' + str(x) + ' INT' for x in station_ids]

# create the table to store available bike data for each citibike station
# concatentate the string and join all the station ids (now with '_' and 'INT' added)
with con:
    cur.execute('DROP TABLE IF EXISTS available_bikes')
    cur.execute("CREATE TABLE available_bikes ( execution_time INT, " +  ", ".join(station_ids) + ");")

# for loop to get data every minute for an hour, store in available_bikes table
for i in range(60):
    r = requests.get('http://www.citibikenyc.com/stations/json')
    exec_time = parse(r.json()['executionTime'])

    cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%Y-%m-%dT%H:%M:%S'),))
    con.commit()

    id_bikes = collections.defaultdict(int)
    for station in r.json()['stationBeanList']:
        id_bikes[station['id']] = station['availableBikes']

    for k, v in id_bikes.iteritems():
        cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = '" + exec_time.strftime('%Y-%m-%dT%H:%M:%S') + "';")
    con.commit()

    time.sleep(60)

# close the database connection when done importing data
con.close()

# reconnect to the database to begin analysis
con = lite.connect('citi_bike.db')
cur = con.cursor()

# SQLite query to sort data by execution time
df = pd.read_sql_query("SELECT * FROM available_bikes ORDER BY execution_time",con,index_col='execution_time')

hour_change = collections.defaultdict(int)
for col in df.columns:
    station_vals = df[col].tolist()
    #remove the "_" inserted for the SQLite column name
	station_id = col[1:]
    station_change = 0
    for k,v in enumerate(station_vals):
        if k < len(station_vals) - 1:
            station_change += abs(station_vals[k] - station_vals[k+1])
    # convert the station id back to integer
	hour_change[int(station_id)] = station_change 

# create a list of the dictionary's keys and values
# goal is to get the most active station
def keywithmaxval(d):
    v = list(d.values())
    k = list(d.keys())

    # return the key with the max value
    return k[v.index(max(v))]

# assign the max key to max_station
max_station = keywithmaxval(hour_change)

# query SQLite for reference information
cur.execute("SELECT id, stationName, latitude, longitude FROM citibike_reference WHERE id = ?", (max_station,))
data = cur.fetchone()
print "The most active station is station id %s at %s latitude: %s longitude: %s " % data
print "With " + str(hour_change[379]) + " bicycles coming and going in the hour between " + datetime.datetime.fromtimestamp(int(df.index[0])).strftime('%Y-%m-%dT%H:%M:%S') + " and " + datetime.datetime.fromtimestamp(int(df.index[-1])).strftime('%Y-%m-%dT%H:%M:%S')

# The most active station is station id 281 at Grand Army Plaza & Central Park S latitude: 40.7643971 longitude: -73.97371465 
# With 15 bicycles coming and going in the hour between 2015-06-27T13:17:57 and 2015-06-27T14:17:57 

# plot bar graph to show station activity
plt.bar(hour_change.keys(), hour_change.values())
plt.show()