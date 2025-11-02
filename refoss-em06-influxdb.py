#!/usr/bin/python3

import argparse
import requests
import json
import random
from hashlib import md5
import string
import time;
from influxdb import InfluxDBClient

EM06IP = ['192.168.5.15/EG/OG', '192.168.5.14/KG/GH']
SUBSCRIBER_NAME = 'refoss_influx'

#INFLUXDB_ADDRESS = '192.168.5.6'
#INFLUXDB_PORT = 8086
INFLUXDB_USER = 'admin'
INFLUXDB_PASSWORD = None
INFLUXDB_DATABASE = 'electricity'

# read the Refoss EM06 electricity values
def readEM06( ip:str ):
    # create signature
    # Generate a random 16 byte string
    randomstring = "".join(
        random.SystemRandom().choice(string.ascii_uppercase + string.digits)
            for _ in range(16)
    )
    # Hash it as md5
    md5_hash = md5()
    md5_hash.update(randomstring.encode("utf8"))
    messageId = md5_hash.hexdigest().lower()
    #
    timestamp = int(round(time.time()))
    #
    userkey = ""
    #
    # Hash the messageId, the key and the timestamp
    md5_hash = md5()
    strtohash = f"{messageId}{userkey}{timestamp}"
    md5_hash.update(strtohash.encode("utf8"))
    signature = md5_hash.hexdigest().lower()

    # create request
    request_data = {
        "header": {
            "messageId": messageId,
            "namespace": "Appliance.Control.ElectricityX",
            "method": "GET",
            "payloadVersion": 1,
            "from": "",
            "timestamp": timestamp,
            "timestampMs": 0,
            "sign": signature
        },
        "payload": {
            "electricity": {}
        }
    }

    try:
        # send request
        response = requests.post(url, json=request_data)

        # get the response
        if response.status_code == 200:
            return response.json()
        else:
            print("Request error:", response.status_code, response.text)

    except requests.exceptions.RequestException as e:
        print("Connection error:", e)

def convert2line( em06Data:dict, unitNames:[], em06ip:str ):
    data = []
    # create influx DB ingestion message and write to server
    timestamp = str(em06Data['header']['timestamp']) + str(em06Data['header']['timestampMs'])
    for channel in em06Data['payload']['electricity']:
        # db name
        line = INFLUXDB_DATABASE 
        # tags
        if len(unitNames) == 2:
            line += ",name="  + unitNames[int((channel['channel'] - 1) / 3)]
            line += ",phase=" + str((channel['channel'] - 1) % 3 + 1)
        else: # using 6 names, one for each channel
            line += ",name="  + unitNames[channel['channel'] - 1]
            line += ",channel=" + str(channel['channel'])
        line += ",ip="    + em06ip
        # fields
        line += " voltage="  + str(channel['voltage'])
        line += ",current="  + str(channel['current'])
        line += ",power="    + str(channel['power'])
        line += ",factor="   + str(channel['factor'])
        line += ",consumed=" + str(channel['mConsume'])
        # timestamp
        line += " " + timestamp
        data.append(line);

    return data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Refoss EM06 energy reader and InfluxDB writer')
    parser.add_argument('-r', '--refossEM06', type=str, 
        help="The EM06 device IPAddress with two names /A/B for two 3-phase groups or as default 'A1/B1/C1/A2/B2/C2'", 
        action='append', nargs='+', metavar='192.1.1.1/A/B')
    parser.add_argument('-i', '--dbip', type=str, help="The influxdb IP-Address", default='127.0.0.1')
    parser.add_argument('-d', '--dbname', type=str, help="The influxdb name", default=INFLUXDB_DATABASE)
    parser.add_argument('-u', '--dbuser', type=str, help="The influxdb username", default=INFLUXDB_USER)
    parser.add_argument('-t', '--dbtoken', type=str, help="The influxdb password or token", default=INFLUXDB_PASSWORD)
    parser.add_argument('-n','--nowrite', help="skip writing to database", action='store_true')
    parser.add_argument('-v','--verbose', help="show verbose output", action='store_true')
    args = parser.parse_args()
    
    verbose = args.verbose

    print(str(args.refossEM06))

    # read devices
    data = []
    for device in args.refossEM06:
        ip, *groupNames = device[0].split('/')
        if len(groupNames) == 0:
            groupNames = ['A1','B1','C1','A2','B2','C2']
        elif len(groupNames) != 2 and len(groupNames) != 6:
            print("ERROR: group names must be 2 or 6 long", file=sys.stderr)
            exit(1)

        url = 'http://' + ip + '/public'

        if verbose:
            print("\nRefoss EM06 ip=" + ip + " groups=" + str(groupNames))

        js = readEM06(url)
        lines = convert2line(js, em06ip=ip, unitNames=groupNames)

        if verbose:
            for line in lines:
                print(line)
                
        data += lines

    # write to DB
    if args.nowrite:
        if verbose: print("\nSkipping writing to database")
    else:
        # open the influxdb connection
        INFLUXDB_DATABASE = args.dbname
        INFLUXDB_ADDRESS = args.dbip
        INFLUXDB_PORT = '8086'
        if ':' in INFLUXDB_ADDRESS:
            INFLUXDB_ADDRESS, INFLUXDB_PORT = INFLUXDB_ADDRESS.split(':')
        influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, INFLUXDB_PORT, args.dbuser, args.dbtoken, None)
        #databases = influxdb_client.get_list_database()
        #if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
        #    influxdb_client.create_database(INFLUXDB_DATABASE)
        #    print('INFO', 'initialized DB')
        #influxdb_client.switch_database(INFLUXDB_DATABASE)
        influxdb_client.write_points(data, database=INFLUXDB_DATABASE, 
            time_precision='ms', batch_size=100, protocol='line')
    
    if verbose:
        print("Done.")
