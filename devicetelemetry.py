import pandas as pd
import json
import random
from  datetime import *
import pyodbc
import numpy as np
import sys
import logging
import time
import os
from azure.eventhub import EventHubClient, Sender, EventData

# This will setup the device table in sql server
#TODO add create table for Device table. Right now the table should  have ben created in advance

def getConfig():
    config = None
    with open('config.json', 'r') as f:
        config = json.load(f)
    return config

def connectToDB(config):
    # Pull the secrets from config.json ( needs to be saved in the same directory)

    server = config['server']
    
    #name of your database
    database = 'iotdatabase' 
    username = config['username']
    password = config['password']
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    return cnxn

def setup(cnxn):
    
    cursor = cnxn.cursor()
    #check to see if the records are already exist int he database
    sql = 'Select count(*) from [dbo].[Device]'
    cursor.execute(sql)
    row = cursor.fetchone()
    numberofDevices = int(row[0])
    
    if(numberofDevices >= 25000):
        return 

    # Define the type of the devices, FirwareVersion
    deviceType = ['Video','Audio']
    firmwareVersion = [1.0,2.0]
    columns = ['DeviceID','DeviceType','FirmwareVersion','DateInstalled','Region','OwnerID']
    
    l = []

    #Generate the IDs for the device
    deviceId = random.sample(range(1111111,2222222),25000)
    
    #Generate the dataframe of the device table to be inserted in SQL server
    for i in range(len(deviceId)):
        l.append([deviceId[i],random.choice(deviceType),random.choice(firmwareVersion),datetime.now()-timedelta(days = 300+ random.randint(1,30)),'CA','1111'])
    deviceDf = pd.DataFrame(l,columns=columns)
    
   #TODO is the table exisit but it has less that 25000 records then truncate it first

    sql = '''INSERT INTO [dbo].[Device]
           ([DeviceId]
           ,[DeviceType]
           ,[FirmwareVersion]
           ,[DateInstalled]
           ,[Region]
           ,[OwnerId]) values (?,?,?,?,?,?)'''

    for index, row in deviceDf.iterrows():
        cursor.execute(sql, row['DeviceID'],row['DeviceType'],row['FirmwareVersion'],row['DateInstalled'],row['Region'],row['OwnerID'])
        cnxn.commit()

# finction to simulate the metrics using normal distribution 
def deviceOperate(deviceType):
    # construct sample rate based on the wifi bandwidth 
    samplerate = None
    dbm = round(random.gauss(-65,6),2)
    if (dbm < -65):
        sampleRate = 128
    elif ( dbm > -65 and dbm < -50):
        sampleRate = 64
    elif ( dbm > -50):
        sampleRate = 48

    # construct resolution  based on the wifi bandwidth 
    resolution = None
    if (dbm < -65):
        resolution = 1080
    elif ( dbm > -65 and dbm < -50):
        resolution = 720
    elif ( dbm > -50):
        resolution = 480

    data = {}
    
    deviceType = deviceType

    # if devicetype == Audio then 
    if (deviceType == 'Audio'):
        data['Metrics'] = {'temp': round(random.gauss(85,2),2), 'volume': round(random.gauss(50,5),2), 'wifi': dbm, 'samplerate':sampleRate} # maybe normal distribution
    else:
        data['Metrics'] = {'temp': round(random.gauss(85,2),2), 'volume': round(random.gauss(50,5),2), 'wifi': dbm, 'resolution':resolution} # maybe normal distribution
    return data
    #next Operation


# Function to send messages to Event Hub
def dispatchEventHub(config, data,partition="0"):
    logger = logging.getLogger("azure")

    # Address can be in either of these formats:
    # "amqps://<URL-encoded-SAS-policy>:<URL-encoded-SAS-key>@<namespace>.servicebus.windows.net/eventhub"
    # "amqps://<namespace>.servicebus.windows.net/<eventhub>"
    # SAS policy and key are not required if they are encoded in the URL

    

    if not config['ADDRESS']:
        raise ValueError("No EventHubs URL supplied.")

    # Create Event Hubs client
    client = EventHubClient(config['ADDRESS'], debug=False, username=config['USER'], password=config['KEY'])
    sender = client.add_sender(partition=partition)
    client.run()
    try:
        sender.send(EventData(data))
        #logger.info("EventHub: Stopped!")
        client.stop()
    except:
        raise
        client.stop()
    finally:
        client.stop()

#Build the device status dataframe to keep track of the device status
#TODO you need to persist this once the simulation ends so when you restart you can get the latest status
def buildDeviceStatusDF(cnxn):
    deviceIds=[]
    deviceTypes=[]

    cursor = cnxn.cursor()
    sql = '''Select DeviceId,DeviceType from Device'''
    cursor.execute(sql)

    row = cursor.fetchone()

    while row:
        #print(row[0])
        deviceIds.append(row[0])
        deviceTypes.append(row[1])
        row = cursor.fetchone()
    deviceStatusDF = pd.DataFrame(columns=['DeviceId','DeviceType','LastOperation','LODate'])
    deviceStatusDF['DeviceId']=deviceIds
    deviceStatusDF['DeviceType']=deviceTypes
    # impute all nans with empty string to make it easy to deal with
    deviceStatusDF.fillna("", inplace=True)
    return deviceStatusDF

#Get all the deviceIds from the database
def getDevice(cnxn):
    deviceIds=[]
    cursor = cnxn.cursor()
    sql = '''Select DeviceId from Device'''
    cursor.execute(sql)

    row = cursor.fetchone()

    while row:
        #print(row[0])
        deviceIds.append(row[0])
        row = cursor.fetchone()
    return deviceIds

# This constructs the messages and send it to event hub
#TODO there is a bug that for each device keeps starting from 300 days ago so you need to start from where the last date 
#was inserted int he deviceStatus DF and pick up from there. The time od operation for each device should go forward
def performOperation(cnxn, config,numberOfMessages = 2000, daysBack = 300):
    
    cursor = cnxn.cursor()
    deviceStatusDF = buildDeviceStatusDF(cursor)
    # time construction 
    startdate = datetime.now()-timedelta(days = daysBack)
    n=0
    while (n < numberOfMessages):
        print(n)
        data={}
        deviceId = random.choice(list(deviceStatusDF['DeviceId']))
        data['DeviceId']= deviceId
        data['DeviceType'] = deviceStatusDF.loc[deviceStatusDF['DeviceId']==deviceId,'DeviceType'].values[0]
        
        lastOperation = deviceStatusDF[deviceStatusDF['DeviceId']==deviceId]['LastOperation'].values[0]
        LODate = deviceStatusDF[deviceStatusDF['DeviceId']==deviceId]['LODate'].values[0]
        #print('DeviceID: ',deviceId,'\nLast Operation: ',lastOperation,'\nDate: ' ,LODate)
        devStatusList = []
        
        
        operation ={}
        
        
        if (not lastOperation) or (lastOperation=='Powered Off'):
            #TODO: the time should go forward here it always starts from the 300 days ago
            #perhaps the deviceStatus table needs to be peristed.( at the end afterh while loop)
            #get the last operationDate from deviceStatus in SQL and it its
            operationDate = datetime.now()-timedelta(days = 300)
            data['Operation'] = ['Powered On', operationDate.strftime('%Y-%m-%d %H:%M:%S')]
            deviceStatusDF.at[deviceStatusDF['DeviceId']==deviceId,'LastOperation']='Powered On'    
            deviceStatusDF.at[deviceStatusDF['DeviceId']==deviceId,'LODate']=operationDate.strftime('%Y-%m-%d %H:%M:%S')
            data['DeviceMetrics'] = deviceOperate(deviceStatusDF.loc[deviceStatusDF['DeviceId']==deviceId,'DeviceType'].values[0])
        elif ( lastOperation == 'Powered On'):
            opDate = pd.to_datetime(LODate)+ timedelta(minutes = random.randint(0,20))
            data['Operation'] = ['Recording',opDate.strftime('%Y-%m-%d %H:%M:%S') ]
            deviceStatusDF.at[deviceStatusDF['DeviceId']==deviceId,'LastOperation']='Recording'
            deviceStatusDF.at[deviceStatusDF['DeviceId']==deviceId,'LODate']= opDate.strftime('%Y-%m-%d %H:%M:%S')
            data['DeviceMetrics'] = deviceOperate(deviceStatusDF.loc[deviceStatusDF['DeviceId']==deviceId,'DeviceType'].values[0])
        elif ( lastOperation == 'Recording'):
            op =random.choice( ['Pause','Replay'])
            opDate = pd.to_datetime(LODate) + timedelta(minutes = random.randint(0,20))
            data['Operation'] = [op,opDate.strftime('%Y-%m-%d %H:%M:%S') ]
            deviceStatusDF.at[deviceStatusDF['DeviceId']==deviceId,'LastOperation']=op
            deviceStatusDF.at[deviceStatusDF['DeviceId']==deviceId,'LODate']=opDate.strftime('%Y-%m-%d %H:%M:%S')
            data['DeviceMetrics'] = deviceOperate(deviceStatusDF.loc[deviceStatusDF['DeviceId']==deviceId,'DeviceType'].values[0])
        elif ( (lastOperation == 'Replay') or (lastOperation == 'Pause')):
            op ='Powered Off'
            opDate = pd.to_datetime(LODate)+ timedelta(minutes = random.randint(0,20))
            data['Operation'] = [op,opDate.strftime('%Y-%m-%d %H:%M:%S') ]
            deviceStatusDF.at[deviceStatusDF['DeviceId']==deviceId,'LastOperation']= op
            deviceStatusDF.at[deviceStatusDF['DeviceId']==deviceId,'LODate']=opDate.strftime('%Y-%m-%d %H:%M:%S')
            data['DeviceMetrics'] = deviceOperate(deviceStatusDF.loc[deviceStatusDF['DeviceId']==deviceId,'DeviceType'].values[0])
        n +=1
        #print(data)
        dispatchEventHub(config,json.dumps(data)) 
        #dispatchEventHub(ADDRESS,USER,KEY,data,partition="0") maybe parameter to sleep with default

if __name__ == '__main__':

    config = getConfig()
    cnxn = connectToDB(config)
    performOperation(cnxn=cursor,config=config)    
