from futu import *
from apscheduler.schedulers.background import BlockingScheduler
import pymongo
import json
import time
from datetime import date, datetime

###-----Status Code-----

#    R : Data received
#    XA : API call error
#    S : Session skipped

# ---Calling the Broker API in every X seconds---
samplingRate = 60

# ---List of target stocks--- 
stockList = []

# ---DB table----
tableName = 'DistFlow'

# ---Class of trading session---
class sessionTime:

    def __init__(self, st_h, st_m, st_s, ed_h, ed_m, ed_s):

        self.st_h = st_h
        self.st_m = st_m
        self.st_s = st_s
        self.ed_h = ed_h
        self.ed_m = ed_m
        self.ed_s = ed_s
        self.today = datetime.today().strftime('%Y-%m-%d')

    def sessionStart(self):
        return self.today + " " + self.st_h + ":" + self.st_m + ":" + self.st_s
    
    def sessionEnd(self):
        return self.today + " " + self.ed_h + ":" + self.ed_m + ":" + self.ed_s


# ---Formation of log Message---
def logMessage(client, status, message, tableName):

    logMessage = { 'Time' : datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'Status': status , 'Message': message}
    client[tableName]['Log'].insert_one(logMessage)
    print(logMessage)

# ---Scraping action---
def distFlow(quote_ctx, client, endHour):

    global stockList, tableName

    for i in stockList:

        ret, data = quote_ctx.get_capital_distribution(i)
        
        if ret == RET_OK:
            
            data = json.loads(data.to_json(orient='records'))
            client[tableName][i].insert_many(data)
            
            logMessage(client = client, status = 'R', message = 'Distribution Flow Recevied - ' + i, tableName = tableName)

        else:

            logMessage(client = client, status = 'XA', message = 'API get_capital_distribution() error - ' + i, tableName = tableName)
    
    # If the trading session is ended, close the DB and API connections
    if datetime.now().hour == int(endHour):

        session = 'AM' if endHour == '12' else 'PM'

        logMessage(client = client, status = 'R', message = datetime.today().strftime('%Y-%m-%d') + ' ' + session + ' Is done', tableName = tableName)

        client.close()
        quote_ctx.close()

    
def driverThread(mainScheduler, session):

    # ---DB connection--- 
    client = pymongo.MongoClient('<DB address>')

    # ---API connection---
    quote_ctx = OpenQuoteContext(host = '<IP address>', port = '<Port>')
    
    global tableName, samplingRate

    today = session.today

    ret, data = quote_ctx.request_trading_days(TradeDateMarket.HK, start = today, end = today)
    
    # ---If the API call is successful,
    # Check whether today is trading day---
    if ret == RET_OK:

        # ---Empty dataframe = No Trading today---
        if not data:

            logMessage(client = client, status = 'S', message = today + ' Is Holiday ! - Skip the Whole Day', tableName = tableName)

            client.close()
            quote_ctx.close()

            return None

        # ---Determine the today's trading session--- 
        # Incoming dataframe is a list of dict
        # need to unpack it to list
        tradingSession = list(data[0].values())[-1]
        currHour = datetime.now().hour
        
        # --Skip the afternoon session if today only has morning session---
        if tradingSession == "MORNING" and currHour > 12:
        
            logMessage(client = client, status = 'S', message = today + ' Only Has Morning Session ! - Skip the Afternoon Session', tableName = tableName)

            client.close()
            quote_ctx.close()

            return None

        # ---Start to scrape the data---
        mainScheduler.add_job(distFlow, trigger = 'interval', args = [quote_ctx, client, session.ed_h], seconds = samplingRate, start_date = session.sessionStart(), end_date = session.sessionEnd() )
    

    # ---If the API call is unsuccessful, 
    # close the API and DB connections---
    else:

        logMessage(client = client, status = 'XA', message = 'API request_trading_days() error ', tableName = tableName)

        client.close()
        quote_ctx.close()


if __name__ == "__main__":

    # ---Read the list of stocks in .txt---  
    with open('StockList.txt') as f:
        stockList = [line.rstrip() for line in f]

    # ---Python Scheduler--- 
    mainScheduler = BlockingScheduler()

    # ---Morning Session---
    st_h, st_m, st_s = '09', '30', '00'
    ed_h, ed_m, ed_s = '12', '30', '00'
    moringSession = sessionTime(st_h, st_m, st_s, ed_h, ed_m, ed_s)

    # Run the driver function a bit earlier than scraping
    mainScheduler.add_job(driverThread, trigger = 'cron', args = [mainScheduler, moringSession], day_of_week = 'mon-fri', hour = "09", minute = "29", second = '50')
  
    # ---Afternoon Session---
    st_h, st_m, st_s = '13', '00', '00'
    ed_h, ed_m, ed_s = '16', '00', '00'
    afternoonSession = sessionTime(st_h, st_m, st_s, ed_h, ed_m, ed_s)

    mainScheduler.add_job(driverThread, trigger = 'cron', args = [mainScheduler, afternoonSession], day_of_week = 'mon-fri', hour = "12", minute = "59", second = '50')

    mainScheduler.start()
    
