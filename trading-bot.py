# Import libraries
import argparse
import configparser
import logging
import signal
import sys
from datetime import date, datetime, timezone, timedelta
import time
import requests
from requests import Request, Session

import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
from pymongo import MongoClient
import certifi
from cryptocmd import CmcScraper
import base64
import json
import random
import websocket, ssl

# Global Constants

PROGNAME="trading_bot"
INPUT_TIMESTAMP_FORMAT = "%Y-%m-%d"
PROG_TIMESTAMP_FORMAT = "%Y-%m-%dT%H.%M.%S.%f%Z"
SLACK_HEADER = {'Content-type' : 'application/json'}
NL_STR = '\n'


# Loggging Setup

def setup_logger(logfilename):
    logging.basicConfig(
        filename=logfilename,
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    logger = logging.getLogger()
    console = logging.StreamHandler()
    # console.setLevel(logging.ERROR)
    logger.addHandler(console)

    logging.info("Starting...")


# Signal handlers

def int_signal_handler(sig, frame):
    logging.info('Received SIGINT signal.')
    logging.info('Shutdown.')
    sys.exit(0)


def term_signal_handler(sig, frame):
    logging.info('Received SIGTERM signal.')
    logging.info('Shutdown.')
    sys.exit(0)


def kill_signal_handler(sig, frame):
    logging.info('Received SIGKILL signal.')
    logging.info('Shutdown.')
    sys.exit(0)


def local_timestamp():
        now = datetime.now()
        localTimeZone = now.astimezone().tzinfo
        localTimestamp = now.strftime(PROG_TIMESTAMP_FORMAT) + str(localTimeZone) + ", "
        return localTimestamp


# Orders collector

# get available instruments

def get_available_instruments(url):
    
    instruments_url = url + "/instruments"
    res_instruments = requests.get(url=instruments_url)
    instruments_json = res_instruments.json()
    instruments_df = pd.DataFrame(instruments_json)
    instruments = list(instruments_df['name'])
    
    return instruments

# get orders

def get_orders(aplo_base_url,bearer_token,mpid):
    orders_url = aplo_base_url + f"/participants/{mpid}/orders"

    headers = {'Authorization': 'Bearer %s' % bearer_token}

    res_orders = requests.get(url=orders_url, headers=headers)
    
    res_orders_json = res_orders.json()
    res_orders_json = res_orders_json.get("data")
    
    return res_orders_json

# store orders in mongodb

def store_orders(url,res_orders_json,mongodb_database,mongodb_collections):
    
    msg=""
    
    # Create a new client and connect to the server
    client = MongoClient(url)

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        msg += "Pinged your deployment. You successfully connected to MongoDB! "
        db = client.get_database(mongodb_database)

        # We define the collection we will store this data in,
        # and insert the data into the collection.
        db_orders = db.get_collection(mongodb_collections)
        inserted = db_orders.insert_many(res_orders_json)
        
        # Print a count of documents inserted.
        msg += str(len(inserted.inserted_ids)) + f" documents inserted into {mongodb_database} {mongodb_collections}"
        
    except Exception as e:
        msg += e
    
    return msg

# get real-time price

def get_ask_price():
    socket = f"wss://sheeldmatch.com:6443/?depth=10&symbols=BTC-USDT.SPOT@Binance/ADA-BTC.PERP@OKEx"

    # Create an empty list to store messages
    received_messages = []

    def on_message(ws, message):
    #     print("Received message:", message)
        received_messages.append(message)  # Store the message in the list
        if json.loads(message).get("type")=="Z":
            ws.close()
    def on_error(ws, error):
        print("Error:", error)

    def on_close(ws, close_status_code, close_msg):
        print("Closed with status code:", close_status_code, "and message:", close_msg)

    def on_open(ws):
        print("WebSocket connection opened")


    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(socket,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close,
                                on_open=on_open)

    ws.run_forever()
    
    ask_price=[json.loads(i).get("type") for i in received_messages].index("S")
    ask_price=received_messages[ask_price]
    ask_price=json.loads(ask_price).get("data")[1].get("price")
    
    return ask_price

# gennerate and send orders

def send_orders(i,date,instrument,side,volume,price,priority,sor_iso_url,headers):
    
    body = {
     "token": f"{date}000{i}",
     "instrument": "BTC-USDT.SPOT",
     "side": side,
     "volume": 0.1,
     "price": price,
     "priority": "price"
   }
    res_sor_iso = requests.post(sor_iso_url, headers=headers, json=body)
    
    print("order sent")


def main():
    """
       main: Get program parameters from .ini file and call functions.
       Usage:
          python aplo_orders_mapper.py aplo_orders_mapper.ini
    """

    # Program Parameters
    parser = argparse.ArgumentParser(description='Aplo Order Mapper')
    parser.add_argument("ini_file", help="Configuration file name.", type=str)

    args = parser.parse_args()

    print("ini_file="+args.ini_file)

    # Read Parameters from .ini file

    config = configparser.ConfigParser()
    config.read(args.ini_file)

    aplo_config = config['Aplo']
    aplo_base_url = aplo_config.get("aplo_base_url","")
    mpid=aplo_config.get("mpid","")
    base = aplo_base_url
    credentials=aplo_config.get("credentials","")
    encoded_credentials = base64.b64encode(credentials.encode("utf-8"))
    headers = {'Authorization': 'Basic %s' % str(encoded_credentials, "utf-8")}
    response = requests.get('%s/auth/signin' % base, headers=headers)
    bearer_token = json.loads(response.content)["authToken"]
    headers = {'Authorization': 'Bearer %s' % bearer_token}
    mpid=mpid.split(',')
    for item in mpid:
        sor_iso_url = aplo_base_url + f"/participants/{mpid}/orders/sor/iso"
    mongodb_url = aplo_config.get("mongodb_url","")
    slack_url = aplo_config.get("slack_url","")
    SLACK_HEADER = {'Content-type' : 'application/json'}
    slack_msg = ""
    
    output_filename_root=aplo_config.get("output_filename_root","")
    mongodb_database=aplo_config.get("mongodb_database","")
    
    mongodb_hist_collections=aplo_config.get("mongodb_hist_collections","")
    mongodb_mapped_collections=aplo_config.get("mongodb_mapped_collections","")
    


    # Get the Local Timezone Today's Date
    
    now = datetime.now()
    today = date.today()
    today_dtm = datetime(today.year, today.month, today.day)
    yesterday = today - timedelta(days=1)
    yesterday_dtm = datetime(yesterday.year, yesterday.month, yesterday.day)
    now_str = now.strftime(PROG_TIMESTAMP_FORMAT)
    today_str = today_dtm.strftime(PROG_TIMESTAMP_FORMAT)
    yesterday_str = yesterday_dtm.strftime(PROG_TIMESTAMP_FORMAT)

    print("Starting at=", now_str)

    # Setup Log and PID file names

    filename = output_filename_root+"_"+now_str

    logfilename = filename+".log"
    setup_logger(logfilename)

    logging.info("now="+now_str)
    logging.info("today_str="+today_str)
    logging.info("yesterday_str="+yesterday_str)

    # Log Parameters for Debugging
    
    logging.info("output_filename_root="+output_filename_root)
    logging.info("mongodb_connect_string="+mongodb_url)
    logging.info("mongodb_database="+mongodb_database)
    logging.info("aplo_connection="+sor_iso_url)
    logging.info("slack_url="+slack_url)

    # Input parameter checks


    if len(mongodb_database) == 0:
        logging.error("mongodb_database is not specified.")
        logging.error("Shutdown.")
        sys.exit(1)

    if len(sor_iso_url) == 0:
        logging.error("aplo_connection is not specified.")
        logging.error("Shutdown.")
        sys.exit(1)

    if len(mongodb_url) == 0:
        logging.error("mongodb_connection is not specified.")
        logging.error("Shutdown.")
        sys.exit(1)


    # Setup Signal handlers

    signal.signal(signal.SIGINT, int_signal_handler)
    signal.signal(signal.SIGTERM, term_signal_handler)

    slack_msg = ""
    if (slack_url):
        slack_msg = local_timestamp() + PROGNAME + " process for " + today_str +  " Started."

        response = requests.post(slack_url, json={'text' : slack_msg}, headers=SLACK_HEADER)
        logging.info("Slack Msg="+slack_msg)
        logging.info("Slack respone="+str(response))
    
    try:
        slack_msg = ""
        instruments = get_available_instruments(aplo_base_url)
#         start_time=time.time()
#         for i in range(30,35):
#             # Get the current date and time
#             current_datetime = datetime.now()

#             # Extract just the date part
#             current_date = current_datetime.date()

#             # Format date
#             format_date = str(current_date).replace("-","")

#             # Randomly pick instrument
#             random_instrument = random.choice(instruments)

#             # Randomly pick buy/sell
#             buy_sell = "buy"#random.choice(['buy','sell'])

#             volume = random.random()
#             ask_price = get_ask_price()
#             send_orders(i,format_date,random_instrument,buy_sell,volume,ask_price,"price",sor_iso_url,headers)
#         end_time=time.time()
#         order_generate_time=end_time-start_time

        logging.info('Getting accounts info')
        
        client = MongoClient(mongodb_url)

        # Send a ping to confirm a successful connection
        
        client.admin.command('ping')
        slack_msg += "Pinged your deployment. You successfully connected to MongoDB! "
        db = client.get_database(mongodb_database)

        ext_acct_map_collection = db.get_collection(mongodb_mapped_collections)
    
        trader_ls=list()
        for doc in ext_acct_map_collection.find({'ext_platform':'Aplo'}):
            trader_ls.append(doc['acct'])

        logging.info('Successfully retrieved external accounts info from mongodb')
        

        # insert hist orders
        orders=list()
        for item in trader_ls:
            tmp = get_orders(aplo_base_url,bearer_token,item)
            orders.extend(tmp)
            
        msg = store_orders(mongodb_url,orders,mongodb_database,mongodb_hist_collections)
        slack_msg += msg + NL_STR
        
        # insert mapped orders
        hist=pd.DataFrame(orders)
        mapped_orders=pd.DataFrame()
        trade_info={"SMRA":{ "trader": "Samara", "ext_platform": "Aplo", "acct": "SMRA"},
            "SMRT":{ "trader": "BlueShift", "ext_platform": "Aplo", "acct": "SMRT"},
            "SMRF":{ "trader": "Grandline", "ext_platform": "Aplo", "acct": "SMRF"},
            "SMTH":{ "trader": "Mudrex", "ext_platform": "Aplo", "acct": "SMTH"}}
        
        mapped_orders['date']=hist['day']
        mapped_orders['trader']=hist['mpid'].apply(lambda x:trade_info.get(x).get('trader'))
        mapped_orders['ext_platform']=hist['mpid'].apply(lambda x:trade_info.get(x).get('ext_platform'))

        mapped_orders['type']='TRADE'
        mapped_orders['ext_account']=hist['mpid']

        mapped_orders['ext_order_id']=hist['token']
        mapped_orders['ext_symbol']=hist['instrument']
        # mapped_orders['symbol']=
        mapped_orders['asset_type']=hist['classification']
        mapped_orders['venue']=hist['venue']
        mapped_orders['side']=hist['side']
        mapped_orders['price']=hist['requestedPrice']
        mapped_orders['quantity']=hist['requestedVolume']
        mapped_orders['tif']=hist['tif']
        mapped_orders['status']=hist['status']
        mapped_orders['amount']=hist['total']
        mapped_orders['close_timestamp']=hist['closingTime']
        mapped_orders['opening_timestamp']=hist['initialDay']
        mapped_orders['order_timestamp']=hist['time']
        mapped_orders['average_price']=hist['averagePrice']
        mapped_orders['executions_count']=hist['executionCount']
        mapped_orders['executions_volume']=hist['executedVolume']
        mapped_orders['quote_ccy']=hist['currencyId']

        insert_mapped_orders=mapped_orders.to_dict("records")
        msg = store_orders(mongodb_url,insert_mapped_orders,mongodb_database,mongodb_mapped_collections)
        
        slack_msg += msg + NL_STR
        

    except Exception as e:
        slack_msg = slack_msg + " Error: {} ".format(e)
        logging.error(slack_msg)

    finally:
        if (slack_url):
            response = requests.post(slack_url, json={'text' : slack_msg}, headers=SLACK_HEADER)
            logging.info("Slack Msg="+slack_msg)
            logging.info("Slack respone="+str(response))

    if (slack_url):
        slack_msg = local_timestamp() + PROGNAME + " process for " + today_str +  " Terminated."

        response = requests.post(slack_url, json={'text' : slack_msg}, headers=SLACK_HEADER)
        logging.info("Slack Msg="+slack_msg)
        logging.info("Slack respone="+str(response))

        
if __name__ == "__main__":
    main()

