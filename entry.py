from __future__ import division

import flask
from pymongo import MongoClient # Database connector
from bson.objectid import ObjectId # For ObjectId to work
import json

import time
import datetime

import requests


app = flask.Flask(__name__)
client = MongoClient('localhost', 27017)    #Configure the connection to the database
db = client.tpmDB    #Select the database


@app.route('/orderGet', methods=["POST"])
def fetchOrder():
	ack = {"success": True}
	if flask.request.method == "POST":
		if flask.request.is_json:
			content = flask.request.get_json()	
			print content
			print "_________________________________________________"
			
			order_id = content['order_id']
			user_id = content['user_id']
			product_id = content['product_id']
			side = content['side']
			ask_price = content['ask_price']
			total_qty = content['total_qty']
			order_stamp = content['order_stamp']
			order_sate = content['state']
			fill = content['fill']
			ts = time.time()
			recv_stamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
			ack['success'] = True

	return flask.jsonify(ack)	

@app.route('/createTrade')
def createTrade():
    #Loading Order File
    orderFile = open('oBuffer.json')
    orderStr = orderFile.read()
    order = json.loads(orderStr)

    #Loading Fills File
    fillFile = open('fBuffer.json')
    fillStr = fillFile.read()
    fillArray = json.loads(fillStr)

    for fill in fillArray:
        trade1 = {
        "orderId" : order["orderId"],
        "clientId" : "CS",
        "bookId" : fill["bookId"],
        "fillId" : fill["fillId"],
        "side" :  order["side"],
        "qtySize" : fill["qtySize"],
        "price" : fill["price"],
        "exId" : fill["exId"],
        "productId" : order["productId"],
        "orderStamp" : order["orderStamp"],
        "exStamp" : fill["exStamp"],
        "tradeStamp" : "#",
        "counterParty" : fill["counterParty"],
        "commision" : "0",
        "state" : "Closed"
        }
        #Dump Trade1 to DataBase
        db.Trade.insert(trade1)

    	#Evaluating Position for Trade
        evaluatePosition(trade1, fill)

        exSide = "BUY"
        if(order["side"] == "BUY"):
            exSide = "SELL";

        trade2 = {
        "orderId" : order["orderId"],
        "clientId" : order["clientId"],
        "bookId" : fill["bookId"],
        "fillId" : fill["fillId"],
        "side" :  exSide,
        "qtySize" : fill["qtySize"],
        "price" : fill["price"],
        "exId" : fill["exId"],
        "productId" : order["productId"],
        "orderStamp" : order["orderStamp"],
        "exStamp" : fill["exStamp"],
        "tradeStamp" : "#",
        "counterParty" : fill["counterParty"],
        "commision" : "1",
        "state" : "Closed"
        }
        #Dump Trade2 to DataBase
        db.Trade.insert(trade2)

    return "Everything is Okay, Saket :)"



def evaluatePosition(trade, fill):
	#Position variables
    lRealisedPL = -1
    lUnrealisedPL = -1
    lNetPosition = -1
    lAvgPrice = -1
    lMarketPrice = -1

	#Taking MarketPrice from #Group2
    lMarketPrice = 97;

	#Updating Position Table for Trade
    cursor = db.Position.find_one({"bookId" : fill["bookId"], "productId" : trade["productId"]})

    fQtySize = int(fill["qtySize"])
    tradePrice = float(fill["price"])

    if(cursor == None):
        # netPosition
        if(trade["side"] == "BUY"):
            lNetPosition = fQtySize
        else:
            lNetPosition = -1 * fQtySize
        #realisedPL
        lRealisedPL = 0
        #avgPrice
        lAvgPrice = tradePrice

    else:
        cursor["netPosition"] = int(cursor["netPosition"])
        cursor["avgPrice"] = float(cursor["avgPrice"])

        if(trade["side"] == "BUY"):
            # netPosition
            lNetPosition = cursor["netPosition"] + fQtySize

            if(cursor["netPosition"] > 0):
                #realisedPL
                lRealisedPL = 0
                #avgPrice
                lAvgPrice = (cursor["avgPrice"] * cursor["netPosition"] + tradePrice * fQtySize) / (cursor["netPosition"] + fQtySize)

            elif(cursor["netPosition"] < 0):
                # netPosition
                if(cursor["netPosition"] + fQtySize < 0):
                    #avgPrice
                    lAvgPrice = cursor["avgPrice"]
                    #realisedPL
                    lRealisedPL = (tradePrice - cursor["avgPrice"]) * fQtySize

                elif(cursor["netPosition"] + fQtySize > 0):
                    #avgPrice
                    lAvgPrice = tradePrice
                    #realisedPL
                    lRealisedPL = (tradePrice - cursor["avgPrice"]) * cursor["netPosition"]

                else:
                    #avgPrice
                    lAvgPrice = 0
                    #realisedPL
                    lRealisedPL = (tradePrice - cursor["avgPrice"]) * fQtySize

            else:
                #avgPrice
                lAvgPrice = tradePrice
                #realisedPL
                lRealisedPL = 0
        else:
            # netPosition
            lNetPosition = cursor["netPosition"] - fQtySize
            if(cursor["netPosition"] < 0):
                #avgPrice
                lAvgPrice = (cursor["avgPrice"] * cursor["netPosition"] + tradePrice * fQtySize) / (cursor["netPosition"] + fQtySize)
                #realisedPL
                lRealisedPL = 0
            elif(cursor["netPosition"] > 0):
                if(cursor["netPosition"] - fQtySize > 0):
                    #avgPrice
                    lAvgPrice = cursor["avgPrice"]
                    #realisedPL
                    lRealisedPL = (tradePrice - cursor["avgPrice"]) * fQtySize

                elif(cursor["netPosition"] - fQtySize < 0):
                    #avgPrice
                    lAvgPrice = tradePrice
                    #realisedPL
                    lRealisedPL = (tradePrice - cursor["avgPrice"]) * cursor["netPosition"]

                else:
                    #avgPrice
                    lAvgPrice = 0
                    #realisedPL
                    lRealisedPL = (tradePrice - cursor["avgPrice"]) * fQtySize

            else:
                #avgPrice
                lAvgPrice = tradePrice
                #realisedPL
                lRealisedPL = 0

    db.Position.update({
    	"bookId" : fill["bookId"],
    	"productId" : trade["productId"]
    },
    {
    	"bookId" : fill["bookId"],
    	"productId" : trade["productId"],
    	"netPosition" : lNetPosition,
    	"realisedPL" : lRealisedPL,
    	"avgPrice" : lAvgPrice,
    	"unrealisedPL" : lUnrealisedPL,
    	"marketPrice" : lMarketPrice
    }, upsert = True)


if __name__ == '__main__':
	app.run(port=5001, debug=True)
