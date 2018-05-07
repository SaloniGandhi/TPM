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

order = {}
fill = {}


@app.route('/orderGet', methods=["POST"])
def fetchOrder():
	ack = {"success": False}
	if flask.request.method == "POST":
		if flask.request.is_json:
			content = flask.request.get_json()
			print content
			print "_________________________________________________"

			order["orderId"] = str(content['order_id'])
			order["clientId"] = str(content['user_id'])
			order["productId"] = str(content['product_id'])
			order["side"] = str(content['side'])
			order["askedPrice"] = str(content['ask_price'])
			order["size"] = str(content['total_qty'])
			order["orderStamp"] = str(content['order_stamp'])
			order["state"] = str(content['state'])
			fill["exStamp"] = str(content['fill']["exchange_stamp"])
			fill["qtySize"] = str(content['fill']["qtydone"])
			fill["price"] = str(content['fill']["price"])
			fill["exId"] = str(content['fill']["exchange_id"])
			fill["bookId"] = "111408029" #account property

			print order
			print fill
			ts = time.time()
			recv_stamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
			ack['success'] = True

	return flask.jsonify(ack)

@app.route('/createTrade')
def createTrade():
	#Loading Order File
	# orderFile = open('oBuffer2.json')
	# orderStr = orderFile.read()
	# order = json.loads(orderStr)
	#
	# #Loading Fills File
	# fillFile = open('fBuffer2.json')
	# fillStr = fillFile.read()
	# fillArray = json.loads(fillStr)

	# for fill in fillArray:
	trade1 = {
		"orderId" : order["orderId"],
		"clientId" : "CS",
		"bookId" : fill["bookId"],
		"side" :  order["side"],
		"qtySize" : fill["qtySize"],
		"price" : fill["price"],
		"exId" : fill["exId"],
		"productId" : order["productId"],
		"orderStamp" : order["orderStamp"],
		"exStamp" : fill["exStamp"],
		"tradeStamp" : "#",
		"counterParty" : "#" , #fill["counterParty"],
		"commision" : "0",
		"state" : "Closed"
	}
	#Dump Trade1 to DataBase
	db.Trade.insert(trade1)

	#Evaluating Position for Trade
	evaluatePosition(trade1, fill)

	exSide = "0"
	if(order["side"] == "0"):
	    exSide = "1";

	print order["clientId"]
	if(order["clientId"] != "CS"):
		print "hellp"
		trade2 = {
	    	"orderId" : order["orderId"],
	    	"clientId" : order["clientId"],
	    	"bookId" : fill["bookId"],
	    	"side" :  exSide,
	    	"qtySize" : fill["qtySize"],
	    	"price" : fill["price"],
	    	"exId" : fill["exId"],
	    	"productId" : order["productId"],
	    	"orderStamp" : order["orderStamp"],
	    	"exStamp" : fill["exStamp"],
	    	"tradeStamp" : "#",
	    	"counterParty" : "#" , #fill["counterParty"],
	    	"commision" : "1",
	    	"state" : "Closed"
	    }
	    #Dump Trade2 to DataBase
		db.Trade.insert(trade2)

	return "OK"



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
	    if(trade["side"] == "0"):
	        lNetPosition = fQtySize
	    else:
	        lNetPosition = -1 * fQtySize
	    #realisedPL
	    lRealisedPL = 0
	    #avgPrice
		lunrealisedPL = fQtySize * lMarketPrice
	    lAvgPrice = tradePrice

	else:
	    cursor["netPosition"] = int(cursor["netPosition"])
	    cursor["avgPrice"] = float(cursor["avgPrice"])

	    if(trade["side"] == "0"):
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
	            lAvgPrice = (cursor["avgPrice"] * cursor["netPosition"] + tradePrice * fQtySize) / (cursor["netPosition"] - fQtySize)
	            #realisedPL
	            lRealisedPL = 0
	        elif(cursor["netPosition"] > 0):
	            if(cursor["netPosition"] - fQtySize > 0):
	                #avgPrice
	                lAvgPrice = cursor["avgPrice"]
	                #realisedPL
	                lRealisedPL = (tradePrice - cursor["avgPrice"]) * fQtySize

	            elif(cursor["netPosition"] - fQtySize2 < 0):
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
		lunrealisedPL = cursor["netPosition"] * marketPrice
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

	return 1

if __name__ == '__main__':
	app.run(port=5001, debug=True)
