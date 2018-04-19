from flask import Flask
from pymongo import MongoClient # Database connector
from bson.objectid import ObjectId # For ObjectId to work
import json

app = Flask(__name__)
client = MongoClient('localhost', 27017)    #Configure the connection to the database
db = client.tpmDB    #Select the database


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

    #
    # cursor = db.Trade.find_one({"bookId" : "111408029", '_id': ObjectId('5ad784f08c61a33aeb4c4270')})
    # # for document in cursor:
    # print(cursor["productId"])


    return "Everything is Okay, Saket :)"



def evaluatePosition(trade, fill):
	#Position variables
    lRealisedPL = 0
    lUnrealisedPL = 0
    lNetPosition = 0
    lAvgPrice = 0
    lMarketPrice = 0

	#Taking MarketPrice from #Group2
    lMarketPrice = 97;

	#Updating Position Table for Trade
    cursor = db.Position.find_one({"bookId" : fill["bookId"], "productId" : trade["productId"]})

    fQtySize = int(fill["qtySize"])

    if(cursor == None):
        lAvgPrice = lMarketPrice
        if(trade["side"] == "BUY"):
            lNetPosition = fill["qtySize"]
        else:
            lNetPosition = -1 * fQtySize
    else:

        cursor["netPosition"] = int(cursor["netPosition"])
        cursor["avgPrice"] = int(cursor["avgPrice"])

        if(trade["side"] == "BUY"):
            lNetPosition = cursor["netPosition"] + int(fill["qtySize"])
            if(cursor["netPosition"] > 0):
                lRealisedPL = 0
                lAvgPrice = (cursor["avgPrice"] * cursor["netPosition"] + lMarketPrice * fQtySize) / (cursor["netPosition"] + fQtySize)

            elif(cursor["netPosition"] < 0):
                if(cursor["netPosition"] + fQtySize < 0):
                    lAvgPrice = cursor["avgPrice"]
                    lRealisedPL = (lMarketPrice - cursor["avgPrice"]) * fQtySize

                elif(cursor["netPosition"] + fQtySize > 0):
                    lAvgPrice = lMarketPrice
                    lRealisedPL = (lMarketPrice - cursor["avgPrice"]) * cursor["netPosition"]

                else:
                    lAvgPrice = 0
                    lRealisedPL = (lMarketPrice - cursor["avgPrice"]) * fQtySize

            else:
                lAvgPrice = lMarketPrice
                lRealisedPL = 0
        else:
            lNetPosition = cursor["netPosition"] - fill["qtySize"]
            if(cursor["netPosition"] < 0):
                lAvgPrice = (cursor["avgPrice"] * cursor["netPosition"] + lMarketPrice * fQtySize) / (cursor["netPosition"] + fQtySize)
                lRealisedPL = 0
            elif(cursor["netPosition"] > 0):
                if(cursor["netPosition"] - fQtySize > 0):
                    lAvgPrice = cursor["avgPrice"]
                    lRealisedPL = (lMarketPrice - cursor["avgPrice"]) * fQtySize

                elif(cursor["netPosition"] - fQtySize < 0):
                    lAvgPrice = lMarketPrice
                    lRealisedPL = (lMarketPrice - cursor["avgPrice"]) * cursor["netPosition"]

                else:
                    lAvgPrice = 0
                    lRealisedPL = (lMarketPrice - cursor["avgPrice"]) * fQtySize

            else:
                lAvgPrice = lMarketPrice
                lRealisedPL = 0


    db.Position.update({
    	"bookId" : fill["bookId"],
    	"productId" : trade["productId"]
    },
    {
    	"bookId" : fill["bookId"],
    	"productId" : trade["productId"],
    	"realisedPL" : lRealisedPL,
    	"unrealisedPL" : lUnrealisedPL,
    	"netPosition" : lNetPosition,
    	"avgPrice" : lAvgPrice,
    	"marketPrice" : lMarketPrice
    }, upsert = True)



if __name__ == '__main__':
    app.run()
