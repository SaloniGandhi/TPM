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


    # cursor = db.Trade.find()
    # for document in cursor:
    #     print("Saket" - document)


    return "Everything is Okay, Saket :)"


if __name__ == '__main__':
    app.run()
