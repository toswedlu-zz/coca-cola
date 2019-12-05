import datetime
import time
import sys
from pymongo import DeleteOne, MongoClient
from pymongo.errors import BulkWriteError, WriteError

# get the current count of documents from the given collection.
# this requests from cosmos and is subject to throughtput constraints as well.
# if the request fails, wait 10ms and try again.
def getDocCount(coll, filter):
    while True:
        try:
            return coll.count_documents(filter)
        except:
            time.sleep(0.01)

def delete(connStr, dbName, collName, filter):
    client = MongoClient(connStr)
    collection = client[dbName][collName]

    done = False
    total = getDocCount(collection, filter)
    while (not done):
        count = 0
        start = datetime.datetime.now()
        try:
            # try to delete as many documents as throughput allows, breaking
            # out of the loop only when all the documents that match the filter
            # are removed
            result = collection.delete_many(filter)
            if (result.deleted_count == 0 and getDocCount(collection, filter) == 0):
                done = True
        except:
            time.sleep(0.01)

        # calculate and print the docs/sec
        timeDiff = (datetime.datetime.now() - start).total_seconds()
        count = getDocCount(collection, filter)
        countDiff = total - count
        total = count
        print(f"{round(countDiff / timeDiff, 2)} docs/sec, total: {total}")
    print("Complete.")

# *** Hey guys! put your connection information here! ***
# *** This is just my own test db connection information and should be replaced! ***
# start the deletion of documents based on query
connStr = "mongodb://tom-mongo-test:F0pbGxwzUdV8nUWukkfBEN8J2xO28BjXTF94HegaV0S4a0yRNfJ68GRJXdn0c6kiDbbXEeLeIOzKVTrKZj5esg==@tom-mongo-test.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@tom-mongo-test@&retrywrites=false"
delete(connStr, "tom_test", "People_copy", {"Gender":0})