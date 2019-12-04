import datetime
import time
from pymongo import MongoClient

# get the current count of documents from the given collection.
# this requests from cosmos and is subject to throughtput constraints as well.
# if the request fails, wait 10ms and try again.
def getDocCount(coll):
    while True:
        try:
            return coll.count_documents({})
        except:
            time.sleep(0.01)

def delete(connStr, dbName, collName, filter):
    client = MongoClient(connStr)
    collection = client[dbName][collName]

    done = False
    total = 0
    count = getDocCount(collection)
    start = datetime.datetime.now()
    zeroCount = 0
    while (not done):
        try:
            collection.delete_many(filter)
            done = True
        except:
            time.sleep(0.01)
        # to avoid too many failures resulting in zero deletions, 
        # break from the loop after 3 consecutive zero deletion iterations.
        total = count - getDocCount(collection)
        if (total == 0):
            zeroCount += 1
        if (zeroCount == 3):
            print("Canceling, zero-deletion threshold met")
            done = True
        diff = (datetime.datetime.now() - start).total_seconds()
        print(f"{round(total / diff, 2)} docs/sec, total: {total}")
    print("Complete.")


# *** Hey guys! put your connection information here! ***
# *** This is just my own test db connection information and should be replaced! ***
# start the deletion of documents based on query
connStr = "mongodb://tom-mongo-test:F0pbGxwzUdV8nUWukkfBEN8J2xO28BjXTF94HegaV0S4a0yRNfJ68GRJXdn0c6kiDbbXEeLeIOzKVTrKZj5esg==@tom-mongo-test.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@tom-mongo-test@&retrywrites=false"
delete(connStr, "tom_test", "People", {"Gender":0})