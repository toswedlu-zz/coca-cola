import datetime
import sys
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

# upload a batch of documents to the given collection.
# if throughput has been exceeded, the count of successfully inserted documents
# is determined, and the bulk insert is retried with the remainder of the batch
def uploadBatch(coll, batch):
    count = 0
    size = len(batch)
    before = getDocCount(coll)
    while count < size:
        try:
            # try to bulk insert the batch
            coll.insert_many(batch[count:])
            count += size
        except:
            # catch the 429 here and determine how many documents are left in the batch to insert
            # count how many were successfully inserted before retrying
            count = getDocCount(coll) - before

# insert all documents from one db into another in batches.
def migrate(fromConnStr, fromDbName, fromCollName, 
            toConnStr, toDbName, toCollName,
            batchSize):
    fromClient = MongoClient(fromConnStr)
    fromCollection = fromClient[fromDbName][fromCollName]
    toClient = MongoClient(toConnStr)
    toCollection = toClient[toDbName][toCollName]

    batch = []
    total = 0
    items = fromCollection.find({})
    start = datetime.datetime.now()
    for item in items:
        # build the batch to upload
        batch.append(item)
        if (len(batch) == batchSize):
            uploadBatch(toCollection, batch)
            # print the docs/sec average after every batch
            total += len(batch)
            diff = (datetime.datetime.now() - start).total_seconds()
            print(f"{round(total / diff, 2)} docs/sec, total: {total}")
            # clear the batch to create a new batch
            batch.clear()
    
    # if there is a batch left over that hasn't been uploaded, upload it
    if (len(batch) > 0):
        uploadBatch(toCollection, batch)
        total += len(batch)
    print(f"Complete. Total uploads: {total}")

# *** Hey guys! put your connection information here! ***
# *** This is just my own test db connection information and should be replaced! ***
# start the copy from one database to another
connStr = "mongodb://tom-mongo-test:F0pbGxwzUdV8nUWukkfBEN8J2xO28BjXTF94HegaV0S4a0yRNfJ68GRJXdn0c6kiDbbXEeLeIOzKVTrKZj5esg==@tom-mongo-test.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@tom-mongo-test@&retrywrites=false"
migrate(connStr, "tom_test", "People",
        connStr, "tom_test", "People_copy",
        500)