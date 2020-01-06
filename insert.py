import datetime
import sys
import time
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

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
    duplicates = 0
    while count < size:
        try:
            # try to bulk insert the batch
            coll.insert_many(batch[count:])
            count += size
        except BulkWriteError as err:
            code = err.details['writeErrors'][0]['code']
            if (code == 11000): # duplicate key
                duplicates = duplicates + 1
            elif (code == 16500): # 429
                time.sleep(0.01)
            # catch the 429 here and determine how many documents are left in the batch to insert
            # count how many were successfully inserted before retrying
            count = getDocCount(coll) - before + duplicates

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
    for item in items:
        # build the batch to upload
        batch.append(item)
        if (len(batch) == batchSize):
            start = datetime.datetime.now()
            uploadBatch(toCollection, batch)
            timeDiff = (datetime.datetime.now() - start).total_seconds()
            total += len(batch)
            print(f"{round(len(batch) / timeDiff, 2)} docs/sec, total: {total}")
            batch.clear()
    
    # if there is a batch left over that hasn't been uploaded, upload it
    if (len(batch) > 0):
        uploadBatch(toCollection, batch)
        total += len(batch)
    print(f"Complete. Total uploads: {total}")

# *** Hey guys! put your connection information here! ***
# *** This is just my own test db connection information and should be replaced! ***
# start the copy from one database to another
#fromConnStr = ""
toConnStr = ""
fromConnStr = toConnStr
toConnStr = "127.0.0.1:27017"

migrate(fromConnStr, "EnrichmentDocs", "NetbaseSummary_test",
        toConnStr, "coke-test", "NetbaseSummary_test",
        500)