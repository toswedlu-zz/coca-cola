import datetime
import logging
import os
import sys

import azure.functions as func
from pymongo import database, MongoClient
from pymongo.errors import DuplicateKeyError

# Determines if the given cursor object contains documents or a valid cursor ID.
def hasChanges(result: object) -> bool:
    cursor = result['cursor']
    return (cursor['id'] != 0 and ('nextBatch' not in cursor or len(cursor['nextBatch']) > 0))

# Gets the initial cursor for the change feed for a given start timestamp.
def getChanges(collName: str, db: database.Database, since: datetime.datetime) -> object:
    logging.info(f"Checking for changes since: {since}")
    pipeline = [
        { "$changeStream": { "fullDocument": "updateLookup" } },
        { "$match": { "operationType": { "$in": ["insert", "update", "replace"] } } },
        { "$project": { "_id": 1, "fullDocument": 1, "ns": 1, "documentKey": 1 } }
    ]
    return db.command({ "aggregate": collName, "pipeline": pipeline, "startAtOperationTime": since })

# Gets more changes from the given cursor in the change feed.
def getMoreChanges(collName: str, db: database.Database, result: object) -> object:
    return db.command({ "getMore": result['cursor']['id'], "collection": collName })

# Gets the current 'since' value as stored in Cosmos or uses
def getSince(db: database.Database, sinceCollName: str) -> datetime.datetime:
    # Get the default 'since' value.
    retval = os.environ["Since"] or '2019-12-01'

    # Check Cosmos for an existing value from a previous function run.
    if (sinceCollName in db.collection_names()):
        result = db[sinceCollName].find({"_id": 0})
        if (result.count() > 0):
            retval = result[0]['since']
    return datetime.datetime.strptime(retval, '%Y-%m-%d %H:%M:%S')

# Sets the given 'since' value into Cosmos
def setSince(db: database.Database, sinceCollName: str, since: datetime.datetime) -> None:
    sinceText = since.strftime('%Y-%m-%d %H:%M:%S')
    db[sinceCollName].update_one({"_id": 0}, {"$set": {"since": sinceText}}, True)

def main(mytimer: func.TimerRequest) -> None:
    monitoredCollName = os.environ["MonitoredCollName"]
    viewCollName = os.environ["ViewCollName"]
    sinceCollName = f"{viewCollName}_since"
    client = MongoClient(os.environ["CosmosConnStr"])
    database = client[os.environ["CosmosDbName"]]

    newSince = datetime.datetime.utcnow()
    since = getSince(database, sinceCollName)
    result = getChanges(monitoredCollName, database, since)
    while (hasChanges(result)):
        result = getMoreChanges(monitoredCollName, database, result)
        for change in result['cursor']['nextBatch']:
            ts = change['fullDocument']['ts']
            aggregation = change['fullDocument']['aggregation']
            doc = { "_id": f"{aggregation}|{ts}", "aggregation": aggregation, "ts": ts}
            try:
                database[viewCollName].insert_one(doc)
            except DuplicateKeyError:
                # Ignore duplicate key errors.
                pass
    setSince(database, sinceCollName, newSince)