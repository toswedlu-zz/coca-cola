from pymongo import collection, MongoClient

def createDocList(coll: collection.Collection) -> []:
    retval = []
    data = coll.aggregate([{ "$group": { "_id": "$aggregation", "ts": { "$addToSet": "$ts" }}}])
    for aggregation in data:
        agg = aggregation['_id']
        for thisTs in aggregation['ts']:
            doc = { "_id": f"{agg}|{thisTs}", "aggregation": agg, "ts": thisTs }
            retval.append(doc)
    return retval

def insertDocs(connStr: str, dbName: str, fromCollName: str, toCollName: str) -> None:
    client = MongoClient(connStr)
    fromColl = client[dbName][fromCollName]
    toColl = client[dbName][toCollName]
    docs = createDocList(fromColl)
    toColl.insert_many(docs)

connStr = ""
dbName = "EnrichmentDocs"
fromCollName = "NetbaseSummary"
toCollName = "NetbaseSummary_mv"
insertDocs(connStr, dbName, fromCollName, toCollName)