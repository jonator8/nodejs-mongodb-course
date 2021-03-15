const {MongoClient, ObjectID} = require('mongodb');

function circulationRepo(){
    const url = 'mongodb://localhost:27017';
    const dbName = 'circulation';
    const collection = 'newspapers';

    function getById(id) {
        return new Promise(async (resolve, reject) => {
            const client = new MongoClient(url,{useUnifiedTopology:true});
            try {
                await client.connect();
                const db = client.db(dbName);
                let item  = await db.collection(collection).findOne({_id: ObjectID(id)});
                resolve(item);
            } catch (err) {
                reject(err);
            } finally {
                await client.close();
            }
        });
    }

    function add(item){
        return new Promise(async (resolve, reject) => {
            const client = new MongoClient(url,{useUnifiedTopology:true});
            try {
                await client.connect();
                const db = client.db(dbName);
                const addedItem  = await db.collection(collection).insertOne(item);
                resolve(addedItem.ops[0]);
            } catch (err) {
                reject(err);
            } finally {
                await client.close();
            }
        });
    }

    function averageFinalists(){
        return new Promise(async (resolve, reject) => {
            const client = new MongoClient(url,{useUnifiedTopology:true});
            try {
                await client.connect();
                const db = client.db(dbName);
                const average = await db.collection(collection)
                    .aggregate([{
                        $group:
                        {
                            _id: null,
                            avgFinalists: {$avg:"$Pulitzer Prize Winners and Finalists, 1990-2014"}
                        }
                    }]).toArray();
                resolve(average[0].avgFinalists);
            } catch (err) {
                reject(err);
            } finally {
                await client.close();
            }
        });
    }

    function averageFinalistsByChange(){
        return new Promise(async (resolve, reject) => {
            const client = new MongoClient(url,{useUnifiedTopology:true});
            try {
                await client.connect();
                const db = client.db(dbName);
                const average = await db.collection(collection)
                    .aggregate([
                        {
                            $project: {
                                "Newspaper": 1,
                                "Pulitzer Prize Winners and Finalists, 1990-2014": 1,
                                "Change in Daily Circulation, 2004-2013": 1,
                                overallChange: {
                                    $cond: {if: {$gte: ["$Change in Daily Circulation, 2004-2013", 0]}, then: "positive", else: "negative"}
                                }
                            }
                        },
                        {
                            $group:
                                {
                                    _id: "$overallChange",
                                    avgFinalists: {$avg:"$Pulitzer Prize Winners and Finalists, 1990-2014"}
                                }
                        }
                    ]).toArray();
                resolve(average);
            } catch (err) {
                reject(err);
            } finally {
                await client.close();
            }
        });
    }

    function update(id, newItem){
        return new Promise(async (resolve, reject) => {
            const client = new MongoClient(url,{useUnifiedTopology:true});
            try {
                await client.connect();
                const db = client.db(dbName);
                const updatedItem  = await db.collection(collection)
                    .findOneAndReplace({_id: ObjectID(id)}, newItem, {returnOriginal:false});
                resolve(updatedItem.value);
            } catch (err) {
                reject(err);
            } finally {
                await client.close();
            }
        });
    }

    function get(query, limit) {
        return new Promise(async (resolve, reject) => {
            const client = new MongoClient(url,{useUnifiedTopology:true});
            try {
                await client.connect();
                const db = client.db(dbName);
                let items  = db.collection(collection).find(query);
                if(limit > 0) {
                    items = items.limit(limit);
                }
                resolve(await items.toArray());
            } catch (err) {
                reject(err);
            } finally {
                await client.close();
            }
        });
    }

    function loadData(data){
        return new Promise(async (resolve, reject) => {
            const client = new MongoClient(url,{useUnifiedTopology:true});
            try {
                await client.connect();
                const db = client.db(dbName);
                resolve(await db.collection(collection).insertMany(data));
            } catch (err) {
                reject(err);
            } finally {
                await client.close();
            }
        })
    }

    function remove(id) {
        return new Promise(async (resolve, reject) => {
            const client = new MongoClient(url,{useUnifiedTopology:true});
            try {
                await client.connect();
                const db = client.db(dbName);
                let removed = await db.collection(collection).deleteOne({_id: ObjectID(id)});
                resolve(removed.deletedCount === 1);
            } catch (err) {
                reject(err);
            } finally {
                await client.close();
            }
        });
    }

    return {loadData, get, getById, add, update, remove, averageFinalists, averageFinalistsByChange};
}

module.exports = circulationRepo();
