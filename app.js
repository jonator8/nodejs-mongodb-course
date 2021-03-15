const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');
const url = 'mongodb://localhost:27017';
const dbName = 'circulation';
const circulationRepo = require('./repos/circulationRepo');
const data = require('./circulation.json');

async function main() {
    const client = new MongoClient(url,{useUnifiedTopology:true});

    try {
        await client.connect();
        const results = await circulationRepo.loadData(data);
        assert.equal(data.length, results.insertedCount);

        const getData = await circulationRepo.get();
        assert.equal(data.length, getData.length);

        const filterData = await circulationRepo.get({Newspaper: getData[4].Newspaper});
        //console.log(filterData[0], getData[4]);
        assert.equal(filterData[0].Newspaper, getData[4].Newspaper);

        const limitData = await circulationRepo.get({}, 3);
        assert.equal(limitData.length, 3);

        const id = getData[4]._id.toString();
        const byId = await circulationRepo.getById(id);
        assert.equal(byId.Newspaper, getData[4].Newspaper);

        const newItem = {
            "Newspaper": "My paper",
            "Daily Circulation, 2004": 2,
            "Daily Circulation, 2013": 1,
            "Change in Daily Circulation, 2004-2013": 10,
            "Pulitzer Prize Winners and Finalists, 1990-2003": 0,
            "Pulitzer Prize Winners and Finalists, 2004-2014": 0,
            "Pulitzer Prize Winners and Finalists, 1990-2014": 0
        };
        const addItem = await circulationRepo.add(newItem);
        assert(addItem._id);
        const addItemQuery = await circulationRepo.getById(addItem._id);
        assert.deepEqual(addItemQuery, newItem);

        const updatedItem = await circulationRepo.update(addItem._id, {
            "Newspaper": "My new paper",
            "Daily Circulation, 2004": 2,
            "Daily Circulation, 2013": 1,
            "Change in Daily Circulation, 2004-2013": 10,
            "Pulitzer Prize Winners and Finalists, 1990-2003": 0,
            "Pulitzer Prize Winners and Finalists, 2004-2014": 0,
            "Pulitzer Prize Winners and Finalists, 1990-2014": 0
        });
        assert.equal(updatedItem.Newspaper, "My new paper");

        const newAddedItemQuery = await circulationRepo.getById(addItem._id);
        assert.equal(newAddedItemQuery.Newspaper, "My new paper");

        const removed = await circulationRepo.remove(addItem._id);
        assert(removed);
        const deletedItem = await circulationRepo.getById(addItem._id);
        assert.equal(deletedItem, null);

        const avgFinalists = await circulationRepo.averageFinalists();
        console.info("Average Finalists: " + avgFinalists);

        const avgByChange = await circulationRepo.averageFinalistsByChange();
        console.info(avgByChange);

    } catch (err) {
        console.log("error: "+err);
    } finally {
        const admin = client.db(dbName).admin();
        await client.db(dbName).dropDatabase();
        console.log(await admin.listDatabases());
        await client.close();
    }

}

main().catch(err => console.info("ERROR: " + err));
