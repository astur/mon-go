var MongoClient = require('mongodb').MongoClient;
var mongoString = process.env.MONGOLAB_URI ||
    'mongodb://localhost:27017/test';

var finalResult = {
    nModified: 0,
    nUpserted: 0,
};

function updateFinalResult(result){
    finalResult.nModified += result.nModified;
    finalResult.nUpserted += result.nUpserted;
}

module.exports = function (keys, collectionName, limit, callback){
    MongoClient.connect(mongoString, function(err, db) {
        if(err){
            callback(err);
        }
        var k = {};
        keys.forEach(function(v){
            k[v] = 1;
        });
        var flats = db.collection(collectionName);
        flats.createIndex(k, {unique: true});
        var bulk = flats.initializeUnorderedBulkOp();
        var counter = 0;

        callback(null, function(doc){
            if (typeof doc === 'function') {
                bulk.execute()
                    .then(updateFinalResult)
                    .then(function(){
                        doc(finalResult);
                    });
            } else {
                var q = {};
                keys.forEach(function(v){
                    q[v] = doc[v];
                });
                bulk.find(q).upsert().replaceOne(doc);
                counter ++;
                if(counter % limit === 0){
                    bulk.execute()
                        .then(updateFinalResult);
                    bulk = flats.initializeUnorderedBulkOp();
                }
            }
        }, db);
    });
};