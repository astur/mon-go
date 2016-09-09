var MongoClient = require('mongodb').MongoClient;
var mongoString = process.env.MONGOLAB_URI ||
    'mongodb://localhost:27017/test';

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
            if (!doc) {
                try{
                    bulk.execute(function(){
                    });
                } catch(e) {
                }
            } else {
                var q = {};
                keys.forEach(function(v){
                    q[v] = doc[v];
                });
                bulk.find(q).upsert().replaceOne(doc);
                counter ++;
                if(counter % limit === 0){
                    bulk.execute();
                    bulk = flats.initializeUnorderedBulkOp();
                }
            }
        }, db);
    });
};