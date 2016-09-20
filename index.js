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

        var save = (function(cName){

            if(/^-/.test(cName)){
                cName = cName.slice(1);
                db.dropCollection(cName);
            }

            var finalResult = {
                nModified: 0,
                nUpserted: 0,
            };

            function updateFinalResult(result){
                finalResult.nModified += result.nModified;
                finalResult.nUpserted += result.nUpserted;
            }

            var collection = db.collection(cName);
            collection.createIndex(k, {unique: true});
            var bulk = collection.initializeUnorderedBulkOp();
            var counter = 0;

            return function(doc){
                if (typeof doc === 'function') {
                    if(bulk.s.currentIndex > 0){
                        bulk.execute()
                            .then(updateFinalResult)
                            .then(function(){
                                doc(finalResult);
                            });
                    } else {
                        doc(finalResult);
                    }
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
                        bulk = collection.initializeUnorderedBulkOp();
                    }
                }
            };
        })(collectionName);

        callback(null, save, db);


    });
};