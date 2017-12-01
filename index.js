const MongoClient = require('mongodb').MongoClient;
const mongoString = process.env.MONGOLAB_URI ||
    'mongodb://localhost:27017/test';

module.exports = function(keys, collectionName, limit, callback){
    MongoClient.connect(mongoString, (err, db) => {
        if(err){
            return callback(err);
        }
        const k = {};
        keys.forEach(v => {
            k[v] = 1;
        });

        const save = (function(cName){
            if(/^-/.test(cName)){
                cName = cName.slice(1);
                db.dropCollection(cName);
            }

            const finalResult = {
                nModified: 0,
                nUpserted: 0,
            };

            function updateFinalResult(result){
                finalResult.nModified += result.nModified;
                finalResult.nUpserted += result.nUpserted;
            }

            const collection = db.collection(cName);
            collection.createIndex(k, {unique: true});
            let bulk = collection.initializeUnorderedBulkOp();
            let counter = 0;

            return function(doc){
                if(typeof doc === 'function'){
                    if(bulk.s.currentIndex > 0){
                        bulk.execute()
                            .then(updateFinalResult)
                            .then(() => {
                                doc(finalResult);
                            });
                    } else {
                        doc(finalResult);
                    }
                } else {
                    const q = {};
                    keys.forEach(v => {
                        q[v] = doc[v];
                    });
                    bulk.find(q).upsert().updateOne({$set: doc});
                    counter++;
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
