// https://docs.mongodb.com/manual/tutorial/write-scripts-for-the-mongo-shell/
db = db.getSiblingDB('sgds');
//collection = db.gdelt_20200101;
collection = db.gdelt_2020;

//print("Delete all documents with no lat or lon")
//collection.remove({
//    "$or": [
//        { "lat": { "$exists": false } },
//        { "lon": { "$exists": false } }
//    ]
//});

print("Iterate over all entries and add geometry");
collection.updateMany( {}, [
  { "$set": {
      "geometry": { 
        type: "Point",
        coordinates: ["$lon", "$lat"]
      }
    }
  }
])

print("Add 2dsphere index");
printjson( collection.createIndex( { "geometry": "2dsphere" } ) );
printjson( collection.ensureIndex( { "geometry": "2dsphere" } ) );

cursor = collection.find().limit(2);
while ( cursor.hasNext() ) {
   printjson( cursor.next() );
}
