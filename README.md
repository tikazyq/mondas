mondas
======

MongoDB is a popular NoSQL database but not support JOIN. 
python pandas can be a way to reduce ease the pain. 

mondas allows you to easily retrieve and aggregate data from MongoDB, ultimately increase analysis productivity.

pandas: https://pypi.python.org/pypi/pandas#downloads
pymongo: http://api.mongodb.org/python/current/installation.html
MongoDB: http://www.mongodb.org/downloads

Example
======
A simple query the database. 
```
from mondas import Mongo

m = Mongo()
# m.set_connection('localhost')  # default 'localhost'
# m.set_db('test')  # default 'test'
m.set_col('zips')

m.add_query({'state': 'NY'})
m.add_project({'state':1, 'pop':1, 'city':1})
m.run()

print(m.res[:10])

     _id              city    pop state
0  06390    FISHERS ISLAND    329    NY
1  10001          NEW YORK  18913    NY
2  10002          NEW YORK  84143    NY
3  10003          NEW YORK  51224    NY
4  10004  GOVERNORS ISLAND   3593    NY
5  10005          NEW YORK    202    NY
6  10006          NEW YORK    119    NY
7  10007          NEW YORK   3374    NY
8  10009          NEW YORK  57426    NY
9  10010          NEW YORK  24907    NY
```

Mongo shell equavalent (when connecting to localhost and test database).
```
var data = [];
db.zips.find({state: 'NY'}, {state:1, pop:1, city:1}).forEach(function(x) {
     var row = {};
     for(var i in x) {
          if(x.hasOwnProperty(i) {
               row[i] = x[i];
          }
     }
     data.push(row);
});
print(data);
```
