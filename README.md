mondas
======

MongoDB is a popular NoSQL database which support fast write/read. However, it does not support JOIN, which will be a pain when most of your work requires querying MongoDB. Furthermore, MongoDB's JSON formatted, unstructured data model is not ideal for data analysis.

mondas (pymongo + pandas) allows you to easily query, transform and aggregate JSON formatted data from MongoDB into structured, tabular data, which would save much of your time on cleansing, wraggling and munging those data.

Downloads
======
pandas: https://pypi.python.org/pypi/pandas#downloads

pymongo: http://api.mongodb.org/python/current/installation.html

MongoDB: http://www.mongodb.org/downloads

Examples
======
A simple query to the database. 
```python
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
```javascript
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
