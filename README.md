mondas
======
mondas allows you to easily query, transform and aggregate JSON formatted data from MongoDB into structured, tabular data, which would save much of your time on cleansing, wraggling and munging those data.

Prerequisites
======
pandas: https://pypi.python.org/pypi/pandas#downloads

pymongo: http://api.mongodb.org/python/current/installation.html

mongodb: http://www.mongodb.org/downloads

Examples
======
The examples below will work on zip code data set provided by mongodb.org. Please follow the guide to import into mongodb.

Link: http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set/

A simple query to the database. 
```python
from mondas import Mongo

m = Mongo()

# m.set_connection('localhost')  # default 'localhost'
# m.set_db('test')  # default 'test'
m.set_col('zips')

m.add_query({'state': 'NY'})
m.add_project({'state': 1, 'pop': 1, 'city': 1})

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

mongo shell equavalent
```javascript
db.zips.find({state: 'NY'}, {state:1, pop:1, city:1});
```

Aggregation example: find top 10 cities with most population
```python
from mondas import AggregationFramework

ag = AggregationFramework()

# ag.set_connection('localhost')  # default 'localhost'
# ag.set_db('test')  # default 'test'
ag.set_col('zips')

ag.add_group({
    '_id': {
        'city': '$city',
        'state': '$state'
    },
    'pop': {'$sum': '$pop'}
})
ag.add_sort({'pop': -1})
ag.add_limit(10)

ag.run()

print(ag.res)

           city state      pop
0       CHICAGO    IL  2452177
1      BROOKLYN    NY  2300504
2   LOS ANGELES    CA  2102295
3       HOUSTON    TX  2095918
4  PHILADELPHIA    PA  1610956
5      NEW YORK    NY  1476790
6         BRONX    NY  1209548
7     SAN DIEGO    CA  1049298
8       DETROIT    MI   963243
9        DALLAS    TX   940191
```
mongo shell equivalent
```javascript
var res = db.zips.aggregate(
{
    $group: {
        _id: {
            city: '$city',
            city: '$state'
        },
        pop: {$sum: '$pop'}
    }
},
{
    $sort: {pop: -1}
},
{
    $limit: 10
})['result'];

printjson(res);
```
