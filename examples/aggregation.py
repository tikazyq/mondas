#!/usr/bin/python
"""
Description: get top 10 cities with most population

Author: yeqing

"""

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