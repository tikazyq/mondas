#!/usr/bin/python
"""
Description: a simple query to zips collection, get all rows from NY state

Author: yeqing

"""

from mondas import Mongo

m = Mongo()

# m.set_connection('localhost')  # default 'localhost'
# m.set_db('test')  # default 'test'
m.set_col('zips')

m.add_query({'state': 'NY'})
m.add_project({'state': 1, 'pop': 1, 'city': 1})

m.run()

print(m.res[:10])