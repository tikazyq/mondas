# !/usr/bin/python
"""
Description:
MongoDB related classes.
'Mongo' -> communicate to mongodb
'AggregationFramework' -> handle aggregation framework and related processing

Author: yeqing

"""

from pymongo import MongoClient
from pandas import DataFrame as df
import re


def flatten_dict(d, separator='.', prefix=''):
    """
    @param d:
    @param separator:
    @param prefix:
    """
    return {prefix + separator + k if prefix else k: v
            for kk, vv in d.items()
            for k, v in flatten_dict(vv, separator, kk).items()
    } if isinstance(d, dict) else {prefix: d}


class Mongo:
    """
    create a Mongo instance
    default:
        client_name = "staging"
        db_name = "shopcade_marketing"
    """

    def __init__(self):
        self.connection = 'localhost:27017'
        self.db_name = "test"
        self.col_name = None
        self.db = self.client[self.db_name]
        self.col = None
        self.query = {}
        self.project = {}
        self.fields = []
        self.res = None
        self.columns = []
        self.quiet = False

    def set_connection(self, connection):
        self.connection = connection
        self.client = MongoClient(self.connection)
        self.db = self.client[self.db_name]
        if not self.quiet:
            print("connection is set to \"%s\"" % connection)

    def set_db(self, db_name=None):
        if db_name is None:
            db_name = self.db_name
        self.db = self.client[db_name]
        if not self.quiet:
            print("db is set to \"%s\"" % db_name)

    def set_col(self, col_name):
        if col_name is None:
            col_name = self.col_name
        else:
            self.col_name = col_name
        self.col = self.db[col_name]
        if not self.quiet:
            print("col is set to \"%s\"" % self.col_name)

    def add_query(self, params):
        for key in params:
            self.query[key] = params[key]
        if not self.quiet:
            print("added query %s" % params)

    def add_project(self, params):
        """
        @param params: {list|dict}
        """
        if str(type(params)) == "<type 'list'>":
            for key in params:
                self.project[key] = 1
        else:
            for key in params:
                self.project[key] = params[key]
        for key in self.project:
            if self.project[key] != 0:
                self.fields.append(key)
        if not self.quiet:
            print("added project %s" % params)

    def __get_json_field(self, json, path):
        keys = path.split('.')
        key = keys[0]
        if len(keys) <= 1:
            return json[key]
        else:
            print(keys)
            return self.__get_json_field(json[key], '.'.join(keys[1:]))

    def __get_json_keys(self, json, prefix=None):
        for i in json:
            if str(type(json[i])) != "<type 'dict'>":
                return ".".join([prefix, i])
            else:
                prefix = ".".join([prefix, i])
                self.columns.append(self.__get_json_keys(json[i], prefix))

    def __is_json_column(self, data, column):
        return str(data[column].dtype) == 'object' \
               and str(type(data[column][0])) == "<type 'dict'>"

    def __get_json_column(self, data):
        ret = None
        for column in data.columns:
            if self.__is_json_column(data, column):
                ret = column
                break
        return ret

    def __get_unwrapped_columns(self):
        json_column = self.__get_json_column(self.res)
        if json_column is None:
            pass
        else:
            # print(self.res[:10])  # debugging
            unwrapped = df(self.res[json_column].tolist())
            columns = []
            for column in unwrapped.columns:
                columns.append(json_column + '_' + column)
            unwrapped.columns = columns
            self.res = self.res.join(unwrapped)
            self.res = self.res.drop(json_column, axis=1)
            self.__get_unwrapped_columns()

    def __cleanup(self):
        # self.__get_unwrapped_columns()
        self.query = {}
        self.project = {}

    def run(self, flatten=True):
        if not self.quiet:
            print("running with collection: %s, query: %s, project: %s" % (str(self.col), self.query, self.project))
        c = self.col.find(self.query, self.project)
        self.res = []
        for item in c:
            if flatten:
                self.res.append(flatten_dict(item))
            else:
                self.res.append(item)
        self.res = df(self.res)
        self.__cleanup()

    def set_quiet(self, value):
        self.quiet = value
        print("quiet is set to %s", value)

    def update(self, spec, doc, upsert=True):
        self.col.update(
            spec,
            doc,
            upsert
        )

    def preview(self):
        print(self.res[:10])
        print(self.res.describe())


class AggregationFramework(Mongo):
    """
    create a AggregationFramework instance inherited from Mongo
    default:
        client_name = "staging"
        db_name = "shopcade_marketing"
    """

    def __init__(self):
        Mongo.__init__(self)
        self.agg_list = []
        self.dim = None
        self.key = {}

    def add_match(self, params):
        try:
            params["$match"]
        except KeyError:
            params = {
                "$match": params
            }
        self.agg_list.append(params)
        if not self.quiet:
            print("added aggregation params %s" % params)

    def add_project(self, params):
        try:
            params["$project"]
        except KeyError:
            params = {
                "$project": params
            }
        self.agg_list.append(params)
        if not self.quiet:
            print("added aggregation params %s" % params)

    def add_group(self, params):
        try:
            params["$group"]
        except KeyError:
            params = {
                "$group": params
            }
        self.agg_list.append(params)
        if not self.quiet:
            print("added aggregation params %s" % params)

    def add_unwind(self, params):
        try:
            params["$unwind"]
        except (KeyError, TypeError):
            if not re.match('^\$', params):
                params = "$" + params
            params = {
                "$unwind": params
            }
        self.agg_list.append(params)
        if not self.quiet:
            print("added aggregation params %s" % params)

    def add_limit(self, params):
        try:
            params["$limit"]
        except (KeyError, TypeError):
            params = {
                "$limit": params
            }
        self.agg_list.append(params)
        if not self.quiet:
            print("added aggregation params %s" % params)

    def add_sort(self, params):
        try:
            params["$sort"]
        except KeyError:
            params = {
                "$sort": params
            }
        self.agg_list.append(params)
        if not self.quiet:
            print("added aggregation params %s" % params)

    def set_dim(self, dim):
        if str(type(dim)) == "<type 'str'>":
            dim = [dim]
        if str(type(dim)) == "<type 'list'>":
            for dim_i in dim:
                self.key[dim_i] = "$" + str(dim_i)
                print("dim \"%s\" added" % dim)
            self.dim = dim
        elif str(type(dim)) == "<type 'dict'>":
            self.dim = []
            for dim_i in dim:
                self.dim.append(dim_i)
            self.key = dim
        else:
            raise TypeError('dim is not correct type')

    def run(self, to_df=True):
        self.res = self.col.aggregate(self.agg_list)
        if to_df:
            self.__to_dataframe()
        self.__cleanup()

    def __cleanup(self):
        self.agg_list = []

    def __to_dataframe(self):
        ret = df(self.res['result'])
        idx = self.__unwind_dict(ret['_id'], '_id')
        ret = idx.join(ret.drop('_id', axis=1))
        self.res = ret

    def __unwind_dict(self, raw, key):
        ret = df.from_dict(raw)[key].tolist()
        ret = df.from_dict(ret)
        return ret

    def to_csv(self, sep=',', fpath=None, name=None, postfix=".csv", index=False):
        fpath_list = []
        if fpath is not None:
            fpath_list.append(fpath)
        if name is not None:
            fpath_list.append(name)
        if postfix is not None:
            fpath_list.append(postfix)
        fpath = ".".join(fpath_list)
        self.res.to_csv(fpath, sep, index=index)
        if not self.quiet:
            print("output file generated %s" % fpath)

    def test(self):
        self.set_db('shopcade_marketing')
        self.set_col('users')
        params = {
            '_id': {
                # 'gender': '$gender',
                # 'age': '$age',
                'user_country': '$user_country',
            },
            'users': {"$sum": 1}
        }
        self.add_match({"create_date": {"$gte": "2014-01-01", "$lte": "2014-02-01"}})
        self.add_group(params)
        self.run()
        print(self.res[:10])
        self.to_csv()


class MongoHistogram(AggregationFramework):
    def __init__(self):
        AggregationFramework.__init__(self)

    def get_histogram(self, col_name, field_name):
        self.set_col(col_name)
        self.add_group({
            "_id": "$" + field_name,
            "count": {"$sum": 1},
        })
        self.add_sort({"_id": 1})
        self.run()

    def normalize(self, keys, dropna=False, cumulative=True, debug=False):
        if type(keys) == "<type 'str'>":
            keys = [keys]
        if dropna:
            self.res = self.res.dropna()
        else:
            self.res = self.res.fillna(0)
        if debug:  # debugging
            print('kesu')
            print(self.res[:10])
        for key in keys:
            self.res[key + '_percent'] = self.res[key].astype(float) / sum(self.res[key])
            if cumulative:
                self.res[key + '_percent_cumulative'] = self.res[key + '_percent'].cumsum()

    def summary(self):
        print(self.res[:10])
        print(self.res.describe())

    def test(self):
        # self.get_histogram('users', 'metrics.activity.alltime.wants')
        # self.add_match({'metrics.recency.days_since_last_login': {"$lte": 7}})
        # self.add_match({'metrics.activity.p7d.lists': {"$gt": 0}})
        # self.get_histogram('users', 'metrics.activity.p30d.clicks')
        self.get_histogram('test', 'score')
        self.normalize(dropna=False)
        self.summary()


def main():
    ag = AggregationFramework()
    ag.test()
    # h = MongoHistogram()
    # h.test()


if __name__ == "__main__":
    main()
