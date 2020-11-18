import pandas as pd
import datetime as dt

from flask import Flask
from flask_restful import Resource, Api, reqparse
from dataclasses import dataclass
from sklearn.preprocessing import MinMaxScaler

app = Flask(__name__)
api = Api(app)


@dataclass
class Fields:
    path = r"/home/maciek/Data-science-webapp/data/cdr_d.csv"
    models = ['MSE']


class Data(Resource, Fields):
    path = Fields.path

    def get(self):
        df = pd.read_csv(self.path)
        df = df.to_dict()
        return {'data': df}, 200


class Describe(Resource, Fields):
    path = Fields.path

    def get(self):
        df = pd.read_csv(self.path)
        df = df.dropna(inplace=True)
        return {'data_description': df.describe().to_dict()}, 200


class Sort(Resource, Fields):
    path = Fields.path

    def patch(self):
        df = pd.read_csv(self.path)

        parser = reqparse.RequestParser()

        parser.add_argument('sort_by', required=True)

        args = parser.parse_args()

        allowed_dic = {'date': 'Data',
                       'close': 'Zamkniecie',
                       'open': 'Otwarcie',
                       'volume': 'Wolumen',
                       'highest': 'Najwyzszy',
                       'lowest': 'Najnizszy'}

        if args['sort_by'] in allowed_dic.keys():

            df = df.sort_values(allowed_dic[args['sort_by']])

            df.to_csv(self.path, index=False)

            return {'sorted_data': df.to_dict()}, 200
        else:
            return {'message': "Niepoprawna wartość"}, 400


class Types(Resource, Fields):
    path = Fields.path

    def get(self):
        df = pd.read_csv(self.path)

        parser = reqparse.RequestParser()
        parser.add_argument('int', required=False)
        parser.add_argument('float', required=False)
        parser.add_argument('bool', required=False)

        args = parser.parse_args()

        if args['int']:
            return {'selected_dtypes': df.select_dtypes('int64').to_dict()}, 200
        elif args['float']:
            return {'selected_dtypes': df.select_dtypes('float64').to_dict()}, 200
        elif args['bool']:
            return {'selected_dtypes': df.select_dtypes('bool').to_dict()}, 200
        else:
            return {'message': "Błędny format danych"}, 400


class Head(Resource, Fields):
    path = Fields.path

    def get(self):
        df = pd.read_csv(self.path)
        return {'data_head': df.head().to_dict()}, 200


class Tail(Resource, Fields):
    path = Fields.path

    def get(self):
        df = pd.read_csv(self.path)
        return {'data_tail': df.tail().to_dict()}, 200


class Models(Resource, Fields):
    models = Fields.models

    def get(self):
        return {'models': self.models}, 200

    def put(self):
        parser = reqparse.RequestParser()
        parser.add_argument('model', required=True)

        args = parser.parse_args()

        if args['model'] in self.models:
            return {'message': f"Model {args['model']} znajduje się już na liście."}, 401
        else:
            self.models.append(args['model'])

            return {'models': self.models}, 200

    def delete(self):
        parser = reqparse.RequestParser()
        parser.add_argument('model', required=True)

        args = parser.parse_args()

        if args['model'] in self.models:

            self.models.remove(args['model'])

            return {'models': self.models}, 200
        else:
            return {'message': f"Nie można usunąć {args['model']} ponieważ nie ma go w liście."}, 404


class Preprocess(Resource, Fields):

    def put(self):

        df = pd.read_csv(self.path)

        parser = reqparse.RequestParser()
        parser.add_argument('new_data_path', required=False)
        parser.add_argument('new_column_path', required=False)

        args = parser.parse_args()

        new_df = pd.read_csv(args['new_data_path'])
        new_col = args["new_column_path"]

        if list(new_df['Data'])[0] in list(df['Data'])[-1]:
            return {'message': f"Data {new_df['Data'][0]} znajduje się już w datasecie."}, 401
        else:
            df = df.append(new_df, ignore_index=True)

        if new_col.head(1) in df.columns:
            return {'message': f"Kolumna {new_col.head(1)} już istnieje."}, 401
        else:
            df = df.join(new_col)

        df.to_csv(self.path, index=False)
        return {'data': df.to_dict()}, 200

    def patch(self):
        df = pd.read_csv(self.path)

        parser = reqparse.RequestParser()
        parser.add_argument('from_year', required=False)
        parser.add_argument('to_year', required=False)
        parser.add_argument('convert_date', required=False)

        args = parser.parse_args()

        if args['from_year']:
            for idx, row in df.iterrows():
                if args['from_year'] not in row["Data"]:
                    return {'message': f"Wprowadzona data {args['from_year']} nie występuje w datasecie."}, 404
                else:
                    df = df[idx + 1:,:]
            
                    df.to_csv(self.path, index=False)
                    
                    return {'data': df.to_dict()}, 200
                
        if args['to_year']:
            for idx, row in df.iterrows():
                if args['to_year'] not in row['Data']:
                    return {'message': f"Wprowadzona data {args['to_year']} nie występuje w datascie."}, 404
                else:
                    df = df[:idx - 1,:]
                    
                    df.to_csv(self.path, index=False)
                    
                    return {'data': df.to_dict()}, 200

        if args['convert_date']:
            date = dt.datetime.strptime()

            df['Data'] = pd.to_datetime(df['Data'], format="%Y-%m-%d")

            df.to_csv(self.path, index=False)

            return {'message': "Prawidłowo skonwertowano typ str na datetime", 'data': df.to_dict()}, 200

    def delete(self):

        df = pd.read_csv(self.path)

        parser = reqparse.RequestParser()
        parser.add_argument('date', required=False)
        parser.add_argument('column', required=False)

        args = parser.parse_args()

        if args['date']:
            if args['date'] in list(df['Data']):

                df = df[df['Data'] != args['date']]

                df.to_csv(self.path, index=False)

                return {'data': df.to_dict()}, 200
            else:
                return {'message': f"Nie znaleziono daty {args['data']}"}, 404

        if args['column']:
            if args['column'] in df.columns:

                df = df.drop(args['column'])

                df.to_csv(self.path, index=False)

                return {'data': df.to_dict()}, 200
            else:
                return {'message': f"Nie znaleziono kolumny {args['column']}"}, 404


class Regression(Resource):
    pass


api.add_resource(Data, "/data")
api.add_resource(Types, "/data/types")
api.add_resource(Head, "/data/head")
api.add_resource(Tail, "/data/tail")
api.add_resource(Describe, "/data/describe")
api.add_resource(Preprocess, "/data/preprocess")
api.add_resource(Sort, "/data/preprocess/sort")
api.add_resource(Models, "/models")
api.add_resource(Regression, "/models/regression")

if __name__ == '__main__':
    app.run()
