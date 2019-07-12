import re

from flask import Flask
from flask_restful import Resource, Api
from sqlalchemy import create_engine, text

from config import Production as config

app = Flask(__name__)
api = Api(app)

class HelloWorld(Resource):
    __engine = None

    @property
    def _engine(self):
        if self.__engine:
            return self.__engine
        else:
            self.__engine = create_engine(config.URL, pool_pre_ping=True)
            return self.__engine
        

    def get(self, host_id):
        if re.match(r'^\d+$', host_id):
            _t = text(f"SELECT * FROM listings_info WHERE host_id = {host_id}")
            res = self._engine.execute(_t)
            first_row = res.fetchone()
            if first_row:
                return {
                    "status": 200,
                    "data": dict(zip(res.keys(), first_row)),
                }
        return {"status": 400, "data": {} }


api.add_resource(HelloWorld, '/<string:host_id>')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
