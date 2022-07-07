from flask import Flask, request
from flask_restful import Api, Resource, reqparse

app = Flask(__name__)
api = Api(app)

sessions_put_args = reqparse.RequestParser()
sessions_put_args.add_argument('session_list', type=list)


class HelloWorld(Resource):
    def get(self):
        return {'data': 'Hello, World!'}

    def post(self):
        sessions = request.get_json()
        print(sessions, type(sessions))
        return '', 201


api.add_resource(HelloWorld, '/helloworld')

if __name__ == '__main__':
    app.run(debug=True)
