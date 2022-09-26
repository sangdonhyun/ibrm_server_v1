#-*- coding: utf-8 -*-
from flask import Flask
from flask_restx import Resource, Api
from todo import Todo

app = Flask(__name__)
api = Api(
    app,
    version='0.1',
    title="JustKode's API Server",
    description="JustKode's Todo API Server!",
    terms_url="/",
    contact="justkode@kakao.com",
    license="MIT"
)

api.add_namespace(Todo, '/todos')

if __name__ == "__main__":
    app.run(debug=True, host='121.170.193.196', port=53001)