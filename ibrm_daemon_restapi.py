import flask
from datetime import timedelta
import ibrm_daemon_send
from flask import Flask,request,jsonify
from flask_cors import CORS,cross_origin
from flask import make_response, request, current_app
from functools import update_wrapper
# from flask_restful import Api
import json
import ast
import common
import os
import ConfigParser

app = Flask(__name__)
app.config['CORS_HEADERS'] = "Content-Type"
app.config['CORS_RESOURCES'] = {r"*": {"origins": "*"}}
CORS(app)

@app.route("/shell-list", methods=["POST"])
@cross_origin()
def shell_list():
    """
        Since the path '/' does not match the regular expression r'/api/*',
        this route does not have CORS headers set.
    """
#     return '''
# <html>
#     <h1>Hello CORS!</h1>
#     <h3> End to end editable example with jquery! </h3>
#     <a class="jsbin-embed" href="http://jsbin.com/zazitas/embed?js,console">JS Bin on jsbin.com</a>
#     <script src="//static.jsbin.com/js/embed.min.js?3.35.12"></script>
#
# </html>
# '''
    shObj = request.get_json()
    # print host,db_name,use_path,path
    print shObj,type(shObj)
    host  =shObj['host']
    db_name= shObj['db']
    use_path= shObj['usePath']
    path = shObj['path']

    port = 53001
    """{
  "host":"121.170.193.200",
  "db":"IBRM",
  "usePath":"Y",
  "path":"/usr/bin"
}   """
    shell_list_data = ibrm_daemon_send.SocketSender(host, port).shell_list(db_name,use_path,path)
    print shell_list_data
    shell_list_data_string = json.dumps(ast.literal_eval(shell_list_data))
    resp = flask.make_response(str(shell_list_data))
    return shell_list_data_string

#@app.route("/<host>/<db_name>/<shell_name>")
@app.route("/shell-detail", methods=["POST"])
@cross_origin()
def shell_detail():
    shObj = request.get_json()
    """
    {
  "host":"121.170.193.200",
  "db":"IBRM",
  "shellName":"test.sh",
  "shellPath":"test.sh"
     }
    """
    host = shObj['host']
    db_name = shObj['db']
    shell_name = shObj['shellName']
    shell_path = shObj['shellPath']
    print 'HOST :',host
    print 'db_name :',db_name
    print 'shell_name :', shell_name
    """
        Since the path matches the regular expression r'/api/*', this resource
        automatically has CORS headers set. The expected result is as follows:

        $ curl --include -X GET http://127.0.0.1:5000/api/v1/users/ \
            --header Origin:www.examplesite.com
        HTTP/1.0 200 OK
        Access-Control-Allow-Headers: Content-Type
        Access-Control-Allow-Origin: *
        Content-Length: 21
        Content-Type: application/json
        Date: Sat, 09 Aug 2014 00:26:41 GMT
        Server: Werkzeug/0.9.4 Python/2.7.8

        {
            "success": true
        }

    """
    port = 53001
    shell_data = ibrm_daemon_send.SocketSender(host, port).shell_detail(db_name,shell_name,shell_path)

    shell_dict = {}
    shell_dict ['shell_detail'] = shell_data
    # print shell_data
    input = json.dumps(shell_data,ensure_ascii=False)
    print 'input :',input
    return_data = json.dumps(ast.literal_eval(input))
    print 'retun data :',return_data
    #shell_data = json.dumps(ast.literal_eval(json.dumps(ast.literal_eval(shell_data))))
    # response.headers.add('Access-Control-Allow-Origin', '*')
    # return_data=common.hangul_dict().pprint(return_data)

    return str(return_data)


@app.route("/api/v1/users/create", methods=['POST'])
def create_user():
    """
        Since the path matches the regular expression r'/api/*', this resource
        automatically has CORS headers set.

        Browsers will first make a preflight request to verify that the resource
        allows cross-origin POSTs with a JSON Content-Type, which can be simulated
        as:
        $ curl --include -X OPTIONS http://127.0.0.1:5000/api/v1/users/create \
            --header Access-Control-Request-Method:POST \
            --header Access-Control-Request-Headers:Content-Type \
            --header Origin:www.examplesite.com
        >> HTTP/1.0 200 OK
        Content-Type: text/html; charset=utf-8
        Allow: POST, OPTIONS
        Access-Control-Allow-Origin: *
        Access-Control-Allow-Headers: Content-Type
        Access-Control-Allow-Methods: DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT
        Content-Length: 0
        Server: Werkzeug/0.9.6 Python/2.7.9
        Date: Sat, 31 Jan 2015 22:25:22 GMT


        $ curl --include -X POST http://127.0.0.1:5000/api/v1/users/create \
            --header Content-Type:application/json \
            --header Origin:www.examplesite.com


        >> HTTP/1.0 200 OK
        Content-Type: application/json
        Content-Length: 21
        Access-Control-Allow-Origin: *
        Server: Werkzeug/0.9.6 Python/2.7.9
        Date: Sat, 31 Jan 2015 22:25:04 GMT

        {
          "success": true
        }

    """
    return jsonify(success=True)

@app.route("/api/exception")
def get_exception():
    """
        Since the path matches the regular expression r'/api/*', this resource
        automatically has CORS headers set.

        Browsers will first make a preflight request to verify that the resource
        allows cross-origin POSTs with a JSON Content-Type, which can be simulated
        as:
        $ curl --include -X OPTIONS http://127.0.0.1:5000/api/exception \
            --header Access-Control-Request-Method:POST \
            --header Access-Control-Request-Headers:Content-Type \
            --header Origin:www.examplesite.com
        >> HTTP/1.0 200 OK
        Content-Type: text/html; charset=utf-8
        Allow: POST, OPTIONS
        Access-Control-Allow-Origin: *
        Access-Control-Allow-Headers: Content-Type
        Access-Control-Allow-Methods: DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT
        Content-Length: 0
        Server: Werkzeug/0.9.6 Python/2.7.9
        Date: Sat, 31 Jan 2015 22:25:22 GMT
    """
    raise Exception("example")

@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request. %s', e)
    return "An internal error occured", 500


if __name__ == "__main__":
    cfg = ConfigParser.RawConfigParser()
    cfg_file = os.path.join('config','config.cfg')
    cfg.read(cfg_file)
    try:
        ip=cfg.get('rest_api','ip')
    except:
        ip='127.0.0.1'
    try:
        port=cfg.get('rest_api','port')
    except:
        port=53004
    app.run(debug=True,host=ip,port=port)