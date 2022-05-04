from flask import Flask, jsonify, request
from pyhive import hive
import paramiko, codecs, flask, json

app = Flask(__name__)
conn_obj = paramiko.SSHClient()
query_execute_command_pre = 'beeline -u jdbc:hive2://localhost:10000/default --silent=true -e'

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/results', methods=['POST'])
def results():
    data = flask.request.get_json()
    print(data)
    # query_command = "python3 cluster.py results \"" + body["term"] + "\""
    query_command = "python3 cluster.py results " + data["term"]
    print(query_command)
    stdin, stdout, stderr = conn_obj.exec_command(query_command)
    stdout.channel.set_combine_stderr(True)
    output = stdout.readlines()
    return json.loads(output[0])

    # stdout.channel.set_combine_stderr(True)
    # stdout.channel.set_combine_stderr(True)
    # # output = stdout.readlines()
    # print("output: -")
    # output = ''
    # for line in stdout.readlines():
    #     output += line
    #
    # if output:
    #     print(output)
    # else:
    #     print("There was no output for this command")
    # return json.loads(output)

@app.route('/trends', methods=['POST'])
def trends():
    data = flask.request.get_json()
    query_command = "python3 cluster.py trends " + data["term"]
    print(query_command)
    stdin, stdout, stderr = conn_obj.exec_command(query_command)
    stdout.channel.set_combine_stderr(True)
    output = stdout.readlines()
    return json.loads(output[0])

@app.route('/popularity', methods=['POST'])
def popularity():
    data = flask.request.get_json()
    query_command = "python3 cluster.py popularity " + data["url"]
    print(query_command)
    stdin, stdout, stderr = conn_obj.exec_command(query_command)
    stdout.channel.set_combine_stderr(True)
    output = stdout.readlines()
    return json.loads(output[0])

@app.route('/getBestTerms', methods=['POST'])
def get_best_terms():
    data = flask.request.get_json()
    query_command = "python3 cluster.py get_best " + data["website"]
    print(query_command)
    stdin, stdout, stderr = conn_obj.exec_command(query_command)
    stdout.channel.set_combine_stderr(True)
    output = stdout.readlines()
    return json.loads(output[0])

def hive_connection(conn_obj):
    host = "10.128.0.5"
    user = "kailee9874"

    conn_obj.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    conn_obj.connect(host, username=user, pkey=paramiko.RSAKey.from_private_key_file('c'))


if __name__ == '__main__':
    hive_connection(conn_obj)
    print("listening")
    app.run("0.0.0.0", "80")
