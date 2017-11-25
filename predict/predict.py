from flask import Flask,render_template,request,Response
import socket
import json
app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/airline',methods=['GET','POST'])
def process():

    return render_template('index.html')
@app.route('/graph',methods=['GET','POST'])
def show():
    print('show got')
    a=open('D:\\MSBD5001\\proj\\grpahs\\time1.png','rb')

    data=a.read()
    return Response(data)


@app.route('/search',methods=['GET','POST'])
def search():
    data=request.form
    data1=dict(data)
    time=data1['time'][0]

    origin=data1['origin'][0]

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    s.connect(('127.0.0.1', 9999))


    msg={'method':'get','time':time,'origin':origin}
    msg=json.dumps(msg)
    s.send(str.encode(msg))

    print('waiting for response from the spark server')

    res=s.recv(1024)

    res=bytes.decode(res)
    if res is not None:
        print(res)
        s.send(str.encode('done'))




    return Response(res)




if __name__ == '__main__':
    app.run()
