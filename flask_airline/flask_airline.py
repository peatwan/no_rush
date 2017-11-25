from flask import Flask,render_template,request,Response

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
    print(type(data1['date']))
    #print(request.form["date"])
    return Response('hello')



if __name__ == '__main__':
    app.run()
