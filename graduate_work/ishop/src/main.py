# This is a sample Python script.
import psycopg2
#import os
from flask import Flask, render_template, request, redirect, flash
from config import Config
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from forms import EditForm
from ml_model import model_prediction
#from forms import LoginForm
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
app = Flask(__name__)
#app.config.from_object(Config)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///shop.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
migrate = Migrate(app, db)

user = {'username': 'mikki'}


class Item(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100), nullable=False)
    price = db.Column(db.Integer, nullable=False)
    isActive = db.Column(db.Boolean, default=True)
    #text = db.Column(db.Text, nullable=False)
    def __repr__(self):
        return self.title

@app.route('/')
def index():
    # Use a breakpoint in the code line below to debug your script.
    #items = Item.query.order_by(Item.price).all()
    #visitor_name = '1233140'
    visitor_name = '2'
    return render_template('index.html', visitor_name=visitor_name)#, data=items)  # Press Ctrl+F8 to toggle the breakpoint.

@app.route('/predictions', methods=['POST', 'GET'])
def predictions():
    
    visitor_name = request.args['visitor_name']
    #args = request.form
    #visitor_name = args['visitor_name']
    
    #form = EditForm()
    #form.visitor_name.data = visitor_name
    
    return render_template('predict.html', visitor_name = visitor_name, ids = model_prediction(visitor_name))
    
    
        
    
@app.route('/about')
def about():
    # Use a breakpoint in the code line below to debug your script.
    return render_template('about.html')  # Press Ctrl+F8 to toggle the breakpoint.

@app.route('/buy/<int:id>')
def item_buy(id):
    return str(id)  # Press Ctrl+F8 to toggle the breakpoint.

@app.route('/create', methods=['POST', 'GET'])
def create():
    # Use a breakpoint in the code line below to debug your script.
    if request.method == "POST":
        title = request.form['title']
        price = request.form['price']

        item = Item(title=title, price=price)

        try:
            db.session.add(item)
            db.session.commit()
            return redirect('/')
        except:
            return "ERROR"
    else:
        return render_template('create.html')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    app.run(debug=True)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/