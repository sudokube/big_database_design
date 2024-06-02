from flask import Flask
from flask import render_template

app = Flask(__name__)

@app.route("/")
def index():
    return render_template('index.html');

@app.route("/q1")
def q1():
    return render_template('q1.html');

@app.route("/q2")
def q2():
    return render_template('q2.html');

@app.route("/q3")
def q3():
    return render_template('q3.html');

@app.route("/q4")
def q4():
    return render_template('q4.html');

@app.route("/q5")
def q5():
    return render_template('q5.html');

@app.route("/q6")
def q6():
    return render_template('q6.html');

@app.route("/q7")
def q7():
    return render_template('q7.html');

@app.route("/q8")
def q8():
    return render_template('q8.html');

@app.route("/q9")
def q9():
    return render_template('q9.html');

@app.route("/q10")
def q10():
    return render_template('q10.html');
