from flask import Flask
from flask import render_template
import plotly.graph_objs as go
import plotly.io as pio

app = Flask(__name__)

@app.route("/")
def index():
    return render_template('index.html')

@app.route("/q1")
def q1():
    return render_template('q1.html')

@app.route("/q2")
def q2():
    return render_template('q2.html')

@app.route("/q3")
def q3():
    return render_template('q3.html')

@app.route("/q4")
def q4():
    return render_template('q4.html')

@app.route("/q5")
def q5():
    return render_template('q5.html')

@app.route("/q6")
def q6():
    return render_template('q6.html')

@app.route("/q7")
def q7():
    # Data for the pie chart
    labels = ['SM', 'YG', 'JYP', 'HYBE', 'Others']
    values = [6007799, 416463, 353966, 835863, 350713392]

    # Create the pie chart
    fig = go.Figure(data=[go.Pie(labels=labels, values=values)])

    # Generage the HTML for the plot
    plot_html = pio.to_html(fig, full_html=False)

    return render_template('q7.html', plot_html=plot_html)

@app.route("/q8")
def q8():
    # Data for the pie chart
    labels = ['SM', 'YG', 'JYP', 'HYBE', 'Others']
    values = [385, 26, 45, 77, 182768]

    # Create the pie chart
    fig = go.Figure(data=[go.Pie(labels=labels, values=values)])

    # Generage the HTML for the plot
    plot_html = pio.to_html(fig, full_html=False)

    return render_template('q8.html', plot_html=plot_html)

@app.route("/q9")
def q9():
    return render_template('q9.html')

@app.route("/q10")
def q10():
    return render_template('q10.html')
