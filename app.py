from flask import Flask
from flask import render_template
import plotly.graph_objs as go
import plotly.io as pio
import numpy as np
from pymongo import MongoClient
from pipeline import query_1, query_2, query_3age, query_3year, query_3genre, query_4, query_5genre, query_5year, query_6, query_7, query_8, query_9
from wordcloud import WordCloud
import pandas as pd
import plotly.colors as colors

# MongoDB 서버에 연결
client = MongoClient('mongodb://localhost:27017/')

# 데이터베이스 선택
db = client['big']

# 컬렉션 선택
collection = db['music']

app = Flask(__name__)

@app.route("/")
def index():
    return render_template('index.html')

@app.route("/q1")
def q1():
    #2014 ~ 2024
    cursor = [[] for _ in range(11)]
    result = [[] for _ in range(11)]

    for index in range(11):
        cursor[index] = query_1(collection, 2014 + index, 'Noun')
        result[index] = list(cursor[index])

    ranked_words = dict()
    ranked_words_cnt = dict()
    for idx in range(11):
        for word in result[idx]:
            ranked_words[word['_id']] = list()
            ranked_words_cnt[word['_id']] = list()

    for word in ranked_words:
        labeled = False
        for idx in range(11):
            rank = next((i for i, d in enumerate(result[idx]) if d['_id'] == word), None)
            ranked_words[word].append(rank+1 if rank != None else None)
            if not labeled:
                if rank != None:
                    labeled = True
                ranked_words_cnt[word].append(word + ' (' + str(result[idx][rank]['cnt']) + ')' if rank != None else None)
            else:
                ranked_words_cnt[word].append(str(result[idx][rank]['cnt']) if rank != None else None)
    
    x = np.arange(2014, 2025)

    # Figure  생성
    fig = go.Figure(layout=go.Layout(title=go.layout.Title(text="시대별 가사에 많이 나온 단어 변화(명사)")))
    fig.update_layout(title_x = 0.5, title_y = 0.9, title_xanchor = "center", title_yanchor = "middle", title_font_size = 24)
    fig.update_layout(height=800)
    fig.update_layout(
        yaxis = dict(autorange="reversed")
    )

    fig.update_layout(
        yaxis = dict(
            tickmode = 'array',
            tickvals = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            ticktext = ['1위', '2위', '3위', '4위', '5위', '6위', '7위', '8위', '9위', '10위']
        )
    )
    fig.update_xaxes(title_text='노래의 발매 연도')
    fig.update_yaxes(title_text='많이 나온 단어 순위')

    fig.update_layout(
        font=dict(size=12),
        legend=dict(font=dict(size = 16))
    )

    # Line Trace 추가
    for word in ranked_words.keys():
        fig.add_trace(go.Scatter(x=x, y=ranked_words[word], name=word,
                                mode='lines+markers+text', line_shape='spline',
                                text=ranked_words_cnt[word], textposition='top center'))
    
    # Generage the HTML for the plot
    plot_html1 = pio.to_html(fig, full_html=False)



    #2003 ~ 2013
    cursor = [[] for _ in range(11)]
    result = [[] for _ in range(11)]

    for index in range(11):
        cursor[index] = query_1(collection, 2003 + index, 'Noun')
        result[index] = list(cursor[index])

    ranked_words = dict()
    ranked_words_cnt = dict()
    for idx in range(11):
        for word in result[idx]:
            ranked_words[word['_id']] = list()
            ranked_words_cnt[word['_id']] = list()

    for word in ranked_words:
        labeled = False
        for idx in range(11):
            rank = next((i for i, d in enumerate(result[idx]) if d['_id'] == word), None)
            ranked_words[word].append(rank+1 if rank != None else None)
            if not labeled:
                if rank != None:
                    labeled = True
                ranked_words_cnt[word].append(word + ' (' + str(result[idx][rank]['cnt']) + ')' if rank != None else None)
            else:
                ranked_words_cnt[word].append(str(result[idx][rank]['cnt']) if rank != None else None)

    x = np.arange(2003, 2013)

    # Figure  생성
    fig = go.Figure(layout=go.Layout(title=go.layout.Title(text="시대별 가사에 많이 나온 단어 변화(명사)")))
    fig.update_layout(title_x = 0.5, title_y = 0.9, title_xanchor = "center", title_yanchor = "middle", title_font_size = 24)
    fig.update_layout(height=800)
    fig.update_layout(
        yaxis = dict(autorange="reversed")
    )

    fig.update_layout(
        yaxis = dict(
            tickmode = 'array',
            tickvals = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            ticktext = ['1위', '2위', '3위', '4위', '5위', '6위', '7위', '8위', '9위', '10위']
        )
    )
    fig.update_xaxes(title_text='노래의 발매 연도')
    fig.update_yaxes(title_text='많이 나온 단어 순위')

    fig.update_layout(
        font=dict(size=12),
        legend=dict(font=dict(size = 16))
    )

    # Line Trace 추가
    for word in ranked_words.keys():
        fig.add_trace(go.Scatter(x=x, y=ranked_words[word], name=word,
                                mode='lines+markers+text', line_shape='spline',
                                text=ranked_words_cnt[word], textposition='top center'))

    # Generage the HTML for the plot
    plot_html2 = pio.to_html(fig, full_html=False)

    return render_template('q1.html', plot_html1=plot_html1, plot_html2=plot_html2)

@app.route("/q2")
def q2():
    # Ballad
    cursor = [[] for _ in range(9)]
    result = [[] for _ in range(9)]

    for index in range(9):
        cursor[index] = query_2(collection, 'GN0100', 2016 + index, 'Noun')
        result[index] = list(cursor[index])

    ranked_words = dict()
    ranked_words_cnt = dict()
    for idx in range(9):
        for word in result[idx]:
            ranked_words[word['_id']] = list()
            ranked_words_cnt[word['_id']] = list()

    for word in ranked_words:
        labeled = False
        for idx in range(9):
            rank = next((i for i, d in enumerate(result[idx]) if d['_id'] == word), None)
            ranked_words[word].append(rank+1 if rank != None else None)
            if not labeled:
                if rank != None:
                    labeled = True
                ranked_words_cnt[word].append(word + ' (' + str(result[idx][rank]['cnt']) + ')' if rank != None else None)
            else:
                ranked_words_cnt[word].append(str(result[idx][rank]['cnt']) if rank != None else None)

    x = np.arange(2016, 2025)

    # Figure  생성
    fig = go.Figure(layout=go.Layout(title=go.layout.Title(text="시대별 가사에 많이 나온 단어 변화(발라드, 명사)")))
    fig.update_layout(title_x = 0.5, title_y = 0.9, title_xanchor = "center", title_yanchor = "middle", title_font_size = 24)
    fig.update_layout(height=800)
    fig.update_layout(
        yaxis = dict(autorange="reversed")
    )

    fig.update_layout(
        yaxis = dict(
            tickmode = 'array',
            tickvals = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            ticktext = ['1위', '2위', '3위', '4위', '5위', '6위', '7위', '8위', '9위', '10위']
        )
    )
    fig.update_xaxes(title_text='노래의 발매 연도')
    fig.update_yaxes(title_text='많이 나온 단어 순위')

    fig.update_layout(
        font=dict(size=12),
        legend=dict(font=dict(size = 16))
    )

    # Line Trace 추가
    for word in ranked_words.keys():
        fig.add_trace(go.Scatter(x=x, y=ranked_words[word], name=word,
                                mode='lines+markers+text', line_shape='spline',
                                text=ranked_words_cnt[word], textposition='top center'))
    
    # Generage the HTML for the plot
    ballad = pio.to_html(fig, full_html=False)


    # Dance
    cursor = [[] for _ in range(9)]
    result = [[] for _ in range(9)]

    for index in range(9):
        cursor[index] = query_2(collection, 'GN0200', 2016 + index, 'Noun')
        result[index] = list(cursor[index])

    ranked_words = dict()
    ranked_words_cnt = dict()
    for idx in range(9):
        for word in result[idx]:
            ranked_words[word['_id']] = list()
            ranked_words_cnt[word['_id']] = list()

    for word in ranked_words:
        labeled = False
        for idx in range(9):
            rank = next((i for i, d in enumerate(result[idx]) if d['_id'] == word), None)
            ranked_words[word].append(rank+1 if rank != None else None)
            if not labeled:
                if rank != None:
                    labeled = True
                ranked_words_cnt[word].append(word + ' (' + str(result[idx][rank]['cnt']) + ')' if rank != None else None)
            else:
                ranked_words_cnt[word].append(str(result[idx][rank]['cnt']) if rank != None else None)

    x = np.arange(2016, 2025)

    # Figure  생성
    fig = go.Figure(layout=go.Layout(title=go.layout.Title(text="시대별 가사에 많이 나온 단어 변화(댄스, 명사)")))
    fig.update_layout(title_x = 0.5, title_y = 0.9, title_xanchor = "center", title_yanchor = "middle", title_font_size = 24)
    fig.update_layout(height=800)
    fig.update_layout(
        yaxis = dict(autorange="reversed")
    )

    fig.update_layout(
        yaxis = dict(
            tickmode = 'array',
            tickvals = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            ticktext = ['1위', '2위', '3위', '4위', '5위', '6위', '7위', '8위', '9위', '10위']
        )
    )
    fig.update_xaxes(title_text='노래의 발매 연도')
    fig.update_yaxes(title_text='많이 나온 단어 순위')

    fig.update_layout(
        font=dict(size=12),
        legend=dict(font=dict(size = 16))
    )

    # Line Trace 추가
    for word in ranked_words.keys():
        fig.add_trace(go.Scatter(x=x, y=ranked_words[word], name=word,
                                mode='lines+markers+text', line_shape='spline',
                                text=ranked_words_cnt[word], textposition='top center'))
        
    # Generage the HTML for the plot
    dance = pio.to_html(fig, full_html=False)

    ## hiphop ##
    cursor = [[] for _ in range(9)]
    result = [[] for _ in range(9)]

    for index in range(9):
        cursor[index] = query_2(collection, 'GN0300', 2016 + index, 'Noun')
        result[index] = list(cursor[index])

    ranked_words = dict()
    ranked_words_cnt = dict()
    for idx in range(9):
        for word in result[idx]:
            ranked_words[word['_id']] = list()
            ranked_words_cnt[word['_id']] = list()

    for word in ranked_words:
        labeled = False
        for idx in range(9):
            rank = next((i for i, d in enumerate(result[idx]) if d['_id'] == word), None)
            ranked_words[word].append(rank+1 if rank != None else None)
            if not labeled:
                if rank != None:
                    labeled = True
                ranked_words_cnt[word].append(word + ' (' + str(result[idx][rank]['cnt']) + ')' if rank != None else None)
            else:
                ranked_words_cnt[word].append(str(result[idx][rank]['cnt']) if rank != None else None)

    x = np.arange(2016, 2025)

    # Figure  생성
    fig = go.Figure(layout=go.Layout(title=go.layout.Title(text="시대별 가사에 많이 나온 단어 변화(랩/힙합, 명사)")))
    fig.update_layout(title_x = 0.5, title_y = 0.9, title_xanchor = "center", title_yanchor = "middle", title_font_size = 24)
    fig.update_layout(height=800)
    fig.update_layout(
        yaxis = dict(autorange="reversed")
    )

    fig.update_layout(
        yaxis = dict(
            tickmode = 'array',
            tickvals = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            ticktext = ['1위', '2위', '3위', '4위', '5위', '6위', '7위', '8위', '9위', '10위']
        )
    )
    fig.update_xaxes(title_text='노래의 발매 연도')
    fig.update_yaxes(title_text='많이 나온 단어 순위')

    fig.update_layout(
        font=dict(size=12),
        legend=dict(font=dict(size = 16))
    )

    # Line Trace 추가
    for word in ranked_words.keys():
        fig.add_trace(go.Scatter(x=x, y=ranked_words[word], name=word,
                                mode='lines+markers+text', line_shape='spline',
                                text=ranked_words_cnt[word], textposition='top center'))
        
    # Generage the HTML for the plot
    hiphop = pio.to_html(fig, full_html=False)

    ## R&B ##

    cursor = [[] for _ in range(9)]
    result = [[] for _ in range(9)]

    for index in range(9):
        cursor[index] = query_2(collection, 'GN0400', 2016 + index, 'Noun')
        result[index] = list(cursor[index])

    ranked_words = dict()
    ranked_words_cnt = dict()
    for idx in range(6):
        for word in result[idx]:
            ranked_words[word['_id']] = list()
            ranked_words_cnt[word['_id']] = list()

    for word in ranked_words:
        labeled = False
        for idx in range(6):
            rank = next((i for i, d in enumerate(result[idx]) if d['_id'] == word), None)
            ranked_words[word].append(rank+1 if rank != None else None)
            if not labeled:
                if rank != None:
                    labeled = True
                ranked_words_cnt[word].append(word + ' (' + str(result[idx][rank]['cnt']) + ')' if rank != None else None)
            else:
                ranked_words_cnt[word].append(str(result[idx][rank]['cnt']) if rank != None else None)

    x = np.arange(2019, 2025)

    # Figure  생성
    fig = go.Figure(layout=go.Layout(title=go.layout.Title(text="시대별 가사에 많이 나온 단어 변화(R&B, 명사)")))
    fig.update_layout(title_x = 0.5, title_y = 0.9, title_xanchor = "center", title_yanchor = "middle", title_font_size = 24)
    fig.update_layout(height=800)
    fig.update_layout(
        yaxis = dict(autorange="reversed")
    )

    fig.update_layout(
        yaxis = dict(
            tickmode = 'array',
            tickvals = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            ticktext = ['1위', '2위', '3위', '4위', '5위', '6위', '7위', '8위', '9위', '10위']
        )
    )
    fig.update_xaxes(title_text='노래의 발매 연도')
    fig.update_yaxes(title_text='많이 나온 단어 순위')

    fig.update_layout(
        font=dict(size=12),
        legend=dict(font=dict(size = 16))
    )

    # Line Trace 추가
    for word in ranked_words.keys():
        fig.add_trace(go.Scatter(x=x, y=ranked_words[word], name=word,
                                mode='lines+markers+text', line_shape='spline',
                                text=ranked_words_cnt[word], textposition='top center'))

        
    # Generage the HTML for the plot
    rb = pio.to_html(fig, full_html=False)


    ## Indie ##
    cursor = [[] for _ in range(9)]
    result = [[] for _ in range(9)]

    for index in range(9):
        cursor[index] = query_2(collection, 'GN0500', 2016 + index, 'Noun')
        result[index] = list(cursor[index])

    ranked_words = dict()
    ranked_words_cnt = dict()
    for idx in range(5):
        for word in result[idx]:
            ranked_words[word['_id']] = list()
            ranked_words_cnt[word['_id']] = list()


    for word in ranked_words:
        labeled = False
        for idx in range(9):
            rank = next((i for i, d in enumerate(result[idx]) if d['_id'] == word), None)
            ranked_words[word].append(rank+1 if rank != None else None)
            if not labeled:
                if rank != None:
                    labeled = True
                ranked_words_cnt[word].append(word + ' (' + str(result[idx][rank]['cnt']) + ')' if rank != None else None)
            else:
                ranked_words_cnt[word].append(str(result[idx][rank]['cnt']) if rank != None else None)

    x = np.arange(2020, 2025)

    # Figure  생성
    fig = go.Figure(layout=go.Layout(title=go.layout.Title(text="시대별 가사에 많이 나온 단어 변화(인디음악, 명사)")))
    fig.update_layout(title_x = 0.5, title_y = 0.9, title_xanchor = "center", title_yanchor = "middle", title_font_size = 24)
    fig.update_layout(height=800)
    fig.update_layout(
        yaxis = dict(autorange="reversed")
    )

    fig.update_layout(
        yaxis = dict(
            tickmode = 'array',
            tickvals = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            ticktext = ['1위', '2위', '3위', '4위', '5위', '6위', '7위', '8위', '9위', '10위']
        )
    )
    fig.update_xaxes(title_text='노래의 발매 연도')
    fig.update_yaxes(title_text='많이 나온 단어 순위')

    fig.update_layout(
        font=dict(size=12),
        legend=dict(font=dict(size = 16))
    )

    # Line Trace 추가
    for word in ranked_words.keys():
        fig.add_trace(go.Scatter(x=x, y=ranked_words[word], name=word,
                                mode='lines+markers+text', line_shape='spline',
                                text=ranked_words_cnt[word], textposition='top center'))

    # Generage the HTML for the plot
    indie = pio.to_html(fig, full_html=False)

    # folk
    cursor = [[] for _ in range(9)]
    result = [[] for _ in range(9)]

    for index in range(9):
        cursor[index] = query_2(collection, 'GN0800', 2016 + index, 'Noun')
        result[index] = list(cursor[index])

    ranked_words = dict()
    ranked_words_cnt = dict()
    for idx in range(9):
        for word in result[idx]:
            ranked_words[word['_id']] = list()
            ranked_words_cnt[word['_id']] = list()

    for word in ranked_words:
        labeled = False
        for idx in range(9):
            rank = next((i for i, d in enumerate(result[idx]) if d['_id'] == word), None)
            ranked_words[word].append(rank+1 if rank != None else None)
            if not labeled:
                if rank != None:
                    labeled = True
                ranked_words_cnt[word].append(word + ' (' + str(result[idx][rank]['cnt']) + ')' if rank != None else None)
            else:
                ranked_words_cnt[word].append(str(result[idx][rank]['cnt']) if rank != None else None)

    x = np.arange(2016, 2025)

    # Figure  생성
    fig = go.Figure(layout=go.Layout(title=go.layout.Title(text="시대별 가사에 많이 나온 단어 변화(포크/블루스, 명사)")))
    fig.update_layout(title_x = 0.5, title_y = 0.9, title_xanchor = "center", title_yanchor = "middle", title_font_size = 24)
    fig.update_layout(height=800)
    fig.update_layout(
        yaxis = dict(autorange="reversed")
    )

    fig.update_layout(
        yaxis = dict(
            tickmode = 'array',
            tickvals = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            ticktext = ['1위', '2위', '3위', '4위', '5위', '6위', '7위', '8위', '9위', '10위']
        )
    )
    fig.update_xaxes(title_text='노래의 발매 연도')
    fig.update_yaxes(title_text='많이 나온 단어 순위')

    fig.update_layout(
        font=dict(size=12),
        legend=dict(font=dict(size = 16))
    )

    # Line Trace 추가
    for word in ranked_words.keys():
        fig.add_trace(go.Scatter(x=x, y=ranked_words[word], name=word,
                                mode='lines+markers+text', line_shape='spline',
                                text=ranked_words_cnt[word], textposition='top center'))

    # Generage the HTML for the plot
    folk = pio.to_html(fig, full_html=False)

    return render_template('q2.html', plot_html1=ballad, plot_html2=dance, plot_html3=hiphop, plot_html4=rb, plot_html5=indie, plot_html6=folk)

@app.route("/q3")
def q3():

    ## age
    random = query_3age(collection)
    data = []
    for x in random:
        data.append(x)

    Ager = [ item['Ager'] for item in data]
    DIVKOR = [item['DIVKOR'] for item in data]
    DIVENG = [item['DIVENG'] for item in data]
    DIVOTR = [item['DIVOTR'] for item in data]

    traces = []

    # 스택 막대 그래프 설정
    trace1 = go.Bar(x=Ager, y=DIVKOR, name='DIVKOR', marker_color='#FA8072', text=[f'{val:.2f}' for val in DIVKOR], textposition='auto',
                    textfont=dict(color='white', size=16, family='Arial, sans-serif'))
    trace2 = go.Bar(x=Ager, y=DIVENG, name='DIVENG', marker_color='skyblue', text=[f'{val:.2f}' for val in DIVENG], textposition='auto',
                    textfont=dict(color='white', size=16, family='Arial, sans-serif'))
    trace3 = go.Bar(x=Ager, y=DIVOTR, name='DIVOTR', marker_color='green', text=[f'{val:.2f}' for val in DIVOTR], textposition='auto',
                    textfont=dict(color='white', size=16, family='Arial, sans-serif'))

    # 레이아웃 설정
    layout = go.Layout(
        title='Stacked Bar Chart by Age',
        xaxis=dict(title='Age'),
        yaxis=dict(title='Values', range=[0, 1]),  # y축 범위 설정
        barmode='stack'  # 스택 모드로 설정하여 스택 막대 그래프 생성
    )

    # Figure 생성
    fig = go.Figure(data=[trace1, trace2, trace3], layout=layout)

    # Generage the HTML for the plot
    age = pio.to_html(fig, full_html=False)

    ##year
    random = query_3year(collection)
    data = []
    for x in random:
        data.append(x)

    Year = [ item['Year'] for item in data]
    DIVKOR = [item['MeanKOR'] for item in data]
    DIVENG = [item['MeanENG'] for item in data]
    DIVOTR = [item['MeanOTR'] for item in data]

    traces = []

    # 스택 막대 그래프 설정
    trace1 = go.Bar(x=Year, y=DIVKOR, name='DIVKOR', marker_color='#FA8072', text=[f'{val:.2f}' for val in DIVKOR], textposition='auto',
                    textfont=dict(color='white', size=16, family='Arial, sans-serif'))
    trace2 = go.Bar(x=Year, y=DIVENG, name='DIVENG', marker_color='skyblue', text=[f'{val:.2f}' for val in DIVENG], textposition='auto',
                    textfont=dict(color='white', size=16, family='Arial, sans-serif'))
    trace3 = go.Bar(x=Year, y=DIVOTR, name='DIVOTR', marker_color='green', text=[f'{val:.2f}' for val in DIVOTR], textposition='auto',
                    textfont=dict(color='white', size=16, family='Arial, sans-serif'))

    # 레이아웃 설정
    layout = go.Layout(
        title='Stacked Bar Chart by Year',
        xaxis=dict(title='Year'),
        yaxis=dict(title='Values', range=[0, 1]),  # y축 범위 설정
        barmode='stack'  # 스택 모드로 설정하여 스택 막대 그래프 생성
    )

    # Figure 생성
    fig = go.Figure(data=[trace1, trace2, trace3], layout=layout)

    # Generage the HTML for the plot
    year = pio.to_html(fig, full_html=False)


    ## genre ##
    random = query_3genre(collection)
    data = []
    for x in random:
        data.append(x)

    Genre = [ item['Genre'] for item in data]
    DIVKOR = [item['DIVKOR'] for item in data]
    DIVENG = [item['DIVENG'] for item in data]
    DIVOTR = [item['DIVOTHER'] for item in data]

    newY= []
    for g in Genre:
        if g == 'GN0400':
            newY.append('R&B/Soul')
        elif g == 'GN0100':
            newY.append('Ballad')
        elif g == 'GN0200' :
            newY.append('Dance')
        elif g == 'GN0300':
            newY.append('Hiphop')
        elif g == 'GN0800':
            newY.append('Fork/Buls')
        elif g == 'GN0500':
            newY.append('Indie')

    traces = []

    # 각 DIV 항목에 대한 막대 그래프 생성
    traces.append(go.Bar(
        x=newY,
        y=DIVKOR,
        name='DIVKOR',
        marker_color='pink',
        text=[f"{val:.3f}" for val in DIVKOR],
        textposition = 'outside'
    ))
    traces.append(go.Bar(
        x=newY,
        y=DIVENG,
        name='DIVENG',
        marker_color='skyblue',
        text=[f"{val:.3f}" for val in DIVENG],
        textposition = 'outside'
    ))
    traces.append(go.Bar(
        x=newY,
        y=DIVOTR,
        name='DIVOTR',
        marker_color='green',
        text=[f"{val:.3f}" for val in DIVOTR],
        textposition = 'outside'
    ))

    layout = go.Layout(
        title='Box Plots by Genre for Rate of KOR, ENG, OTHER',
        xaxis=dict(title='Genre'),
        yaxis=dict(title='Values'),
        boxmode='group'  # 그룹화하여 박스플롯 표시
    )
    fig = go.Figure(data=traces, layout=layout)

    # Generage the HTML for the plot
    genre = pio.to_html(fig, full_html=False)

    return render_template('q3.html', plot_html=age, plot_html2=year, plot_html3=genre)

@app.route("/q4")
def q4():
    random = query_4(collection)
    data = []
    for x in random:
        data.append(x)

    Type = [ item['Type'] for item in data]
    Genre = [ item['Genre'] for item in data]
    Prop = [item['Proportion'] for item in data]

    def ReturnGenre(g):
        if g == 'GN0400':
            return 'R&B/Soul'
        elif g == 'GN0100':
            return 'Ballad'
        elif g == 'GN0200' :
            return 'Dance'
        elif g == 'GN0300':
            return 'Hiphop'
        elif g == 'GN0800':
            return 'Fork/Buls'
        elif g == 'GN0500':
            return 'Indie'

    # 데이터를 Genre 기준으로 정렬
    sorted_data = sorted(zip(Genre, Type, Prop), key=lambda x: x[0])
    Genre, Type, Prop = zip(*sorted_data)

    unique_genres = list(set(Genre))
    unique_types = list(set(Type))
    #unique_genders = list(set(Gender))

    # 각 Genre에 대해 색상 지정
    color_scale = colors.qualitative.Plotly
    genre_color_map = {genre: color_scale[i % len(color_scale)] for i, genre in enumerate(unique_genres)}


    # Plotly 막대그래프 설정
    traces = []

    for genre in unique_genres:
        for type_ in unique_types:
            x = []
            y = []
            text = []
            for i in range(len(Genre)):
                if Genre[i] == genre and Type[i] == type_:

                    if type_ == 1:
                        x.append("Group")
                    else:
                        x.append("Solo")
                    #x.append(f"{type_}")
                    y.append(Prop[i])
                    text.append(Prop[i])
                    break  # 해당 조합의 첫 번째 항목만 필요하므로 break
            if x and y:

                traces.append(go.Bar(name=f"{ReturnGenre(genre)}", x=x, y=y, marker_color=genre_color_map[genre], text=text, texttemplate='%{text:.3f}', textposition='outside'))

    layout = go.Layout(
        title='Grouped Bar Chart by Genre, Type, and Gender',
        xaxis=dict(title='Type'),
        yaxis=dict(title='Proportion'),
        barmode='group'
    )

    fig = go.Figure(data=traces, layout=layout)

    # Generage the HTML for the plot
    plot_html = pio.to_html(fig, full_html=False)

    return render_template('q4.html', plot_html=plot_html)

@app.route("/q5")
def q5():
    random = query_5genre(collection)
    data = []
    for x in random:
        data.append(x)

    Genre = [ item['Genre'] for item in data]
    AVGWC = [item['MEN_word'] for item in data]
    AVGUWC = [item['MEN_uniqe_word'] for item in data]
    AVGLine = [item['MEN_Line'] for item in data]

    newY= []
    for g in Genre:
        if g == 'GN0400':
            newY.append('R&B/Soul')
        elif g == 'GN0100':
            newY.append('Ballad')
        elif g == 'GN0200' :
            newY.append('Dance')
        elif g == 'GN0300':
            newY.append('Hiphop')
        elif g == 'GN0800':
            newY.append('Fork/Buls')
        elif g == 'GN0500':
            newY.append('Indie')

    traces = []

    # 스택 막대 그래프 설정
    trace1 = go.Bar(x=newY, y=AVGWC, name='AVGWC', marker_color='#FA8072', text=[f'{val}' for val in AVGWC], textposition='outside',
                    textfont=dict(color='black', size=15, family='Arial, sans-serif'))
    trace2 = go.Bar(x=newY, y=AVGUWC, name='AVGUWC', marker_color='skyblue', text=[f'{val}' for val in AVGUWC], textposition='outside',
                    textfont=dict(color='black', size=15, family='Arial, sans-serif'))
    trace3 = go.Bar(x=newY, y=AVGLine, name='AVGLine', marker_color='green', text=[f'{val}' for val in AVGLine], textposition='outside',
                    textfont=dict(color='black', size=15, family='Arial, sans-serif'))

    # 레이아웃 설정
    layout = go.Layout(
        title='Stacked Bar Chart by Year',
        xaxis=dict(title='Year'),
        yaxis=dict(title='Values(Num)'),  # y축 범위 설정
    )

    # Figure 생성
    fig = go.Figure(data=[trace1, trace2, trace3], layout=layout)

    # Generage the HTML for the plot
    genre = pio.to_html(fig, full_html=False)


    ## year ##

    random = query_5year(collection)
    data = []
    for x in random:
        data.append(x)

    Year = [ item['Year'] for item in data]
    AVGWC = [item['AVGWC'] for item in data]
    AVGUWC = [item['AVGUWC'] for item in data]
    AVGLine = [item['AVGLine'] for item in data]

    traces = []

    # 스택 막대 그래프 설정
    trace1 = go.Bar(x=Year, y=AVGWC, name='KOR', marker_color='#FA8072', text=[f'{val}' for val in AVGWC], textposition='outside',
                    textfont=dict(color='black', size=30, family='Arial, sans-serif'))
    trace2 = go.Bar(x=Year, y=AVGUWC, name='ENG', marker_color='skyblue', text=[f'{val}' for val in AVGUWC], textposition='outside',
                    textfont=dict(color='black', size=30, family='Arial, sans-serif'))
    trace3 = go.Bar(x=Year, y=AVGLine, name='OTHER', marker_color='green', text=[f'{val}' for val in AVGLine], textposition='outside',
                    textfont=dict(color='black', size=30, family='Arial, sans-serif'))

    # 레이아웃 설정
    layout = go.Layout(
        title='Stacked Bar Chart by Year',
        xaxis=dict(title='Year'),
        yaxis=dict(title='Values(num)'),  # y축 범위 설정
    )

    # Figure 생성
    fig = go.Figure(data=[trace1, trace2, trace3], layout=layout)

    # Generage the HTML for the plot
    year = pio.to_html(fig, full_html=False)

    return render_template('q5.html', plot_html1=genre, plot_html2=year)

@app.route("/q6")
def q6():
    cursor = [[] for _ in range(4)]
    result = [[] for _ in range(4)]

    cursor[0] = query_6(collection, ['(주)SM엔터테인먼트', '키이스트', '미스틱스토리'],'Noun', 10)
    cursor[1] = query_6(collection, ['(주)JYP엔터테인먼트'], 'Noun', 10)
    cursor[2] = query_6(collection, ["YG엔터테인먼트", "(주)YG엔터테인먼트", "THEBLACKLABEL"], 'Noun', 10)
    cursor[3] = query_6(collection, ["빅히트엔터테인먼트", "빌리프랩", "(주)쏘스뮤직", "플레디스", "KOZ엔터테인먼트", "주식회사어도어"], 'Noun', 10)

    ranked_words = dict()
    ranked_words_cnt = dict()
    ranked_words_num = dict()
    for idx in range(0, 4):
        result[idx] = list(cursor[idx])
        for word in result[idx]:
            ranked_words[word['_id']] = list()
            ranked_words_cnt[word['_id']] = list()
            ranked_words_num[word['_id']] = list()

    for word in ranked_words:
        for idx in range(0, 4):
            rank = next((i for i, d in enumerate(result[idx]) if d['_id'] == word), None)
            ranked_words[word].append(rank+1 if rank != None else None)
            ranked_words_cnt[word].append(word + ' (' + str(result[idx][rank]['cnt']) + ')' if rank != None else None)
            ranked_words_num[word].append(result[idx][rank]['cnt'] if rank != None else 0)

    x = [0, 1, 2, 3]

    # Figure  생성
    fig = go.Figure(layout=go.Layout(title=go.layout.Title(text="소속사별 가사에 많이 나온 단어")))
    fig.update_layout(title_x = 0.5, title_y = 0.9, title_xanchor = "center", title_yanchor = "middle", title_font_size = 24)
    fig.update_layout(height=700)
    fig.update_layout(
        yaxis = dict(autorange="reversed")
    )

    fig.update_layout(
        xaxis = dict(
            tickmode = 'array',
            tickvals = [0, 1, 2, 3],
            ticktext = ['SM엔터테인먼트', 'JYP엔터테인먼트', 'YG엔터테인먼트', '빅히트엔터테인먼트']
        )
    )
    fig.update_layout(
        yaxis = dict(
            tickmode = 'array',
            tickvals = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
            ticktext = ['1위', '2위', '3위', '4위', '5위', '6위', '7위', '8위', '9위', '10위',
                        '11위', '12위', '13위', '14위', '15위', '16위', '17위', '18위', '19위', '20위',]
        )
    )
    fig.update_xaxes(title_text='소속사')
    fig.update_yaxes(title_text='많이 나온 단어 순위')

    fig.update_layout(
        font=dict(size=15),
        legend=dict(font=dict(size = 16))
    )

    # Line Trace 추가
    for word in ranked_words.keys():
        fig.add_trace(go.Scatter(x=x, y=ranked_words[word], name=word,
                                mode='lines+markers+text', line_shape='spline',
                                text=ranked_words_cnt[word], textposition='top center'))
        
    # Generage the HTML for the plot
    plot_html = pio.to_html(fig, full_html=False)


    # WordCloud
    cursor = [[] for _ in range(4)]
    result = [[] for _ in range(4)]

    cursor[0] = query_6(collection, ['(주)SM엔터테인먼트', '키이스트', '미스틱스토리'],'Noun', 50)
    cursor[1] = query_6(collection, ['(주)JYP엔터테인먼트'], 'Noun', 50)
    cursor[2] = query_6(collection, ["YG엔터테인먼트", "(주)YG엔터테인먼트", "THEBLACKLABEL"], 'Noun', 50)
    cursor[3] = query_6(collection, ["빅히트엔터테인먼트", "빌리프랩", "(주)쏘스뮤직", "플레디스", "KOZ엔터테인먼트", "주식회사어도어"], 'Noun', 50)

    ranked_words = dict()
    ranked_words_cnt = dict()
    ranked_words_num = dict()
    for idx in range(0, 4):
        result[idx] = list(cursor[idx])
        for word in result[idx]:
            ranked_words[word['_id']] = list()
            ranked_words_num[word['_id']] = list()

    for word in ranked_words:
        for idx in range(0, 4):
            rank = next((i for i, d in enumerate(result[idx]) if d['_id'] == word), None)
            ranked_words[word].append(rank+1 if rank != None else None)
            ranked_words_num[word].append(result[idx][rank]['cnt'] if rank != None else 0)

    wordcloud_htmls = []
    company_names = ['SM 엔터', 'JYP 엔터', 'YG 엔터', '빅히트 엔터']

    for idx in range(4):
        word_count = []
        for word in ranked_words.keys():
            if ranked_words_num[word][idx] > 0:
                word_count.append((word, ranked_words_num[word][idx]))

        wordcloud = WordCloud(max_font_size=50, background_color='white', font_path='NanumGothic-Bold.ttf').generate_from_frequencies(dict(word_count))
        
        # Save the wordcloud to an image file
        image_filename = f"static/{company_names[idx]}.png"
        wordcloud.to_file(image_filename)
        
        wordcloud_htmls.append(image_filename)

    return render_template('q6.html', plot_html=plot_html, wordcloud=wordcloud_htmls)

@app.route("/q7")
def q7():
    result = query_7(collection)

    company_likes = {doc['_id']: doc['likes'] for doc in result}

    # Data for the pie chart
    labels = ['SM', 'YG', 'JYP', 'HYBE', 'Others']
    values = [company_likes.get(label, 0) for label in labels]

    # Create the pie chart
    fig = go.Figure(data=[go.Pie(labels=labels, values=values)])

    # Generage the HTML for the plot
    plot_html = pio.to_html(fig, full_html=False)

    return render_template('q7.html', plot_html=plot_html)

@app.route("/q8")
def q8():
    result = query_8(collection)

    company_musics = {doc['_id']: doc['totalMusic'] for doc in result}

    # Data for the pie chart
    labels = ['SM', 'YG', 'JYP', 'HYBE', 'Others']
    values = [company_musics.get(label, 0) for label in labels]

    # Create the pie chart
    fig = go.Figure(data=[go.Pie(labels=labels, values=values)])

    # Generage the HTML for the plot
    plot_html = pio.to_html(fig, full_html=False)

    return render_template('q8.html', plot_html=plot_html)

@app.route("/q9")
def q9():
    cursor = [[] for _ in range(5)]
    result = [[] for _ in range(5)]

    # 두 글자 걸그룹
    cursor[0] = query_9(collection, ["ITZY (있지)", "WOOAH (우아)", "PIXY (픽시)", "Billlie (빌리)", "1CHU (원츄)", "We;Na (위나)",
                        "X:IN (엑신)", "NiziU (니쥬)", "PUZZLE (퍼즐)", "카라", "캣츠", "스완"])
    # 세 글자 걸그룹
    cursor[1] = query_9(collection, ["써니힐", "마마무 (Mamamoo)", "다비치", "앨리스 (ALICE)", "(여자)아이들", "듀자매",
                        "CRAXY (크랙시)", "Weeekly (위클리)", "큐빅스 (Q6ix)", "aespa", "럼블지", "IVE (아이브)",
                        "Rocking doll (록킹돌)", "Kep1er (케플러)", "H1-KEY (하이키)", "VIVIZ (비비지)", "NMIXX", "NewJeans",
                        "첫사랑", "MAVE: (메이브)", "브브걸 (BBGIRLS)", "EL7Z UP (엘즈업)", "Loossemble (루셈블)",
                        "YOUNG POSSE (영파씨)", "eite (에이트)", "아일릿(ILLIT)", "RESCENE (리센느)", "UNIS(유니스)", "Candy Shop",
                        "VVUP(비비업)", "SPIA(수피아)", "빅마마", "블랙펄", "4minute", "티아라", "시크릿"])
    # 네 글자 걸그룹
    cursor[2] = query_9(collection, ["Apink (에이핑크)", "Red Velvet (레드벨벳)", "오마이걸 (OH MY GIRL)", "TWICE (트와이스)", "우주소녀",
                        "BLACKPINK", "드림캐쳐", "시크엔젤 (Chicangel)", "위키미키 (Weki Meki)", "해시태그", "버스터즈",
                        "하이큐티 (HI CUTIE)", "세러데이 (SATURDAY)", "드림노트 (DreamNote)", "3YE (써드아이)", "로켓펀치(Rocket Punch)",
                        "cignature (시그니처)", "마카마카 (Maka' Maka)", "블랙스완", "프레셔스 (Precious)", "STAYC(스테이씨)",
                        "트라이비(TRI.BE)", "퍼플키스 (PURPLE KISS)", "미니마니 (MINIMANI)", "LIGHTSUM", "스카이리 (SKYLE)",
                        "파시걸스 (PoshGirls)", "ICHILLIN' (아이칠린)", "뷰티박스 (BEAUTY BOX)", "GOT the beat", "아일리원(ILY:1)",
                        "LE SSERAFIM (르세라핌)", "Lapillus(라필루스)", "IRRIS (아이리스)", "소녀세상", "mimiirose", "Queenz Eye",
                        "프림로즈", "LIMELIGHT (라임라잇)", "ADYA (에이디야)", "Geenius", "유니코드 (UNICODE)", "BEWAVE(비웨이브)",
                        "BADVILLAIN (배드빌런)", "소녀시대 (GIRLS' GENERATION)", "원더걸스", "미스에스", "2NE1", "f(x)", "제이큐티", "레인보우"])
    # 다섯 글자 걸그룹
    cursor[3] = query_9(collection, ["가비엔제이", "EVERGLOW (에버글로우)", "SECRET NUMBER (시크릿넘버)", "파스텔걸스(Pastel Girls)",
                        "tripleS (트리플에스)", "주주 시크릿", "브랜뉴데이", "애프터스쿨"])
    # 여섯 글자 이상 걸그룹
    cursor[4] = query_9(collection, ["브라운아이드걸스", "ODD EYE CIRCLE (ARTMS)", "프로미스나인", "ARTBEAT", "FIFTY FIFTY",
                        "KISS OF LIFE", "BABYMONSTER", "레이디 컬렉션"])

    for idx in range(5):
        result[idx] = list(cursor[idx])

    data = []
    for idx in range(5):
        for group in result[idx]:
            classname = str(idx+2)+'글자 걸그룹' if idx != 4 else '6글자 이상 걸그룹'
            data.append({ 'name': group['_id'].split(' (')[0], 'like': group['Like'],
                        'debut': int(group['FirstRelease'].split('.')[0]),  'class': classname })

    groups = pd.DataFrame(data)

    # Figure 생성
    fig = go.Figure(layout=go.Layout(title=go.layout.Title(text="걸그룹 이름 글자 수에 따른 누적 좋아요 개수 비교")))
    fig.update_layout(title_x = 0.5, title_y = 0.92, title_xanchor = "center", title_yanchor = "middle", title_font_size = 24)
    fig.update_layout(height=1000)

    fig.update_xaxes(title_text='노래 첫 발매년도')
    fig.update_yaxes(title_text='최다 좋아요 개수')
    fig.update_yaxes(type="log")

    fig.update_layout(
        font=dict(size=10),
        legend=dict(font=dict(size = 14))
    )

    # 수평선 그리기
    fig.add_hline(y=100000,line_width=3, line_dash="dash",
                line_color="blue",
                annotation_text="좋아요 10만개", 
                annotation_position="bottom right",
                annotation_font_size=12,
                annotation_font_color="blue")

    class_list = [['2글자 걸그룹', '#636efa'], ['3글자 걸그룹', '#ef553b'], ['4글자 걸그룹', '#00cc96'],
                ['5글자 걸그룹', '#ab63fa'], ['6글자 이상 걸그룹', '#ffa15a']]

    for classname in class_list:
        subgroups = groups[groups['class'] == classname[0]]

        # Scatter Plot
        fig.add_trace(go.Scatter(x=list(subgroups.debut), y=list(subgroups.like), name=classname[0], mode='markers+text',
                                text=list(subgroups.name), textposition='top center', textfont_color=classname[1]))
        
    # Generage the HTML for the plot
    plot_html = pio.to_html(fig, full_html=False)

    return render_template('q9.html', plot_html=plot_html)