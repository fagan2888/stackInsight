import dash
from flask import Flask
import dash_core_components as dcc
import dash_html_components as html

import psycopg2
import pandas as pd
from dash.dependencies import Input, Output, State
import pandas as pd

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

def load_data(query):
    conn = psycopg2.connect(host= "amazon-ec2-host",dbname= "ls", user= "postgres", password="passwd")
    cur = conn.cursor()
    sql_command = (query)
    print (sql_command)

    # Load the data
    data1 = pd.read_sql(sql_command, conn)

    print(data1.shape)
    return (data1)

comm_query="SELECT DISTINCT(community) FROM questions;"
community = load_data(comm_query)

print(community.head())

community_dict = [{'label': comm, 'value':comm}  for comm in community['community']]
print(community_dict[0:5])
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server
app.layout = html.Div([
    html.Div([
        html.Div([
            html.H1('StackInsights')

        ], className="twelve columns")
        ]),
    html.Div([
        html.Div([

            #html.Div(id='output')

            dcc.Dropdown(
               id='input-1-state',
               options= community_dict,
               placeholder="Select a community",
               )

        ], className="three columns"),


        html.Div([
            dcc.Input(id='input-2-state', type='text', placeholder='Enter Tags Here'),

            html.Div(id='output1')

        ], className="three columns"),

        html.Div([
            dcc.Input(id='input-3-state', type='text', placeholder='Enter 2nd Set of Tags Here'),

            html.Div(id='output2')

        ], className="three columns"),

        html.Div([
            html.Button(id='submit-button', n_clicks=0, children='Submit'),
            #html.Div(id='output-state')
        ], className="three columns"),
    ], className="row"),
    html.Div([
        html.Div([
            html.H4('Number of questions'),

            dcc.Graph(id='g1')
        ], className="six columns"),

        html.Div([
            html.H4('Response Time in Days'),
            dcc.Graph(id='g2')
        ], className="six columns"),
    ], className="row"),
    html.Div([
        html.Div([
            html.H4('Proportion of Questions which have Received Accepted Answers'),
            dcc.Graph(id='g3')
        ], className="six columns"),

        html.Div([
            html.H4('PageRank Distribution'),
            dcc.Graph(id='g4')
        ], className="six columns"),
    ], className="row")


])
app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})

@app.callback([Output('g1','figure'),
               Output('g2','figure'),
               Output('g3','figure'),
               Output('g4','figure')],
              [Input('submit-button', 'n_clicks')],
              [State('input-1-state', 'value'),
               State('input-2-state', 'value'),
               State('input-3-state', 'value')])
def update_figure(n_clicks, input1, input2, input3):
    
    #query1 = "SELECT COUNT(*) AS num_ques,date_trunc('month',create_date) AS q_month FROM questions WHERE tags @> '{" + str(input2)  +"}'::varchar[] AND community='" + str(input1) +"' GROUP BY q_month ORDER BY q_month;"
    query1 = "SELECT COUNT(*) AS num_ques, date_trunc('month',create_date) AS q_month FROM (SELECT DISTINCT  ON(qid) create_date FROM questions WHERE tags @> '{" + str(input2)  +"}'::varchar[] AND COMMUNITY='" + str(input1) +"' ) AS temp GROUP BY q_month ORDER BY q_month;"
    query_output1 = load_data(query1)

    query5 =  "SELECT COUNT(*) AS num_ques, date_trunc('month',create_date) AS q_month FROM (SELECT DISTINCT  ON(qid) create_date FROM questions WHERE tags @> '{" + str(input3)  +"}'::varchar[] AND COMMUNITY='" + str(input1) +"' ) AS temp GROUP BY q_month ORDER BY q_month;"
    query_output5 = load_data(query5)

    #query2 = "SELECT  AVG(CAST(duration/14400 as decimal)) AS dur_days ,date_trunc('month',create_date) AS q_month FROM questions WHERE tags @> '{" + str(input2)  +"}'::varchar[] AND community='" + str(input1) +"'AND duration > 0 GROUP BY q_month ORDER BY q_month;"
    query2 = "SELECT AVG(CAST(duration/14400 as decimal)) AS dur_days , date_trunc('month',create_date) AS q_month FROM (SELECT DISTINCT  ON(qid) create_date, duration FROM questions WHERE tags @> '{" + str(input2)  +"}'::varchar[] AND COMMUNITY='" + str(input1) +"' ) AS temp GROUP BY q_month ORDER BY q_month;"
    query_output2 = load_data(query2)
    
    query6 = "SELECT AVG(CAST(duration/14400 as decimal)) AS dur_days , date_trunc('month',create_date) AS q_month FROM (SELECT DISTINCT  ON(qid) create_date, duration FROM questions WHERE tags @> '{" + str(input3)  +"}'::varchar[] AND COMMUNITY='" + str(input1) +"' ) AS temp GROUP BY q_month ORDER BY q_month;"
    query_output6 = load_data(query6)
    #query3 = "SELECT cast(cast(COUNT(duration) as decimal) / COUNT(*)*100 as integer) AS prop,date_trunc('month',create_date) AS q_month  FROM questions WHERE tags @> '{" + str(input2)  + "}'::varchar[] AND community='" + str(input1) +"' GROUP BY q_month ORDER BY q_month;"
    query3 = "SELECT cast(cast(COUNT(duration) as decimal) / COUNT(*)*100 as integer) AS prop , date_trunc('month',create_date) AS q_month FROM (SELECT DISTINCT  ON(qid) create_date, duration FROM questions WHERE tags @> '{" + str(input2)  +"}'::varchar[] AND COMMUNITY='" + str(input1) +"' ) AS temp GROUP BY q_month ORDER BY q_month;"    
    query_output3 = load_data(query3)

    query7 = "SELECT cast(cast(COUNT(duration) as decimal) / COUNT(*)*100 as integer) AS prop , date_trunc('month',create_date) AS q_month FROM (SELECT DISTINCT  ON(qid) create_date, duration FROM questions WHERE tags @> '{" + str(input3)  +"}'::varchar[] AND COMMUNITY='" + str(input1) +"' ) AS temp GROUP BY q_month ORDER BY q_month;"
    query_output7 = load_data(query7)

    #query4 = "SELECT AVG(pr_score) AS popularity,create_date FROM questions WHERE tags @> '{" + str(input2)  + "}'::varchar[] AND COMMUNITY='" + str(input1) + "' GROUP BY create_date ORDER BY create_date;"
    query4 = "SELECT AVG(pr_score) AS popularity,date_trunc('month',create_date) AS q_month FROM questions WHERE tags @> '{" + str(input2)  +"}'::varchar[] AND COMMUNITY='" + str(input1) +"' GROUP BY q_month ORDER BY q_month;" 
    query_output4 = load_data(query4)

    query8 = "SELECT AVG(pr_score) AS popularity,date_trunc('month',create_date) AS q_month FROM questions WHERE tags @> '{" + str(input3)  +"}'::varchar[] AND COMMUNITY='" + str(input1) +"' GROUP BY q_month ORDER BY q_month;"
    query_output8 = load_data(query8)
    return [{'data': [{'x': query_output1['q_month'],'y': query_output1['num_ques'], 'name': input2},{'x': query_output5['q_month'],'y': query_output5['num_ques'],'name': input3}],'layout': { 'xaxis':{ 'title':'Days' }, 'yaxis':{ 'title':'Number of Questions' } }},{'data': [{'x': query_output2['q_month'],'y': query_output2['dur_days'],'name': input2},{'x': query_output6['q_month'],'y': query_output6['dur_days'],'name': input3}],'layout': { 'xaxis':{ 'title':'Days' }, 'yaxis':{ 'title':'Response Time in Days' }} }, {'data': [{'x': query_output3['q_month'],'y': query_output3['prop'],'name': input2},{'x': query_output7['q_month'],'y': query_output7['prop'],'name': input3}],'layout': { 'xaxis':{ 'title':'Days' }, 'yaxis':{ 'title':'Proportion of Questions With Accepted Answers' }} },{'data': [{'x': query_output4['q_month'],'y': query_output4['popularity'],'name': input2},{'x': query_output8['q_month'],'y': query_output8['popularity'],'name': input3}],'layout': { 'xaxis':{ 'title':'Days' }, 'yaxis':{ 'title':'Popularity of Page' }} } ]

if __name__ == '__main__':
    app.run_server(debug=True,host="0.0.0.0",port=8080)

