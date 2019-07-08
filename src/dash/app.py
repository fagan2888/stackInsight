import dash
import dash_core_components as dcc
import dash_html_components as html

import psycopg2
import pandas as pd
from dash.dependencies import Input, Output, State
import pandas as pd

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']


def load_data(query):
    
    conn = psycopg2.connect(host= "ec2-3-214-216-152.compute-1.amazonaws.com",dbname= "ls", user= "postgres", password="")
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

        ], className="four columns"),


        html.Div([
            dcc.Input(id='input-2-state', type='text', placeholder='Enter Tags Here'),

            html.Div(id='output')

        ], className="four columns"),

        html.Div([
            html.Button(id='submit-button', n_clicks=0, children='Submit'),
            #html.Div(id='output-state')
        ], className="four columns"),
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
            html.H4('Credibility of Associated Pages'),
            dcc.Graph(id='g4')
        ], className="six columns"),
    ], className="row")


])
app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})

#@app.callback(Output('output-state', 'children'),
#              [Input('submit-button', 'n_clicks')],
#              [State('input-1-state', 'value')])
@app.callback([Output('g1','figure'),
               Output('g2','figure'),
               Output('g3','figure'),
               Output('g4','figure')],
              [Input('submit-button', 'n_clicks')],
              [State('input-1-state', 'value'),
               State('input-2-state', 'value')])
def update_figure(n_clicks, input1, input2):
    
    query1 = "SELECT COUNT(DISTINCT(qid)) AS num_ques,date_trunc('month',create_date) AS q_month FROM questions WHERE tags @> '{" + str(input2)  +"}'::varchar[] AND community='" + str(input1) +"' GROUP BY q_month ORDER BY q_month;"
    query_output1 = load_data(query1)

    query2 = "SELECT  AVG(CAST(duration/14400 as decimal)) AS dur_days ,date_trunc('month',create_date) AS q_month FROM questions WHERE tags @> '{" + str(input2)  +"}'::varchar[] AND community='" + str(input1) +"'AND duration > 0 GROUP BY q_month ORDER BY q_month;"
    query_output2 = load_data(query2)

    query3 = "SELECT cast(cast(COUNT(duration) as decimal) / COUNT(*)*100 as integer) AS prop,date_trunc('month',create_date) AS q_month  FROM questions WHERE tags @> '{" + str(input2)  + "}'::varchar[] AND community='" + str(input1) +"' GROUP BY q_month ORDER BY q_month;"
    query_output3 = load_data(query3)


    query4 = "SELECT AVG(pr_score) AS popularity,create_date FROM questions WHERE tags @> '{" + str(input2)  + "}'::varchar[] AND COMMUNITY='" + str(input1) + "' GROUP BY create_date ORDER BY create_date;"
    query_output4 = load_data(query4)

    return [{'data': [{'x': query_output1['q_month'],'y': query_output1['num_ques']}],'layout': { 'xaxis':{ 'title':'Days' }, 'yaxis':{ 'title':'Number of Questions' } }},{'data': [{'x': query_output2['q_month'],'y': query_output2['dur_days']}],'layout': { 'xaxis':{ 'title':'Days' }, 'yaxis':{ 'title':'Response Time in Days' }} }, {'data': [{'x': query_output3['q_month'],'y': query_output3['prop']}],'layout': { 'xaxis':{ 'title':'Days' }, 'yaxis':{ 'title':'Proportion of Questions With Accepted Answers' }} },{'data': [{'x': query_output4['create_date'],'y': query_output4['popularity']}],'layout': { 'xaxis':{ 'title':'Days' }, 'yaxis':{ 'title':'Popularity of Page' }} } ]

#def update_output(n_clicks, input1):
#    return u'''
#        The Button has been pressed {} times,
#        Input 1 is "{}",
#        and Input 2 is ""
#    '''.format(n_clicks, input1)
if __name__ == '__main__':
    app.run_server(debug=True,host="0.0.0.0")
