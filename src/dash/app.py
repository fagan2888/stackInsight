import dash
import dash_core_components as dcc
import dash_html_components as html
import socket
import psycopg2
import pandas as pd
from dash.dependencies import Input, Output, State
import pandas as pd
host = socket.gethostbyname(socket.gethostname())
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
conn = psycopg2.connect("host=,dbname=ls user=postgres password=")
cur = conn.cursor()
q=pd.read_csv('q.csv')
rt = pd.read_csv('rt.csv')
a = pd.read_csv('ans.csv')
popul = pd.read_csv('popul.csv')

def load_data(query):

    sql_command = (query)
    print (sql_command)

    # Load the data
    data1 = pd.read_sql(sql_command, conn)

    print(data1.shape)
    return (data1)

comm_query="SELECT DISTINCT(community) FROM qtable;"
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
            #dcc.Input(id='input-1-state', type='text', value='Montréal'),

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
            html.Div(id='output-state')
        ], className="four columns"),
    ], className="row"),
    html.Div([
        html.Div([
            html.H4('Number of questions'),
-- INSERT --                                                                                                                62,5          41%
            dcc.Graph(id='g1', figure={'data': [{'x': q['create_date'],'y': q['count']}],'layout':{ 'xaxis':{ 'title':'Days' }, 'yaxis':{ 'title':'Number of Questions' } }})
        ], className="six columns"),

        html.Div([
            html.H4('Response Time in Hours'),
            dcc.Graph(id='g2', figure={'data': [{'x': rt['create_date'],'y': rt['rt_hours']}],'layout':{ 'xaxis':{ 'title':'Days' }, 'yaxis':{ 'title':'Response Time (In Hours)'} } })
        ], className="six columns"),
    ], className="row"),
    html.Div([
        html.Div([
            html.H4('Number of Questions which have Received Accepted Answers'),
            dcc.Graph(id='g3', figure={'data': [{'x': a['create_date'],'y': a['count']}],'layout':{ 'xaxis':{ 'title':'Days' }, 'yaxis':{ 'title':'Questions with Accepted Answers' } }})
        ], className="six columns"),

        html.Div([
            html.H4('Credibility of Page <PageRank>'),
            dcc.Graph(id='g4', figure={'data': [{'x': popul['create_date'],'y': popul['popularity']}],'layout':{ 'xaxis':{ 'title':'Days' }, 'yaxis':{ 'title':'Credibility' } }})
        ], className="six columns"),
    ], className="row")


])
app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})

#@app.callback(Output('output-state', 'children'),
#              [Input('submit-button', 'n_clicks')],
#              [State('input-1-state', 'value')])
@app.callback(Output('output-state', 'children'),
              [Input('submit-button', 'n_clicks')],
              [State('input-1-state', 'value'),
               State('input-2-state', 'value')])
def update_output(n_clicks, input1, input2):
    return u'''
        The Button has been pressed {} times,
        Input 1 is "{}",
        and Input 2 is "{}"
    '''.format(n_clicks, input1, input2)

#def update_output(n_clicks, input1):
#    return u'''
#        The Button has been pressed {} times,
#        Input 1 is "{}",
#        and Input 2 is ""
#    '''.format(n_clicks, input1)
if __name__ == '__main__':
    app.run_server(debug=True,host=host,port=8081)
