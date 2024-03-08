from dash import Dash, Input, Output, callback, dcc, html, dash_table, clientside_callback
import dash_vega_components as dvc
import altair as alt
import plotly.express as px
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from urllib.parse import quote_plus
from sqlalchemy import create_engine
import json
# import dash



# Get Data
def get_data():
    # snapshot_dt='ALL',interface_name='ALL',dag_id='ALL',task_id='ALL',test_case_status='ALL',tc_criticality='ALL',exec_status='ALL'):
    db_connection_str = f'mysql+pymysql://beat_dq_readonly:{quote_plus("Gspann@123")}@35.187.158.251/beat_results_dev'
    db_connection = create_engine(db_connection_str)
    # if snapshot_dt=='ALL' and interface_name=='ALL'and dag_id=='ALL' and task_id=='ALL' and test_case_status=='ALL' and tc_criticality=='ALL' and exec_status=='ALL':
    df = pd.read_sql(
        """SELECT snapshot_dt,interface_name,dag_id,task_id,test_case_status,tc_criticality,exec_status,run_id
                     FROM beat_results_dev.vw_dv_summary_rpt;""",
        con=db_connection,
    )
    return df


df = get_data()


def generate_options(columnname):
    options = df[columnname].unique()
    options = options.tolist()
    options.append("All")
    return options


# Define available color options


# ////////////////Drop Down///////////////////

app = Dash()
app.layout = html.Div(
    [
        html.Div(
            children=[html.H1("Data Quality Dashboard")], style={"text-align": "center"}
        ),
        html.Div(
            children=[
                html.Div(
                    [
                        html.Label("Execution Date", style={"font-size": "25px"}),
                        dcc.Dropdown(
                            id="Date_dropdown",
                            options=generate_options("snapshot_dt"),
                            value="All",
                        ),
                    ],
                    style={"display": "inline-block", "margin-right": "20px"},
                ),
                html.Div(
                    [
                        html.Label("WorkFlow Name", style={"font-size": "25px"}),
                        dcc.Dropdown(
                            options=generate_options("interface_name"),
                            value="All",
                            id="workflow_dropdown",
                        ),
                    ],
                    style={"display": "inline-block", "margin-right": "20px"},
                ),
                html.Div(
                    [
                        html.Label("ETL Process (DAG ID)", style={"font-size": "25px"}),
                        dcc.Dropdown(
                            options=generate_options("dag_id"),
                            value="All",
                            id="DagId_dropdown",
                        ),
                    ],
                    style={"display": "inline-block", "margin-right": "20px"},
                ),
                html.Div(
                    [
                        html.Label(
                            "ETL Process (Task ID)", style={"font-size": "25px"}
                        ),
                        dcc.Dropdown(
                            options=generate_options("task_id"),
                            value="All",
                            id="TaskID_dropdown",
                        ),
                    ],
                    style={"display": "inline-block", "margin-right": "20px"},
                ),
                html.Div(
                    [
                        html.Label("Test Case Status", style={"font-size": "25px"}),
                        dcc.Dropdown(
                            options=generate_options("test_case_status"),
                            value="All",
                            id="TestCase_dropdown",
                        ),
                    ],
                    style={"display": "inline-block"},
                ),
            ],
            style={
                "display": "flex",
                "justify-content": "space-between",
                "padding": "10px 20px",
            },
        ),
        html.Div(
            children=[
                html.Div([html.Label("Execution Summary", style={"font-size": "25px"})])
            ],
        style = {'margin-bottom' : '50px'}),
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Test Runs Executed", style={"font-size": "15px"}),
                        html.Div(
                            id="selected-values-output", style={"margin-top": "0px"}
                        ),
                        html.Div(
                            [html.Label("Till Date", style={"font-size": "10px"})]
                        ),
                    ],
                    style={"display": "inline-block", "margin-right": "100px"},
                ),
                # problem area problem area problem area problem area problem area problem area problem area problem area problem area problem area
                html.Div(
                    [
                        html.Label(
                            "Test Case Execution Status", style={"font-size": "15px"}
                        ),
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Div(id="selected-values-output1"),
                                        html.Label(
                                            "Completed", style={'color': 'LightGreen',"font-size": "10px"}
                                        ),
                                    ],
                                    style={
                                        "display": "inline-block",
                                        "margin-right": "5px",
                                    },
                                ),
                                html.Div(
                                    [
                                        html.Div(id="selected-values-output2"),
                                        html.Label(
                                            "In Progress", style={'color': 'Orange',"font-size": "10px"}
                                        ),
                                    ],
                                    style={
                                        "display": "inline-block",
                                        "margin-right": "5px",
                                    },
                                ),
                                html.Div(
                                    [
                                        html.Div(id="selected-values-output3"),
                                        html.Label(
                                            "Failed", style={'color': 'Red',"font-size": "10px"}
                                        ),
                                    ],
                                    style={
                                        "display": "inline-block",
                                        "margin-right": "5px",
                                    },
                                ),
                            ]
                        ),
                    ],
                    style={
                        "display": "inline-block",
                        "margin-right": "100px",
                        # "margin-top": "20px",
                    },
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Label(
                                    "Test Case Status By Criticality",
                                    style={"font-size": "15px"},
                                ),                             
                                html.Div(
                                    id="selected-values-output4"
                                    # style={"margin-top": "0px"},
                                ),
                                   html.Div(
                                    [
                                        html.Label(
                                            "Completed", style={'color': 'LightGreen',"font-size": "10px"}
                                        )
                                    ]
                                ),
                            ],
                            style={"display": "inline-block", "margin-right": "20px"},
                        ),
                        html.Div(
                            [
                                dvc.Vega(
                                    id="bar-graph", spec={}
                                ),  # Replace with your Vega specification
                            ],
                            style={
                                "display": "inline-block",
                                "margin-left": "0px",
                                "vertical-align": "top" 
                            },
                        ),  # Adjust margin as needed
                    ],
                    style={"display": "inline-block","vertical-align": "top"},
                ),
            ]),  ############################ Radio Button#####################
            
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Execution History", style={"font-size": "25px"})
                    ],),
                html.Div(
                    [
                        dcc.RadioItems(['All', 'High', 'Medium','Low'], 'All', id='controls-and-radio-item',inline=True),
                    ], ),
                   html.Div(
                    
                        id="selected-values-output5", style={"margin-top": "0px"}
                    ),                        
                    ], 
                    style={"display": "inline-block", "margin-right": "100px"},
                ),
            
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Summary Report By Scenerio", style={"font-size": "25px"}),
                        dcc.RadioItems(['All', 'High', 'Medium','Low'], 'All', id='controls-and-radio-item1',inline=True),
                        html.Div(
                            id="selected-values-output6", style={"margin-top": "0px"}
                        ),
                            ],
                    style={"display": "inline-block", "margin-right": "100px"},
                ),
            ] , style = {'display' : 'inline-block' , 'vertical-align' : 'top'}),


            html.Div(
            [
                html.Div(
                    [
                        html.Label("Detailed Report By Scenerio", style={"font-size": "25px"}),
                        html.Div(
                            id="selected-values-output7", style={"margin-top": "0px"}
                        ),
                            ],
                    style={"display": "inline-block", "margin-right": "100px"},
                ),
            ])
           

  ###################  
      ])      
  



# Add controls to build the interaction
@app.callback(
    Output("selected-values-output", "children"),
    Output("selected-values-output1", "children"),
    Output("selected-values-output2", "children"),
    Output("selected-values-output3", "children"),
    Output("selected-values-output4", "children"),
    Output("bar-graph", "spec"),
    Output("selected-values-output5", "children"),
    Output("selected-values-output6", "children"),
    Output("selected-values-output7", "children"),
    [
        Input("Date_dropdown", "value"),
        Input("workflow_dropdown", "value"),
        Input("DagId_dropdown", "value"),
        Input("TaskID_dropdown", "value"),
        Input("TestCase_dropdown", "value"),
        Input(component_id='controls-and-radio-item', component_property='value'),
        Input(component_id='controls-and-radio-item1', component_property='value'),

    ],
)
def update_output(
    selected_date,
    selected_workflow,
    selected_dag_id,
    selected_task_id,
    selected_test_case_status,
    col_chosen,
    col_chosen1    
):
    # SQL query with formatted parameters
    test_runs_executed_query = """SELECT COUNT(run_id) FROM vw_dv_summary_rpt """
    test_case_executed_complete_query = """select count(exec_status) from vw_dv_summary_rpt where exec_status='completed' """
    test_case_executed_inprogress_query = """select count(exec_status) from vw_dv_summary_rpt where exec_status='InProgress' """
    test_case_executed_failed_query = """select count(exec_status) from vw_dv_summary_rpt where exec_status='Failed' """
    test_case_executed_query = """SELECT  test_case_status,count(test_case_status) as status,tc_criticality FROM vw_dv_summary_rpt """
    Execution_history_query = """SELECT dag_id "DAG Name",substr(exec_start_date,1,10) "Execution Date",
                tc_criticality Criticality,
                sum(case when test_case_status != "IN Progress" then 1 else 0 end) as Completed,
                sum(case when test_case_status = "IN Progress" then 1 else 0 end) as "In Progress",
                sum(case when test_case_status = "PASS" then 1 else 0 end) as Passed,
                sum(case when test_case_status = "FAIL" then 1 else 0 end) as Failed
                from vw_dv_summary_rpt"""
    summary_report_query = """SELECT scenario_name Scenario,tc_criticality Criticality,exec_status "Execution Status",count(*) Executed,
                        sum(case when test_case_status = "IN Progress" then 1 else 0 end) as "In Progress",
                        sum(case when test_case_status = "FAIL" then 1 else 0 end) as Failed,
                        sum(case when test_case_status != "IN Progress" then 1 else 0 end) as Completed,
                        sum(case when test_case_status = "PASS" then 1 else 0 end) as Passed
                        from vw_dv_summary_rpt"""
    detail_report_query = """SELECT scenario_name "Scenario Name",scenario_desc "Scenario Desc",dag_id "DAG ID",exec_status "Execution Status",tc_criticality Criticality,count(*) Executed,
                                sum(case when test_case_status != "IN Progress" then 1 else 0 end) as Completed,
                                sum(case when test_case_status = "IN Progress" then 1 else 0 end) as "In Progress",
                                sum(case when test_case_status = "PASS" then 1 else 0 end) as Passed,
                                sum(case when test_case_status = "FAIL" then 1 else 0 end) as Failed
                                from vw_dv_summary_rpt"""
    
    # Build WHERE clause based on selections
    where_clause = []
    if selected_date != "All":
        where_clause.append(f"snapshot_dt = '{selected_date}'")
    if selected_workflow != "All":
        where_clause.append(f"interface_name = '{selected_workflow}'")
    if selected_dag_id != "All":
        where_clause.append(f"dag_id = '{selected_dag_id}'")
    if selected_task_id != "All":
        where_clause.append(f"task_id = '{selected_task_id}'")
    if selected_test_case_status != "All":
        where_clause.append(f"test_case_status = '{selected_test_case_status}'")



    if where_clause:
        test_runs_executed_query += f" WHERE {' AND '.join(where_clause)}"
        test_case_executed_complete_query += f" AND  {' AND '.join(where_clause)}"
        test_case_executed_inprogress_query += f" AND {' AND '.join(where_clause)}"
        test_case_executed_failed_query += f" AND {' AND '.join(where_clause)}"
        test_case_executed_query += f" WHERE {' AND '.join(where_clause)}"
         ###########Radio##############
        Execution_history_query += f" WHERE {' AND '.join(where_clause)}"
        summary_report_query += f" WHERE {' AND '.join(where_clause)}"
      
         #########################
        detail_report_query += f" WHERE {' AND '.join(where_clause)}"


        ################################Radio#################3333333
    if col_chosen:
        if  col_chosen != "All":
            # where_clause.append(f" tc_criticality= '{col_chosen}'")
            if where_clause:
                 Execution_history_query += f" AND tc_criticality= '{col_chosen}'"
            else:
                Execution_history_query += f" WHERE tc_criticality= '{col_chosen}'"


                       

    ###########Radio1############
    if col_chosen1:
        if  col_chosen1 != "All":
            #  where_clause.append(f" tc_criticality= '{col_chosen1}'")
            if where_clause:
                 summary_report_query += f" AND tc_criticality= '{col_chosen1}'"
            else:
                summary_report_query += f" WHERE tc_criticality= '{col_chosen1}'"
    

    ######GROUP BY#######################

    test_case_executed_query += f" GROUP BY tc_criticality,test_case_status"
    ###########Radio##############
    Execution_history_query += f" GROUP BY tc_criticality,dag_id,substr(exec_start_date,1,10)"
    summary_report_query += f" group by scenario_name ,tc_criticality ,exec_status"
    ####################
    detail_report_query += f" GROUP BY scenario_name,scenario_desc,dag_id,tc_criticality,exec_status"

    # Execute the query using your database connection and engine
    db_connection_str = f'mysql+pymysql://beat_dq_readonly:{quote_plus("Gspann@123")}@35.187.158.251/beat_results_dev'
    db_connection = create_engine(db_connection_str)
    test_runs_executed = pd.read_sql(test_runs_executed_query, con=db_connection)
    test_case_executed_complete = pd.read_sql(
        test_case_executed_complete_query, con=db_connection
    )
    test_case_executed_inprogress = pd.read_sql(
        test_case_executed_inprogress_query, con=db_connection
    )
    test_case_executed_failed = pd.read_sql(
        test_case_executed_failed_query, con=db_connection
    )
    test_case_executed_query = pd.read_sql(test_case_executed_query, con=db_connection)

    Execution_history = pd.read_sql(Execution_history_query, con=db_connection)
    summary_report = pd.read_sql(summary_report_query, con=db_connection)
    detail_report = pd.read_sql(detail_report_query, con=db_connection)

    # result_message={}
    # Display the query result
    # selected_values = f"Selected values: Date - {selected_date}, Workflow - {selected_workflow}, DAG ID - {selected_dag_id}, Task ID - {selected_task_id}, Test Case Status - {selected_test_case_status}"
    result_message = f"{test_runs_executed.iloc[0, 0]}"
    result_message1 = f"{test_case_executed_complete.iloc[0, 0]}"
    result_message2 = f"{test_case_executed_inprogress.iloc[0, 0]}"
    result_message3 = f"{test_case_executed_failed.iloc[0, 0]}"
    # result_message4 = test_case_executed_query.to_dict("records")
    result_message5 = Execution_history.to_dict("records")
    result_message6 = summary_report.to_dict("records")
    result_message7 = detail_report.to_dict("records")   


    ##################Graph####################################
    # Define color scale based on field value
    color_scale = alt.Scale(domain=['FAIL', 'PASS', 'IN Progress','Undefined'],
                        range=['red', 'green', 'orange','blue'])


    base_chart = (
        alt.Chart(test_case_executed_query)
        .mark_bar(size=10)
        .encode(
            x="status",
            # y=alt.Y("tc_criticality", type="quantitative").translate(offset=5),
            # y=alt.Y("tc_criticality", scale=alt.Scale(domain=[20, 25])),  # Restrict the Y-axis to display values between 5 and 25
            y="tc_criticality",
            color=alt.Color('test_case_status', scale=color_scale, legend=alt.Legend(orient='top')),
            # color=alt.condition(
            #     alt.datum.test_case_status == "FAIL", alt.value("red"),  # Red for "FAIL"
            #     alt.condition(
            #         alt.datum.status == "Completed", alt.value("green"),  # Green for Completed
            #         alt.condition(
            #             alt.datum.status == "Running", alt.value("orange"),  # Orange for Running (new)
            #             alt.value("blue")  # Default to blue for others
            #             )
            #             )
            #             ),
            tooltip=["test_case_status", "status"],
        )
        .interactive()
    )

    # Create the label for the side value ("category")
    # text_elements1 = base_chart.mark_text(dx=10, angle=0).encode(
    #     text=alt.Text("test_case_status"), color=alt.Color('test_case_status', scale=alt.Scale(scheme='viridis'))  # Adjust as needed
    #  )

    # Create the text mark for the inside value ("value") and position it within the bar
    text_elements2 = base_chart.mark_text(dx=1, angle=0).encode(
        text=alt.Text("status") , color=alt.Color('status', scale=alt.Scale(scheme='viridis'),legend=None)  # Adjust as needed
        )

    # Create legend for test case status
    # legend = alt.Chart(test_case_executed_query).mark_bar().encode(
    # y=alt.Y('test_case_status', title='Test Case Status'),
    # color=alt.Color('test_case_status', scale=color_scale, legend=alt.Legend(orient='top'))  # Legend placed at the top
    # )


    chart = alt.layer(base_chart,text_elements2).properties(
        width=200,
        height=100,  # Adjust chart width as needed
    )

        # Combine chart and legend
    # chart = alt.vconcat(chart, legend)

    return [
        html.P(result_message),
        html.P(result_message1),
        html.P(result_message2),
        html.P(result_message3),
        html.P(result_message),
        chart.to_dict(),
        # json.dumps(result_message5),
        # json.dumps(result_message6),
        # json.dumps(result_message7)
         ###########Radio##############
       dash_table.DataTable(
            data=result_message5, page_size=6,
            columns=[{'name': col, 'id': col} for col in Execution_history.columns],  # Define columns
            style_cell={'minWidth': '50px', 'width': '50px', 'maxWidth': '80px'}  # Optional styling
        ),
        dash_table.DataTable(
            data=result_message6, page_size=6,
            columns=[{'name': col, 'id': col} for col in summary_report.columns],  # Define columns
            style_cell={'minWidth': '60px', 'width': '60px', 'maxWidth': '80px'},  # Optional styling
            style_table={'overflowX': 'auto'}
        ),
        ##############################
        dash_table.DataTable(
            data=result_message7, page_size=6,
            columns=[{'name': col, 'id': col} for col in detail_report.columns],  # Define columns
            style_cell={'minWidth': '100px', 'width': '100px', 'maxWidth': '100px'},  # Optional ,
            style_table={'overflowX': 'auto'}

        )
      ]


# # call main function
if __name__ == "__main__":
    app.run(debug=True)