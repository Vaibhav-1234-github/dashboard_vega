import tkinter as tk 
import mysql.connector
import pandas as pd 
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from urllib.parse import quote_plus
from sqlalchemy import create_engine , text 
import json 
import altair as alt 
from tkinter import ttk



#declaring global variables 
Test_cases_executed  = 0 
Test_cases_completed = 0 
Test_cases_progress = 0 
Test_cases_failed = 0 
Critical_test_cases_completed = 0 
critical_total = 0 
critical_passed = 0 
critical_failed = 0 

#connecting to the database 
db_connection_str = f'mysql+pymysql://beat_dq_readonly:{quote_plus("Gspann@123")}@35.187.158.251/beat_results_dev'
db_connection = create_engine(db_connection_str)


#functions returning the values selected in dropdown options 
def option_selected_1(*args):
 '''method used to get the option selected from the dropdowns'''
 return clicked_1.get()
def option_selected_2(*args):
 '''method used to get the option selected from the dropdowns'''
 return clicked_2.get()
def option_selected_3(*args):
 '''method used to get the option selected from the dropdowns'''
 return clicked_3.get()
def option_selected_4(*args):
 '''method used to get the option selected from the dropdowns'''
 return clicked_4.get()
def option_selected_5(*args):
 '''method used to get the option selected from the dropdowns'''
 return clicked_5.get()


def fetch_rows(query):
 with db_connection.connect() as connection:
  result = connection.execute(text(query))
  rows = result.fetchall()
  return rows 
 

#getting the results of the query 
query = '''SELECT dag_id,snapshot_dt, SUM(CASE WHEN exec_status = 'Failed' THEN 1 ELSE 0 END) AS Failed,SUM(CASE WHEN exec_status = 'In Progress' THEN 1 ELSE 0 END) AS InProgress,SUM(CASE WHEN exec_status = 'Completed' THEN 1 ELSE 0 END) AS Completed,SUM(CASE WHEN test_case_status = 'PASS' THEN 1 ELSE 0 END) AS PASS,SUM(CASE WHEN test_case_status = 'IN Progress' THEN 1 ELSE 0 END) AS INProgress,SUM(CASE WHEN test_case_status = 'FAIL' THEN 1 ELSE 0 END) AS FAIL,SUM(CASE WHEN test_case_status = 'Undefined' THEN 1 ELSE 0 END) AS Undefinedd FROM vw_dv_summary_rpt GROUP BY dag_id,snapshot_dt;'''
#we will be displaying  the below dataframe on our gui
df = pd.read_sql(query,con=db_connection)
columns_df = list(df.columns)  

def on_click():
 global Test_cases_executed 
 global critical_failed
 global critical_passed
 '''this will be activated when a user clicks on onclick button'''
 drop_val_1 = option_selected_1()
 drop_val_2 = option_selected_2()
 drop_val_3 = option_selected_3()
 drop_val_4 = option_selected_4()
 drop_val_5 = option_selected_5() 
 test_runs_executed = f''' select count(run_id) from vw_dv_summary_rpt where (snapshot_dt=  '{drop_val_1}') and (interface_name= '{drop_val_2}') and (dag_id= '{drop_val_3}') and (task_id= '{drop_val_4}') and (test_case_status= '{drop_val_5}') '''
 rows = fetch_rows(test_runs_executed)
 query_completed = f'''select count( exec_status) end from vw_dv_summary_rpt where exec_status='completed'  and (snapshot_dt='{drop_val_1}') and (interface_name = '{drop_val_2}') and (dag_id = '{drop_val_3}') and (task_id = '{drop_val_4}') and (test_case_status= '{drop_val_5}')'''

 query_progress = f'''select count( exec_status) end from vw_dv_summary_rpt where exec_status='completed'  and (snapshot_dt= '{drop_val_1}') and (interface_name = '{drop_val_2}') and (dag_id = '{drop_val_3}') and (task_id = '{drop_val_4}') and (test_case_status = '{drop_val_5}')'''

 query_failed = f'''select count( exec_status) end from vw_dv_summary_rpt where exec_status='Failed '  and (snapshot_dt = '{drop_val_1}') and (interface_name= '{drop_val_2}') and (dag_id = '{drop_val_3}') and (task_id = '{drop_val_4}') and (test_case_status = '{drop_val_5}')'''

 critical_total = f'''select count( test_case_status)  from vw_dv_summary_rpt where  (snapshot_dt='{drop_val_1}') and (interface_name='{drop_val_2}')and (dag_id='{drop_val_3}') and (task_id='{drop_val_4}') and (test_case_status='{drop_val_5}')'''
 critical_passd_query  = f'''select count( test_case_status) end from vw_dv_summary_rpt where test_case_status='PASS'  and (snapshot_dt='{drop_val_1}') and (interface_name = '{drop_val_2}') and (dag_id = '{drop_val_3}') and (task_id = '{drop_val_4}') and (test_case_status= '{drop_val_5}')'''
 critical_failed_query  = f'''select count( test_case_status) end from vw_dv_summary_rpt where test_case_status='Undefined'  and (snapshot_dt='{drop_val_1}') and (interface_name = '{drop_val_2}') and (dag_id = '{drop_val_3}') and (task_id = '{drop_val_4}') and (test_case_status= '{drop_val_5}')'''
  


 Test_cases_executed = rows[0][0]
 # Update the data label with the new value

 data_label.config(text=Test_cases_executed)

 Test_cases_completed_ros = fetch_rows(query_completed)  
 Test_cases_progress_ros =  fetch_rows(query_progress)
 Test_cases_failed_ros = fetch_rows(query_failed)

 #now updating the data in the text fields 
 # Update the labels for completed, in progress, and failed test cases
 Test_execution_status_lbl_comp_ct.config(text=Test_cases_completed_ros[0][0])
 Test_execution_status_lbl_prg_ct.config(text=Test_cases_progress_ros[0][0])
 Test_execution_status_lbl_fld_ct.config(text=Test_cases_failed_ros[0][0])

 #getting outputs from the critcal test cases values here 
 critical_total_ros  = fetch_rows(critical_total)
 critical_passed_ros     = fetch_rows(critical_passd_query)
 critical_failed_ros  = fetch_rows(critical_failed_query)
 #configuring  values for the critical test cases results 
 Test_case_criticality_lbl_completed.config(text = critical_total_ros[0][0])
 #changing the value of global variables associated with above queries 
 critical_passed = critical_passed_ros[0][0]
 critical_falied =  critical_failed_ros[0][0]

 test_case_high_lbl.config(text = f"High    {critical_passed}")
 test_case_low_lbl.config(text =  f"Low  {critical_failed}")


 

#code starts from  here
if __name__ == "__main__":
 window = tk.Tk()
#  window.state("zoomed")
 window.geometry('1000x400')  # Adjusted width to fit labels
 window.title("DATA QUALITY DASHBOARD")
 #writing  the heading  label 
 heading = tk.Label(text="Data Quality Dashboard",master=window,font=('Arial' , 30))
 heading.pack(fill="x",pady=15)

 #writing the dropdowns portion 
 #drop_down_frm will contain all of the labels of dropdowns 
 drop_down_frm = tk.Frame(master= window)
 drop_down_frm.pack(fill="x" , pady= 20)

 drop_label_1 = tk.Label(text = "Execution Date", master = drop_down_frm ,font=("Arial" , 20))
 drop_label_1.grid(row=0,column=0,padx=20)
 rows = [('2024-01-02')]
 clicked_1 = tk.StringVar() 
 clicked_1.set('2024-01-02')
 dropdown = tk.OptionMenu(drop_down_frm , clicked_1 , *rows )
 dropdown.config(width=15)
 dropdown.grid(row=1,column=0)
 clicked_1.trace_add('write' ,  option_selected_1)


 drop_label_2 = tk.Label(text = "Workflow Name" , master = drop_down_frm,font=("Arial" , 20))
 drop_label_2.grid(row=0,column=1,padx=10)
 #putting  the dropdown 
 rows = [('*UNK*')]
 clicked_2 = tk.StringVar()
 dropdown = tk.OptionMenu(drop_down_frm , clicked_2 , *rows)
 dropdown.config(width=15)
 dropdown.grid(row=1,column=1)
 drop_val_2 = clicked_2.trace_add('write' , option_selected_2)

 drop_label_3 = tk.Label(text = "ETL Process(DAG) ID" , master =  drop_down_frm,font=("Arial" , 20))
 drop_label_3.grid(row=0,column=2,padx=10)
 rows =  [('dag_name'), ('dag_name2'), ('dag_name42'), ('dag_name44'), ('dag_name45'), ('gcp_terraform_n'), ('dag_name46')]
 clicked_3 = tk.StringVar()
 dropdown = tk.OptionMenu(drop_down_frm , clicked_3 , *rows)
 dropdown.config(width=15)
 dropdown.grid(row=1,column=2)
 drop_val_3 = clicked_3.trace_add('write' , option_selected_3)


 drop_label_4 = tk.Label(text = "ETL Process (DAG) TASK ID", master =  drop_down_frm,font=("Arial" , 20))
 drop_label_4.grid(row=0,column=3,padx=10)
 #putting the dropdown 
 rows = [('task_dq'), ('task_dq_1')]
 clicked_4 = tk.StringVar()
 dropdown = tk.OptionMenu(drop_down_frm , clicked_4 , *rows)
 dropdown.config(width=15)
 dropdown.grid(row=1,column=3)
 drop_val_4 = clicked_4.trace_add('write' , option_selected_4)

 drop_label_5 = tk.Label(text = "Test Case Status", master =  drop_down_frm,font = ("Arial"  , 20))
 drop_label_5.grid(row=0 , column=4,padx=10)
 #putting the dropdown 
 query = 'SELECT distinct  test_case_status "Test Case Status" FROM beat_results_dev.vw_dv_summary_rpt;'
 rows = [('PASS'), ('Undefined'), ('IN Progress')]
 clicked_5 = tk.StringVar()
 dropdown = tk.OptionMenu(drop_down_frm , clicked_5 , *rows)
 dropdown.config(width=15)
 dropdown.grid(row=1,column=4)
 drop_val_5 = clicked_5.trace_add('write' , option_selected_5)

 #building the save all button 
 save_btn = tk.Button(text = "Save All.." , command=on_click, width=20)
 save_btn.pack(anchor="center",pady=10)



 #title of 3rd layer
 exec_port_title = tk.Label(text ="Execution Summary",font=("Arial" , 15))
 exec_port_title.pack(anchor="w" , padx=20,pady=20)


 Test_runs_executed_frm_out = tk.Frame(master=window)  #this widget is used for storing all of the other widgets 
 Test_runs_executed_frm_out.pack(anchor= "w") 

 
 Test_runs_executed_frm = tk.Frame(master=Test_runs_executed_frm_out,borderwidth=2,relief="sunken" , background='#f0f0f0',border=2)
 Test_runs_executed_frm.grid(row = 0 , column = 0 ,padx= 20) 

 #column 1
 #Define the style for the title 
 title_style = ttk.Style()
 title_style.configure('Title.TLabel', font=('Arial', 15, 'bold'))
 # Adding the "Test Runs Executed" label
 title_label = ttk.Label(master=Test_runs_executed_frm , text="Test Runs Executed", style='Title.TLabel',padding='10')
 title_label.grid(row=0,column=0)

 #Define the style for the data value
 data_style = ttk.Style()
 data_style.configure('Data.TLabel', font=('Arial', 18, 'bold'), foreground='red')

 #Adding the data label , #here this label needs to be dynamic 
 data_label = ttk.Label(master=Test_runs_executed_frm, text=Test_cases_executed, style='Data.TLabel')
 data_label.grid(row=1,column=0,pady=20)

 #column 2 
 #making a frame , positioning it usign grid , and putting it in the outside frame , positioned by pack 
 Test_execution_status = tk.Frame(master = Test_runs_executed_frm_out ,borderwidth= 2, relief = "sunken" , background='#f0f0f0',border=2 )
 Test_execution_status.grid(row = 0 , column = 1,padx = 30)

 Test_execution_status_lbl = tk.Label(master = Test_execution_status , text = "Test Case Execution Status",font=("Arial" , 15 , "bold"),anchor="center")
 Test_execution_status_lbl.grid(row = 0 , column = 0,padx =  20,pady = 20)

 Test_execution_status_lbl_comp_ct = tk.Label(master=Test_execution_status, text = Test_cases_completed,font=("Arial" , 15 , "bold") ,width=5)
 Test_execution_status_lbl_comp_ct.grid(row = 1,column= 0)

 Test_execution_status_lbl_prg_ct = tk.Label(master=Test_execution_status,text= Test_cases_progress,font=("Arial" , 15 , "bold"),width=5,foreground="green")
 Test_execution_status_lbl_prg_ct.grid(row = 1,column= 1)

 Test_execution_status_lbl_fld_ct = tk.Label(master=Test_execution_status,text= Test_cases_failed,font=("Arial" , 15 , "bold") , width=5,foreground="blue")
 Test_execution_status_lbl_fld_ct.grid(row = 1,column=2)

 Test_execution_status_lbl_comp = tk.Label(master= Test_execution_status , text = "Completed" ,font=("Arial" , 10 , "bold"))
 Test_execution_status_lbl_comp.grid(row = 2, column = 0)
 Test_execution_status_lbl_prg = tk.Label(master= Test_execution_status , text = "In progress" ,font=("Arial" , 10 , "bold"))
 Test_execution_status_lbl_prg.grid(row = 2, column = 1)
 Test_execution_status_lbl_fld = tk.Label(master= Test_execution_status , text = "Failed" , font=("Arial" , 10 , "bold"))
 Test_execution_status_lbl_fld.grid(row = 2, column = 2)

 

 #column 3 
 #here making the third column noww...... 
 #this section will contain information about test case status by criticality
 Test_case_criticality= tk.Frame(master = Test_runs_executed_frm_out , borderwidth= 2, relief = "sunken" , background='#f0f0f0',border=2 )
 Test_case_criticality.grid(row = 0 , column = 2,padx = 30)

 Test_case_criticality_lbl = tk.Label(master = Test_case_criticality , text = 'Test Case Status By Criticality',font=("Arial" , 15 , "bold"),anchor="center"  ,padx=20,pady = 20)
 Test_case_criticality_lbl.grid(row = 0 , column = 0)

 
 Test_case_criticality_lbl_completed = tk.Label(master = Test_case_criticality , text = Critical_test_cases_completed ,font=("Arial" , 15 , "bold"))
 Test_case_criticality_lbl_completed.grid(row = 1 , column = 0 )

 Test_case_criticality_lbl_sbhd = tk.Label(master = Test_case_criticality , text = "Completed" , font=("Arial" , 10 , "bold") , anchor="e")
 Test_case_criticality_lbl_sbhd.grid(row = 2 , column = 0)

 #this is for the high label 
 test_case_high_lbl = tk.Label(master = Test_case_criticality , text =  f"High    0", font = ("Arial" ,10 ,  "bold"))
 test_case_high_lbl.grid(row = 1 , column = 1)

 #this is for the low label 
 test_case_low_lbl = tk.Label(master = Test_case_criticality , text=  f"Low     0", font = ("Arial" ,10 ,  "bold"))
 test_case_low_lbl.grid(row = 2 , column = 1)

 #and get dispalyed corresponding to the highs and lows labels
 high_bar =  tk.Frame(master = Test_case_criticality , width = 100 , height = 10 , background= "green")
 high_bar.grid(row = 1, column = 2 , padx =  20)  
 low_bar =  tk.Frame(master = Test_case_criticality , width = 100 , height =  10, background  = "red" )
 low_bar.grid(row = 2 , column = 2 , padx = 20)


 #third row 
 #displaying table using treeview 
 heading = tk.Label(text = "Execution History",font=("Arial" , 15 , "bold"))
 heading.pack(pady=10,padx=20,anchor = "w")

 tree = ttk.Treeview(master=window,columns=columns_df,show="headings")

 #defining the headings 
 tree.heading('dag_id', text='Dag Id')
 tree.heading('snapshot_dt', text='snapshot_dt')
 tree.heading('Failed', text='Failed')
 tree.heading('InProgress', text='InProgress')
 tree.heading('Completed', text='Completed')
 tree.heading('PASS', text='PASS')
 tree.heading('InProgress', text='InProgress')
 tree.heading('FAIL', text='FAIL')
 tree.heading('Undefinedd', text='Undefinedd')

 # Create a list to store the column widths (adjust as needed)
 column_widths = [80, 80, 80, 60, 60, 60, 60, 60, 60]


 # Set column widths
 for i, width in enumerate(column_widths):
    tree.column(columns_df[i], width=width)

 # Display DataFrame rows
 for i, row in df.iterrows():
    tree.insert("", i, text=str(i), values=tuple(row))

 tree.pack(padx = 20,anchor="w")




 
 window.mainloop()
 



