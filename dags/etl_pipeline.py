"import required airflow library function"
from airflow import DAG 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
"import user defined classes"
from scripts.sql.sql_script import SQLQueries
from datetime import date, datetime

#config
BUCKET_NAME = "yelpraw"
CLEAN_FOLDER = "/Users/onkar/airflow/dags/clean/"

#define function to upload files to s3
def local_to_s3():
    """Function to upload processed 
    local csv files to the s3 bucket.
    """
    #define file list
    files = ['user.csv', 'business.csv', 'review.csv', 'tip.csv']
    s3 = S3Hook(aws_conn_id="aws_credentials")
    #Loop through the files and upload to s3
    for f in files:
        filename = CLEAN_FOLDER + f
        s3.load_file(filename=filename, bucket_name=BUCKET_NAME, replace=True, key=f)

def delete_s3():
    """Function to clear the files from s3 bucket.
    """
    #define file list
    files = ['user.csv', 'business.csv', 'review.csv', 'tip.csv']
    s3 = S3Hook(aws_conn_id="aws_credentials")
    #Loop through the files and delete the files
    for f in files:
        s3.delete_objects(bucket=BUCKET_NAME, keys=f)
    

#define default args for dag
default_args = {
    'owner': 'onkar',
    'depends_on_past': False,
    'email': ['onkrane@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries' : 1
}
#define the dag
dag = DAG(
    "etl_pipeline",
    default_args=default_args,
    schedule_interval='@once',
    max_active_runs=1,
    start_date= datetime(2021, 5, 21)
)

#create tasks
#start etl pipeline
start_pipeline = DummyOperator(task_id = "Start_ETL_Pipeline", dag = dag)

#Submit Spark Job
spark_submit = BashOperator (
                    task_id = 'Submit_Spark_Job',
                    bash_command= 'source /Users/onkar/.bash_profile; cd /Users/onkar/airflow/dags/scripts/spark; spark-submit spark_jobs.py ',
                    dag = dag)
#upload files to s3 bucket
upload_data = PythonOperator(
                    task_id = "Upload_Data_To_s3",
                    python_callable= local_to_s3,
                    dag = dag)

#create tables 
create_user = PostgresOperator(
                    task_id = "Create_Users_Table",
                    postgres_conn_id="redshift_conn",
                    autocommit=True,
                    sql= SQLQueries.create_table_user, 
                    dag = dag)

create_business = PostgresOperator(
                    task_id = "Create_Business_Table",
                    postgres_conn_id="redshift_conn",
                    autocommit=True,
                    sql= SQLQueries.create_table_business, 
                    dag = dag)

create_review = PostgresOperator(
                    task_id = "Create_Review_Table",
                    postgres_conn_id="redshift_conn",
                    autocommit=True,
                    sql= SQLQueries.create_table_review, 
                    dag = dag)

create_tip = PostgresOperator(
                    task_id = "Create_Tip_Table",
                    postgres_conn_id="redshift_conn",
                    autocommit=True,
                    sql= SQLQueries.create_table_tip, 
                    dag = dag)

#insert data from s3 to tables
insert_user = S3ToRedshiftTransfer(
                    s3_bucket="yelpraw",
                    s3_key="user.csv",
                    schema="PUBLIC",
                    table="user",
                    copy_options=['csv'],
                    task_id='Load_User_Data',
                    aws_conn_id='aws_credentials',
                    redshift_conn_id='redshift_conn',
                    dag = dag)

insert_business = S3ToRedshiftTransfer(
                    s3_bucket="yelpraw",
                    s3_key="business.csv",
                    schema="PUBLIC",
                    table="business",
                    copy_options=['csv'],
                    task_id='Load_Business_Data',
                    aws_conn_id='aws_credentials',
                    redshift_conn_id='redshift_conn',
                    dag = dag)

insert_review = S3ToRedshiftTransfer(
                    s3_bucket="yelpraw",
                    s3_key="review.csv",
                    schema="PUBLIC",
                    table="review",
                    copy_options=['csv'],
                    task_id='Load_Review_Data',
                    aws_conn_id='aws_credentials',
                    redshift_conn_id='redshift_conn',
                    dag = dag)

insert_tip = S3ToRedshiftTransfer(
                    s3_bucket="yelpraw",
                    s3_key="tip.csv",
                    schema="PUBLIC",
                    table="tip",
                    copy_options=['csv'],
                    task_id='Load_Tip_Data',
                    aws_conn_id='aws_credentials',
                    redshift_conn_id='redshift_conn',
                    dag = dag)

#empty the s3 bucket
delete_data = PythonOperator(
                    task_id = "Delete_s3_Data",
                    python_callable= delete_s3,
                    dag = dag)

#start etl pipeline
end_pipeline = DummyOperator(task_id = "End_ETL_Pipeline", dag = dag)

#define task flow
'''start_pipeline >> spark_submit
spark_submit >> upload_data
upload_data >> create_user >> insert_user >> insert_review
upload_data >> create_business >> insert_business >>insert_review
insert_review >> insert_tip >> delete_data
upload_data >> create_tip >> end_pipeline'''


start_pipeline >> spark_submit >> upload_data
upload_data >> create_user >> create_review
upload_data >> create_business >> create_review
create_review >> create_tip
create_tip >> insert_user >> insert_review
create_tip >> insert_business >> insert_review
insert_review >> insert_tip
insert_tip >> delete_data >> end_pipeline
