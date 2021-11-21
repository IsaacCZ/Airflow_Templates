from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import os.path
import pandas as pd
import io
import airflow.utils.dates
from airflow.decorators import dag, task
from airflow import DAG

class S3ToPostgresTransfer(BaseOperator):
    template_fields = ()

    template_ext = ()

    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            schema,
            table,
            s3_bucket,
            s3_key,
            aws_conn_postgres_id ='postgres_default',
            aws_conn_id='aws_default',
            verify=None,
            wildcard_match=False,
            copy_options=tuple(),
            autocommit=False,
            parameters=None,
            *args, **kwargs):
        super(S3ToPostgresTransfer, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_postgres_id  = aws_conn_postgres_id 
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.wildcard_match = wildcard_match
        self.copy_options = copy_options
        self.autocommit = autocommit
        self.parameters = parameters
  
    def execute(self, context):
        
        # Create an instances to connect S3 and Postgres DB.
        self.log.info(self.aws_conn_postgres_id)   
        
        self.pg_hook = PostgresHook(postgre_conn_id = self.aws_conn_postgres_id)
        self.s3 = S3Hook(aws_conn_id = self.aws_conn_id, verify = self.verify)

        self.log.info("Downloading S3 file")
        self.log.info(self.s3_key + ', ' + self.s3_bucket)

        # Validate if the file source exist or not in the bucket.
        if self.wildcard_match:
            if not self.s3.check_for_wildcard_key(self.s3_key, self.s3_bucket):
                raise AirflowException("No key matches {0}".format(self.s3_key))
            s3_key_object = self.s3.get_wildcard_key(self.s3_key, self.s3_bucket)
        else:
            if not self.s3.check_for_key(self.s3_key, self.s3_bucket):
                raise AirflowException(
                    "The key {0} does not exists".format(self.s3_key))
            s3_key_object = self.s3.get_key(self.s3_key, self.s3_bucket)

        # Read and decode the file into a list of strings.  
        list_srt_content = s3_key_object.get()['Body'].read().decode(encoding = "utf-8", errors = "ignore")
        
        schema ={           'invoice_number' :'string',
                                  'stock_code':'string',
                                  'detail':'string',
                                  'quantity':'string',
                                  'inovoice_date':'string',
                                  'unit_price':'float',
                                  'customer_id': 'string',
                                  'country': 'string' }
        


        # read a csv file with the properties required.
        df_products = pd.read_csv(io.StringIO(list_srt_content), 
                         #header=0, 
                         #delimiter=",",
                         #quotechar='"',
                         low_memory=False,
                         #parse_dates=date_cols,                                             
                         dtype=schema                         
                         )
        self.log.info(df_products)
        self.log.info(df_products.info())

        # formatting and converting the dataframe object in list to prepare the income of the next steps.
        df_products = df_products.replace(r"[\"]", r"'")
        list_df_products = df_products.values.tolist()
        list_df_products = [tuple(x) for x in list_df_products]
        self.log.info(list_df_products)   
       
        proposito_del_archivo = "r" #r es de Lectura
        codificación = "UTF-8" #Tabla de Caracteres,
                               #ISO-8859-1 codificación preferidad por
                               #Microsoft, en Linux es UTF-8
                # set the columns to insert, in this case we ignore the id, because is autogenerate.

            #with open("s3://s3-data-bootcamp-20211120192557093300000004", proposito_del_archivo, encoding=codificación) as manipulador_de_archivo:
        list_target_fields = [  'invoice_number',
                                  'stock_code',
                                  'detail',
                                  'quantity',
                                  'inovoice_date',
                                  'unit_price',
                                  'customer_id',
                                  'country' ]
        
        self.current_table = self.table #self.schema + '.' + self.table
        self.pg_hook.insert_rows(self.current_table,  
                                 list_df_products, 
                                 target_fields = list_target_fields, 
                                 commit_every = 1000,
                                 replace = False)


default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('dag_insert_data', default_args = default_args, schedule_interval = '@daily')

process_dag = S3ToPostgresTransfer(
    task_id = 'dag_s3_to_postgres',
    schema = 'dbname',
    table= 'user_purchase',
    s3_bucket = 's3-data-bootcamp-20211120192557093300000004',
    s3_key =  'user',
    aws_conn_postgres_id = 'postgres_default',
    aws_conn_id = 'my_conn_S3',   
    dag = dag
)

process_dag
