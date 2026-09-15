from datetime import timedelta

from airflow.models import DAG

from airflow.operators.python import PythonOperator

from airflow.utils.dates import days_ago
import requests
import tarfile

# Define the path for each task needs
DIR='/home/project/airflow/dags/python_etl/staging'
source_file= 'tolldata.tgz'
vehicle_data = f'{DIR}/vehicle-data.csv'
csv_data = f'{DIR}/csv_data.csv'
tollplaza_data = f'{DIR}/tollplaza-data.tsv'
tsv_data= f'{DIR}/tsv_data.csv'
payment_data=f'{DIR}/payment-data.txt'
fixed_width_data=f'{DIR}/fixed_width_data.csv'
extracted_data=f'{DIR}/extracted_data.csv'
transformed_file = f'{DIR}/transformed_data.csv'


# DAG arguments
default_args = {
    'owner': 'BinHong',
    'start_date': days_ago(0),
    'email': ['youemail@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG difinition
dag = DAG(
    dag_id='ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)

def download_dataset():
    url='https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        target=f'{DIR}/{source_file}'
        with open(target, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    print(f"File downloaded successfully: {source_file}")

def untar_dataset():
    tgz_path=f'{DIR}/{source_file}'
    with tarfile.open(tgz_path, "r:gz") as tar:
        extract_path=f'{DIR}'
        tar.extractall(path=extract_path, filter='data')
    print("untar the file successfully")

def extract_data_from_csv():
    print("extract_data_from_csv")
    with open(vehicle_data, 'r') as sour, open(csv_data, 'w') as ex_file:
        for line in sour.readlines():
            field = line.split(',')
            ex_data = field[0]+ '\t' +field[1]+'\t' +field[2]+'\t' +field[3]+ '\n'
            ex_file.write(ex_data) 

def extract_data_from_tsv():
    print("extract_data_from_tsv")
    with open(tollplaza_data, 'r') as sour, open(tsv_data, 'w') as ex_file:
        for line in sour.readlines():
            field = line.rstrip('\r\n').split('\t')
            ex_data = field[4]+ '\t' +field[5]+'\t' +field[6]+ '\n'
            ex_file.write(ex_data)

def extract_data_from_fixed_width():
    print("extract_data_from_fixed_width")
    with open(payment_data, 'r') as sour, open(fixed_width_data, 'w') as ex_file:
        for line in sour.readlines():
            field = line.split()
            ex_data = field[9]+ '\t' +field[10]+ '\n'
            ex_file.write(ex_data)

def consolidate_data():
    with open(csv_data, 'r') as c_data, open(tsv_data, 'r') as t_data, open(fixed_width_data, 'r') as f_data, open(extracted_data, 'w') as ex_data:
        data_A = c_data.readlines()
        data_B = t_data.readlines()
        data_C = f_data.readlines()
        
        for i in range(0, len(data_A)):
            A = data_A[i].rstrip('\r\n')
            B = data_B[i].rstrip('\r\n')
            C = data_C[i].rstrip('\r\n')
            merge_data = '\t'.join([A, B, C])
            ex_data.write(merge_data + '\n')

def transform_data():
    print("transform_data")
    ls_head = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles', 
                'Tollplaza id', 'Tollplaza code', 'Type of Payment code', 'Vehicle Code']
    head_data = ','.join(ls_head)
    with open(extracted_data, 'r') as sour, open(transformed_file, 'w') as ex_file:
        ex_file.write(head_data + '\n')
        for line in sour.readlines():
            field = line.strip().split('\t')
            ex_data = field[3].upper()
            lst = field[0:3] + [ex_data] + field[4:]
            t_data = ','.join(lst)
            ex_file.write(t_data + '\n')

# Define the tasks   
download_data = PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag,
)

unzip_data = PythonOperator(
    task_id='untar_dataset',
    python_callable=untar_dataset,
    dag=dag,
)

extract_data_from_csv = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)

extract_data_from_tsv = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)
extract_data_from_fixed_width = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

consolidate_data = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Set the task dependencies
download_data >> unzip_data >> [extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width] >> consolidate_data >> transform_data