from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define la ruta a los dos scripts de Python
path_to_script_1 = "/home/user/anaconda3/envs/indicadores_mundiales/indicadores_mundiales/script_ingesta_datos.py"
path_to_script_2 = "/home/user/anaconda3/envs/indicadores_mundiales/indicadores_mundiales/script_insert_DB.py"

# Define el nombre del ambiente de Conda
conda_env_name = "indicadores_mundiales"

# Define los argumentos por defecto para el DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Define una función para ejecutar el primer script de Python
def run_script_1():
    import subprocess
    # Ejecuta el primer script de Python usando Conda para activar el ambiente y luego llama al script
    subprocess.call(['bash', '-c', f'source activate {conda_env_name} && python {path_to_script_1}'])

# Define una función para ejecutar el segundo script de Python
def run_script_2():
    import subprocess
    # Ejecuta el segundo script de Python usando Conda para activar el ambiente y luego llama al script
    subprocess.call(['bash', '-c', f'source activate {conda_env_name} && python {path_to_script_2}'])

# Define el DAG con el nombre "automatizacion_data" y los argumentos por defecto
# El DAG está programado para ejecutarse diariamente a las 8:00
with DAG('automatizacion_data', default_args=default_args, schedule_interval='0 0 1-5 * *') as dag:

    # Crea un operador Bash que descargue la información y la transforme
    t1 = BashOperator(
        task_id='descarga_y_transformacion',
        bash_command=f'source activate {conda_env_name} && python ' + path_to_script_1)

    # Crea un operador Python que ejecute el segundo script y envíe los resultados a la base de datos
    t2 = PythonOperator(
        task_id='transformacion_y_envio',
        python_callable=run_script_2)

    # Define la dependencia entre los dos operadores
    # Primero se ejecuta el operador t1 y luego el operador t2
    t1 >> t2 