"""Trigger Dags #1 and #2 and do something if they succeed."""
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago


with DAG(
        'master_dag',
        schedule_interval='*/1 * * * *',  # Every 1 minute
        start_date=days_ago(0),
        catchup=False
    ) as dag_spark :

    def greeting():
        """Just check that the DAG is started in the log."""
        import logging
        logging.info('Hello World from DAG MASTER')


    Task1= SparkSubmitOperator(task_id='Task1',
        conn_id='spark_local',
        application=f'{pyspark_app_home}/spark/Task1.py',
        total_executor_cores=4,
        executor_cores=2,
        executor_memory='5g',
        driver_memory='5g',
        name='Task1',
        execution_timeout=timedelta(minutes=10),
        dag=dag_spark
    )

    Task2= SparkSubmitOperator(task_id='Task2',
        conn_id='spark_local',
        application=f'{pyspark_app_home}/spark/Task1.py',
        total_executor_cores=4,
        executor_cores=2,
        executor_memory='5g',
        driver_memory='5g',
        name='Task2',
        execution_timeout=timedelta(minutes=10),
        dag=dag_spark
    )


    Task3= SparkSubmitOperator(task_id='Task3',
        conn_id='spark_local',
        application=f'{pyspark_app_home}/spark/Task1.py',
        total_executor_cores=4,
        executor_cores=2,
        executor_memory='5g',
        driver_memory='5g',
        name='Task3',
        execution_timeout=timedelta(minutes=10),
        dag=dag_spark
    )


    Task4= SparkSubmitOperator(task_id='Task4',
        conn_id='spark_local',
        application=f'{pyspark_app_home}/spark/Task1.py',
        total_executor_cores=4,
        executor_cores=2,
        executor_memory='5g',
        driver_memory='5g',
        name='Task4',
        execution_timeout=timedelta(minutes=10),
        dag=dag_spark
    )

    Task5= SparkSubmitOperator(task_id='Task5',
        conn_id='spark_local',
        application=f'{pyspark_app_home}/spark/Task1.py',
        total_executor_cores=4,
        executor_cores=2,
        executor_memory='5g',
        driver_memory='5g',
        name='Task5',
        execution_timeout=timedelta(minutes=10),
        dag=dag_spark
     )

    Task1 >>  [Task2, Task3] >> [Task4, Task5, Task6]




