from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime


@dag(start_date=datetime(2022, 8, 1), schedule=None, catchup=False)
def airflow_docker_operator():

    extract_mongo_task = DockerOperator(
        task_id='extract_mongo',
        image='mongoextract:latest',
        command=["python", "extract_mongo_clickhouse.py"],
        container_name='extract-mongo',
        docker_url='unix://var/run/docker.sock',
        network_mode='de-master',
        mount_tmp_dir=False,
        auto_remove='success',
        environment={
            'CLICKHOUSE_HOST': 'clickhousedb',
            'CLICKHOUSE_USER': 'vrams',
            'CLICKHOUSE_PASSWORD': 'vinu2003',
            'MONGO_HOST': 'mongodb',
            'MONGO_USER': 'vrams',
            'MONGO_PASSWORD': 'vinu2003'
        }
    )

    fact_house_task = DockerOperator(
        task_id='fact_house',
        image='clickhousedw:latest',
        command=["run", "--models", "fact_house"],
        container_name='fact-house',
        docker_url='unix://var/run/docker.sock',
        network_mode='de-master',
        mount_tmp_dir=False,
        auto_remove='success',
        environment={
            'HOST_CLICKHOUSE': 'clickhousedb'
        }   
    )

    mart_region_house_sell_task = DockerOperator(
        task_id='mart_region_house_sell',
        image='clickhousedw:latest',
        command=["run", "--models", "mart_region_house_sell"],
        container_name='mart-region-house-sell',
        docker_url='unix://var/run/docker.sock',
        network_mode='de-master',
        mount_tmp_dir=False,
        auto_remove='success',
        environment={
            'HOST_CLICKHOUSE': 'clickhousedb'
        }   
    )

    mart_spec_house_price_task = DockerOperator(
        task_id='mart_spec_house_price',
        image='clickhousedw:latest',
        command=["run", "--models", "mart_spec_house_price"],
        container_name='mart_spec_house_price',
        docker_url='unix://var/run/docker.sock',
        network_mode='de-master',
        mount_tmp_dir=False,
        auto_remove='success',
        environment={
            'HOST_CLICKHOUSE': 'clickhousedb'
        }   
    )

    extract_mongo_task >> fact_house_task >> mart_region_house_sell_task
    fact_house_task >> mart_spec_house_price_task


airflow_docker_operator()