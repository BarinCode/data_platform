import configparser
import os

aiflow_home = os.environ.get("AIRFLOW_HOME")

# get setting
config_online = configparser.ConfigParser()
# config
config_online.read(f'{aiflow_home}/airflow-online.cfg')
# ---------------------------------------------------------------------------------------
# metastore: mysql
sql_alchemy_conn_online = config_online.get('core', 'sql_alchemy_conn')
# dag file
# dags_folder_online = config_online.get('core', 'dags_folder')
dags_folder_online = config_online.get('core', 'dags_folder')
# executor: CeleryExecutor
executor_online = config_online.get('core', 'executor')
# Are DAGs paused by default at creation
dags_are_paused_at_creation_online = config_online.get(
    "core", "dags_are_paused_at_creation")
plugins_folder_online = config_online.get("core", "plugins_folder")
# ---------------------------------------------------------------------------------------
# Default DAG view. Valid values are: ``tree``, ``graph``, ``duration``, ``gantt``, ``landing_times``
dag_default_view_online = config_online.get("webserver", "dag_default_view")
# ---------------------------------------------------------------------------------------
# How often (in seconds) to scan the DAGs directory for new files. Default to 5 minutes.
dag_dir_list_interval_online = config_online.get(
    "scheduler", "dag_dir_list_interval")
child_process_log_directory_online = config_online.get(
    "scheduler", "child_process_log_directory")
# ---------------------------------------------------------------------------------------
base_log_folder_online = config_online.get("logging", "base_log_folder")
dag_processor_manager_log_location_online = config_online.get(
    "logging", "dag_processor_manager_log_location")
# ---------------------------------------------------------------------------------------
# RabbitMQ or redis
broker_url_online = config_online.get('celery', 'broker_url')
# backend: redis or mysql
result_backend_online = config_online.get('celery', 'result_backend')
# worker的并发度，worker可以执行的任务实例的数量
worker_concurrency_online = config_online.get("celery", "worker_concurrency")
# worker日志服务的端口
worker_log_server_port_online = config_online.get(
    "celery", "worker_log_server_port")
flower_host_online = config_online.get("celery", "flower_host")
flower_port_online = config_online.get("celery", "flower_port")
# ---------------------------------------------------------------------------------------
default_queue_online = config_online.get("operators", "default_queue")
# ---------------------------------------------------------------------------------------

#
dag_default_view_online = config_online.get("webserver", "dag_default_view")


# update setting
config = configparser.ConfigParser()
config.read(f'{aiflow_home}/airflow.cfg.example')
config.set('core', 'sql_alchemy_conn', sql_alchemy_conn_online)
config.set('core', 'dags_folder', os.environ.get('DAG_FILE_HOME', "dags"))
config.set('core', 'executor', executor_online)
config.set("core", "dags_are_paused_at_creation",
           dags_are_paused_at_creation_online)
config.set("core", "plugins_folder", plugins_folder_online)

config.set("webserver", "dag_default_view", dag_default_view_online)

config.set("scheduler", "dag_dir_list_interval", dag_dir_list_interval_online)
config.set("scheduler", "child_process_log_directory",
           child_process_log_directory_online)

config.set("logging", "base_log_folder", base_log_folder_online)
config.set("logging", "dag_processor_manager_log_location",
           dag_processor_manager_log_location_online)

config.set('celery', 'broker_url', broker_url_online)
config.set('celery', 'result_backend', result_backend_online)
config.set("celery", "worker_concurrency", worker_concurrency_online)
config.set("celery", "worker_log_server_port", worker_log_server_port_online)
config.set("celery", "flower_host", flower_host_online)
config.set("celery", "flower_port", flower_port_online)

config.set("operators", "default_queue", default_queue_online)

# write airflow.cfg
with open(f'{aiflow_home}/airflow.cfg', 'w') as configfile:
    config.write(configfile)
