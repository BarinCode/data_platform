import os

from jinja2 import Environment, PackageLoader
from typing import Dict, List
from json.decoder import JSONDecodeError
from loguru import logger
from datetime import datetime

from apps.core.enums import CategoryEnum
from apps.services import work
import json


class Dag:
    def __init__(self, work_instance, payload, db_session):
        self.work_instance = work_instance
        self.payload = payload
        self.db_session = db_session

    @classmethod
    def to_py(cls, template, dag):
        env = Environment(loader=PackageLoader("apps", package_path='./services/dag'))
        template = env.get_template(template)
        callback = {
            "ums": os.environ.get('USER_SERVICE'),
            "wms": os.environ.get('WORK_SERVICE')
        }
        dag_py = template.render(dag=dag, callback=callback)
        return dag_py

    def from_work(self):
        try:
            if self.work_instance.category == CategoryEnum.SQL:
                sql_nodes = [
                    {
                        "nodeId": 1,
                        "parentIds": [1],
                        "dagNodeName": "node1",
                        "workId": self.work_instance.work_id
                    }
                ]
                self.work_instance.nodes = json.dumps(sql_nodes)
            nodes: List[Dict] = json.loads(self.work_instance.nodes)
        except JSONDecodeError as exception:
            logger.error(exception)
            return {}

        airflow_nodes: List[Dict] = []
        relations = []
        for node in nodes:
            sub_work_id = node.get('workId')
            sub_work = work.get_common_work_by_id(sub_work_id, self.db_session)
            if not sub_work:
                raise Exception(f"{sub_work_id} is missing")

            connection = work.get_connection_by_id(sub_work.connection_id, self.db_session)
            if not connection:
                raise Exception(f"{sub_work.connection_id} connection is missing")

            airflow_node = {
                "node_id": "node_{}_{}".format(self.work_instance.uuid, node.get("nodeId")),
                "node_name": node.get("dagNodeName"),
                "node_type": 1,
                "ip": connection.host,
                "port": connection.port,
                "username": connection.username,
                "password": connection.password,
                "database": connection.database_name,
                "sql": self.sql_split(sub_work.executable_sql)
            }
            if self.work_instance.category is CategoryEnum.DAG:
                for parent_node_id in node.get("parentIds"):
                    parent = "node_{}_{}".format(self.work_instance.uuid, parent_node_id)
                    child = "node_{}_{}".format(self.work_instance.uuid, node.get('nodeId'))
                    relations.append("{} >> {}".format(parent, child))
            airflow_nodes.append(airflow_node)

        dag_data = {
            "dag_id": self.work_instance.uuid,
            "user_id": self.work_instance.user_id,
            "work_id": self.work_instance.work_id,
            "dag_name": self.work_instance.name,
            "start_date": self.work_instance.started_at or datetime.now(),
            "end_date": self.work_instance.ended_at,
            "schedule_interval": self.work_instance.cron_expression or "@once",
            "retries": self.work_instance.failed_retry_times,
            "retry_delay": self.work_instance.retry_delta_minutes,
            "node_relations": relations,
            "nodes": airflow_nodes
        }

        return dag_data

    def save(self, filepath, dag):
        dag_py = self.to_py("dag_template", dag)
        current_dir = os.getcwd()
        if not os.path.exists(filepath):
            os.mkdir(filepath)
        os.chdir(filepath)
        dag_name = dag.get("dag_name")
        dag_id = dag.get("dag_id")
        generated_file_name = f"{dag_name}_{dag_id}.py"
        with open(generated_file_name, 'w') as fh:
            fh.writelines(dag_py)
        os.chdir(current_dir)
        dag_file_path: str = filepath + generated_file_name
        return dag_file_path

    def sql_split(self, multiple_sql):
        result = []
        if multiple_sql is None:
            result.append("")
        elif ";" in multiple_sql:
            for sql in multiple_sql.split(";"):
                result.append(sql + ";")
        else:
            result.append(multiple_sql)
        return result
