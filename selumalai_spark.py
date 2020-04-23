# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""
# [START tutorial]
from datetime import timedelta

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import datetime

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    'selumalai_spark',
    default_args=default_args,
    description='A simple tutorial DAG',
    start_date=datetime.datetime(2020, 4, 22),
    schedule_interval='0/10 * * * *',
)
# [END instantiate_dag]

# t1, t2 and t3 are examples of tasks created by instantiating operators
# [START basic_task]
first_task = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)
copy_jar_task= BashOperator(
  task_id='copy_jar',
  dag=dag,
  bash_command='cp  /Users/e192270/Desktop/Saranya_Docs/HEB/Misc/JD-Spark-WordCount/target/JD-Spark-WordCount-1.0-SNAPSHOT.jar .'
  )

spark_task = BashOperator(
    task_id='spark_java',
    bash_command='spark-submit --class com.journaldev.sparkdemo.SparkDeltaSQLExample   /Users/e192270/Desktop/Saranya_Docs/HEB/Misc/JD-Spark-WordCount/target/JD-Spark-WordCount-1.0-SNAPSHOT.jar',
    dag=dag
)

# [END basic_task]

first_task >> copy_jar_task >> spark_task
# [END tutorial]
