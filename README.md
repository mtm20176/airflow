# Airflow

An example Airflow repo

includes 2 DAGs that use the BashOperator. One just echos Hello World, the other uses pandas to read and analyze public datasets.

##### clean.coder.coder.com

[![Open in Coder](https://clean.demo.coder.com/static/image/embed-button.svg)](https://clean.demo.coder.com/wac/build?template_oauth_service=625ff6b7-9e0fbb71f34a2ed66ae5a2e5&template_url=https://github.com/mtm20176/airflow_wac.git&template_ref=main&template_filepath=.coder/coder.yaml)

##### Definitions

###### DAG (Directed Acyclic Graph)

In Airflow, a DAG – or a Directed Acyclic Graph – is a collection of all the tasks you want to run, organized in a way that reflects their relationships and dependencies. A DAG is defined in a Python script, which represents the DAGs structure (tasks and their dependencies) as code.