#!/bin/bash
# Shows the state of all MTG DAG runs and task instances
psql -At -F ' ' -d airflow -c "select 'RUN', run_id, state from dag_run where dag_id='MTG' order by execution_date"
psql -At -F ' ' -d airflow -c "select to_char(execution_date,'MM-DD HH24:MI'), task_id, coalesce(state,'-'), coalesce(round(duration)::text,'') from task_instance where dag_id='MTG' order by execution_date, start_date nulls last"
