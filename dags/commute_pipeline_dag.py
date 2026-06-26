import sys
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

PROJECT_DIR = str(Path(__file__).parent.parent)
PYTHON = sys.executable
SGT = timezone(timedelta(hours=8))

log = logging.getLogger(__name__)

default_args = {
    "owner": "snaic",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
}


def _is_evaluation_window(**ctx):
    """Only let evaluate_model run during the 8 AM SGT half-hour window."""
    return datetime.now(timezone.utc).astimezone(SGT).hour == 8


with DAG(
    dag_id="commute_pipeline",
    description="SG Commute Pulse — ingest → transform → ML predict → evaluate",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["commute", "ml", "singapore"],
) as dag:

    schema_check = BashOperator(
        task_id="schema_check",
        bash_command=f"cd '{PROJECT_DIR}' && '{PYTHON}' scripts/schema.py",
    )

    ingest = BashOperator(
        task_id="ingest",
        bash_command=f"cd '{PROJECT_DIR}' && '{PYTHON}' scripts/ingest.py",
    )

    transform = BashOperator(
        task_id="transform",
        bash_command=f"cd '{PROJECT_DIR}' && '{PYTHON}' scripts/transform.py",
    )

    predict_commute = BashOperator(
        task_id="predict_commute",
        bash_command=f"cd '{PROJECT_DIR}' && '{PYTHON}' scripts/model.py --predict",
    )

    backfill_actuals = BashOperator(
        task_id="backfill_actuals",
        bash_command=f"cd '{PROJECT_DIR}' && '{PYTHON}' scripts/model.py --backfill",
    )

    gate_evaluate = ShortCircuitOperator(
        task_id="gate_evaluate",
        python_callable=_is_evaluation_window,
    )

    evaluate_model = BashOperator(
        task_id="evaluate_model",
        bash_command=f"cd '{PROJECT_DIR}' && '{PYTHON}' scripts/model.py --evaluate",
    )

    # fmt: off
    schema_check >> ingest >> transform >> predict_commute >> backfill_actuals >> gate_evaluate >> evaluate_model
    # fmt: on
