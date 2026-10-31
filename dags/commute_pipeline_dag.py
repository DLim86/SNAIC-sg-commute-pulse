import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG

# Airflow 3 moved these operators into the standard provider package. Importing
# both ways keeps this DAG loadable on Airflow 2.x and 3.x without an edit.
try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import ShortCircuitOperator
except ImportError:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import ShortCircuitOperator

PROJECT_DIR = str(Path(__file__).parent.parent)

# Airflow orchestrates; it does not have to share the pipeline's environment.
# COMMUTE_PYTHON lets the tasks run in the project's own uv-locked venv, so a
# task behaves the same whether Airflow runs it or it is run by hand — and the
# ML .pkl files are loaded by the scikit-learn version that trained them.
# Unset (the plain `airflow standalone` case), this is the interpreter Airflow
# itself runs on, which is the previous behaviour.
PYTHON = os.environ.get("COMMUTE_PYTHON") or sys.executable
SGT = timezone(timedelta(hours=8))

log = logging.getLogger(__name__)

default_args = {
    "owner": "snaic",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
}


EVAL_HOUR_SGT = 8


def _is_evaluation_window(**ctx):
    """Pass exactly once per day: the single */30 run whose logical time is 08:00 SGT.

    Uses the DAG run's logical time, not wall clock, so a delayed or re-run task
    still evaluates for the interval it was scheduled for. The minute check is
    what makes it once-daily — with a */30 schedule, an hour-only test passes at
    both 08:00 and 08:30.
    """
    logical = ctx.get("logical_date") or ctx.get("data_interval_start")
    if logical is None:
        logical = datetime.now(timezone.utc)
    if logical.tzinfo is None:
        logical = logical.replace(tzinfo=timezone.utc)
    local = logical.astimezone(SGT)
    fire = local.hour == EVAL_HOUR_SGT and local.minute < 30
    log.info(
        "gate_evaluate: logical %s SGT → %s",
        local.strftime("%Y-%m-%d %H:%M"), "RUN" if fire else "skip",
    )
    return fire


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
