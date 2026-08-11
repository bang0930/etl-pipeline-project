import sys
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment, StrictUndefined


pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGS_ROOT = PROJECT_ROOT / "dags"
if str(DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(DAGS_ROOT))


def test_stock_price_etl_dag_has_expected_task_flow():
    dag_bag = DagBag(dag_folder=str(DAGS_ROOT))

    assert dag_bag.import_errors == {}

    dag = dag_bag.get_dag("stock_price_etl")
    assert dag is not None
    assert dag.timetable.summary == "0 19 * * *"
    assert dag.timetable.serialize()["timezone"] == "Asia/Seoul"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert isinstance(dag.on_failure_callback, SlackWebhookNotifier)
    assert dag.on_failure_callback.slack_webhook_conn_id == "slack_default"
    assert "Stock Price ETL 실패" in dag.on_failure_callback.text
    assert "{{ task_instance.log_url }}" in dag.on_failure_callback.text
    assert "{{ dag_run.state }}" in dag.on_failure_callback.text
    assert "{{ exception }}" not in dag.on_failure_callback.text
    assert set(dag.task_ids) == {
        "extract_and_validate_raw",
        "transform_and_publish_snapshot",
        "verify_published_snapshot",
    }

    assert dag.get_task("extract_and_validate_raw").downstream_task_ids == {
        "transform_and_publish_snapshot"
    }
    assert dag.get_task(
        "transform_and_publish_snapshot"
    ).downstream_task_ids == {"verify_published_snapshot"}

    for task_id in dag.task_ids:
        task = dag.get_task(task_id)
        assert task.retries == 2
        assert task.retry_delay == timedelta(minutes=2)
        assert task.retry_exponential_backoff is False


def test_slack_failure_message_renders_without_exception_context():
    """DAG 실패 콜백 문맥에 exception이 없어도 메시지가 렌더링되어야 한다."""
    dag_bag = DagBag(dag_folder=str(DAGS_ROOT))
    dag = dag_bag.get_dag("stock_price_etl")

    rendered_message = Environment(undefined=StrictUndefined).from_string(
        dag.on_failure_callback.text
    ).render(
        dag=SimpleNamespace(dag_id="stock_price_etl"),
        run_id="failure_test",
        dag_run=SimpleNamespace(state="failed"),
        task_instance=SimpleNamespace(
            task_id="extract_and_validate_raw",
            try_number=3,
            log_url="http://localhost:8080/task-log",
        ),
    )

    assert "failed" in rendered_message
    assert "Airflow Task 로그 열기" in rendered_message
