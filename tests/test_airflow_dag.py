import sys
from pathlib import Path

import pytest


pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag


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
