from uuid import UUID

import pytest

from director.exceptions import WorkflowNotFound
from director.tasks.workflows import start, end
from director.models.tasks import Task
from director.models.workflows import Workflow

from tests.conftest import _remove_keys


def test_create_unknown_workflow(create_builder):
    with pytest.raises(WorkflowNotFound):
        create_builder("project", "UNKNOW_WORKFLOW", {})


def test_build_one_task(create_builder):
    data, builder = create_builder("example", "WORKFLOW", {"foo": "bar"})
    assert data == {
        "name": "WORKFLOW",
        "payload": {"foo": "bar"},
        "project": "example",
        "fullname": "example.WORKFLOW",
        "status": "pending",
        "periodic": False,
    }

    assert len(builder.canvas.tasks) == 3
    assert builder.canvas.tasks[0].task == "director.tasks.workflows.start"
    assert builder.canvas.tasks[-1].task == "director.tasks.workflows.end"
    assert builder.canvas.tasks[1].task == "TASK_EXAMPLE"


def test_build_chained_tasks(app, create_builder):
    keys = ["id", "created", "updated", "task"]
    data, builder = create_builder("example", "SIMPLE_CHAIN", {"foo": "bar"})
    assert data == {
        "name": "SIMPLE_CHAIN",
        "payload": {"foo": "bar"},
        "project": "example",
        "fullname": "example.SIMPLE_CHAIN",
        "status": "pending",
        "periodic": False,
    }

    # Check the Celery canvas
    assert len(builder.canvas.tasks) == 5
    assert builder.canvas.tasks[0].task == "director.tasks.workflows.start"
    assert builder.canvas.tasks[-1].task == "director.tasks.workflows.end"
    assert builder.canvas.tasks[1].task == "TASK_A"
    assert builder.canvas.tasks[2].task == "TASK_B"
    assert builder.canvas.tasks[3].task == "TASK_C"

    # Check the tasks in database (including previouses ID)
    with app.app_context():
        tasks = Task.query.order_by(Task.created_at.asc()).all()
    assert len(tasks) == 3
    assert _remove_keys(tasks[0].to_dict(), keys) == {
        "key": "TASK_A",
        "previous": [],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[1].to_dict(), keys) == {
        "key": "TASK_B",
        "previous": [str(tasks[0].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }

    assert _remove_keys(tasks[2].to_dict(), keys) == {
        "key": "TASK_C",
        "previous": [str(tasks[1].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }


def test_build_grouped_tasks(app, create_builder):
    keys = ["id", "created", "updated", "task"]
    data, builder = create_builder("example", "SIMPLE_GROUP", {"foo": "bar"})
    assert data == {
        "name": "SIMPLE_GROUP",
        "payload": {"foo": "bar"},
        "project": "example",
        "fullname": "example.SIMPLE_GROUP",
        "status": "pending",
        "periodic": False,
    }

    # Check the Celery canvas
    assert len(builder.canvas.tasks) == 4
    assert builder.canvas.tasks[0].task == "director.tasks.workflows.start"
    assert builder.canvas.tasks[-1].task == "director.tasks.workflows.end"
    assert builder.canvas.tasks[1].task == "TASK_A"
    assert builder.canvas.tasks[2].task == "celery.group"
    group_tasks = builder.canvas.tasks[2].tasks
    assert len(group_tasks) == 2
    assert [group_tasks[0].task, group_tasks[1].task] == [
        "TASK_B",
        "TASK_C",
    ]

    # Check the tasks in database (including previouses ID)
    with app.app_context():
        tasks = Task.query.order_by(Task.created_at.asc()).all()
    assert len(tasks) == 3
    print(tasks[0].id)
    print(tasks[1].previous)
    print(tasks[2].previous)
    assert _remove_keys(tasks[0].to_dict(), keys) == {
        "key": "TASK_A",
        "previous": [],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[1].to_dict(), keys) == {
        "key": "TASK_B",
        "previous": [str(tasks[0].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[2].to_dict(), keys) == {
        "key": "TASK_C",
        "previous": [str(tasks[0].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }


def test_build_tasks_with_routing(create_builder):
    data, builder = create_builder("example", "TASK_ROUTING", {"foo": "bar"})

    assert len(builder.canvas.tasks) == 4
    assert builder.canvas.tasks[0].task == "director.tasks.workflows.start"
    assert builder.canvas.tasks[-1].task == "director.tasks.workflows.end"

    # Checl the task queues in canvas
    assert builder.canvas.tasks[1].task == "TASK_A"
    assert builder.canvas.tasks[1].options["queue"] == "q1"

    assert builder.canvas.tasks[2].task == "celery.group"
    group_tasks = builder.canvas.tasks[2].tasks
    assert len(group_tasks) == 2

    assert group_tasks[0].task == "TASK_B"
    assert group_tasks[1].task == "TASK_C"

    assert group_tasks[0].options["queue"] == "q2"
    assert group_tasks[1].options["queue"] == "q1"

def test_build_nested_tasks_1_group_chained(app, create_builder):
    keys = ["id", "created", "updated", "task"]
    data, builder = create_builder("example", "NESTED_WORKFLOW", {"foo": "bar"})
    assert data == {
        "name": "NESTED_WORKFLOW",
        "payload": {"foo": "bar"},
        "project": "example",
        "fullname": "example.NESTED_WORKFLOW",
        "status": "pending",
        "periodic": False,
    }

    # Check the Celery canvas
    assert len(builder.canvas) == 6
    assert builder.canvas.tasks[0].task == "director.tasks.workflows.start"
    assert builder.canvas.tasks[-1].task == "director.tasks.workflows.end"
    assert builder.canvas.tasks[1].task == "TASK_A"
    assert builder.canvas.tasks[2].task == "celery.group"
    group_tasks = builder.canvas.tasks[2].tasks
    print(builder.canvas.tasks[2].tasks)
    assert len(group_tasks) == 2
    assert [group_tasks[0].task, group_tasks[1].task] == [
        "celery.chain",
        "TASK_C",
    ]
    chain_nested_tasks = group_tasks[0].tasks
    assert len(chain_nested_tasks) == 2
    assert [chain_nested_tasks[0].task, chain_nested_tasks[1].task] == [
        "TASK_1",
        "TASK_2",
    ]
    # assert builder.canvas[3].phase.task == "TASK_D"
    # Check the tasks in database (including previouses ID)
    with app.app_context():
        tasks = Task.query.order_by(Task.created_at.asc()).all()
    assert len(tasks) == 5
    assert _remove_keys(tasks[0].to_dict(), keys) == {
        "key": "TASK_A",
        "previous": [],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[1].to_dict(), keys) == {
        "key": "TASK_1",
        "previous": [str(tasks[0].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[2].to_dict(), keys) == {
        "key": "TASK_2",
        "previous": [str(tasks[1].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[3].to_dict(), keys) == {
        "key": "TASK_C",
        "previous": [str(tasks[0].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[4].to_dict(), keys) == {
        "key": "TASK_D",
        "previous": [str(tasks[2].to_dict()["id"]), str(tasks[3].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    

def test_build_nested_tasks_2_group_chained(app, create_builder):
    keys = ["id", "created", "updated", "task"]
    data, builder = create_builder("example", "NESTED_WORKFLOW_2_GROUP_CHAINED", {"foo": "bar"})
    assert data == {
        "name": "NESTED_WORKFLOW_2_GROUP_CHAINED",
        "payload": {"foo": "bar"},
        "project": "example",
        "fullname": "example.NESTED_WORKFLOW_2_GROUP_CHAINED",
        "status": "pending",
        "periodic": False,
    }

    # Check the Celery canvas
    assert len(builder.canvas.tasks) == 5
    assert builder.canvas.tasks[0].task == "director.tasks.workflows.start"
    assert builder.canvas.tasks[-1].task == "director.tasks.workflows.end"
    assert builder.canvas.tasks[1].task == "TASK_A"
    assert builder.canvas.tasks[2].task == "celery.group"
    assert builder.canvas.tasks[3].task == "TASK_D"
    group_tasks = builder.canvas.tasks[2].tasks
    print(builder.canvas.tasks[2].tasks)
    assert len(group_tasks) == 2
    assert [group_tasks[0].task, group_tasks[1].task] == [
        "celery.chain",
        "celery.chain",
    ]
    chain_nested_tasks = group_tasks[0].tasks
    assert len(chain_nested_tasks) == 2
    assert [chain_nested_tasks[0].task, chain_nested_tasks[1].task] == [
        "TASK_1",
        "TASK_2",
    ]
    chain_nested_tasks = group_tasks[1].tasks
    assert len(chain_nested_tasks) == 2
    assert [chain_nested_tasks[0].task, chain_nested_tasks[1].task] == [
        "TASK_1",
        "TASK_2",
    ]
    # assert builder.canvas[3].phase.task == "TASK_D"
    # Check the tasks in database (including previouses ID)
    with app.app_context():
        tasks = Task.query.order_by(Task.created_at.asc()).all()
    assert len(tasks) == 6
    assert _remove_keys(tasks[0].to_dict(), keys) == {
        "key": "TASK_A",
        "previous": [],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[1].to_dict(), keys) == {
        "key": "TASK_1",
        "previous": [str(tasks[0].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[2].to_dict(), keys) == {
        "key": "TASK_2",
        "previous": [str(tasks[1].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[3].to_dict(), keys) == {
        "key": "TASK_1",
        "previous": [str(tasks[0].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[4].to_dict(), keys) == {
        "key": "TASK_2",
        "previous": [str(tasks[3].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[5].to_dict(), keys) == {
        "key": "TASK_D",
        "previous": [str(tasks[2].to_dict()["id"]), str(tasks[4].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    
    
def test_build_nested_tasks_2_multi_group(app, create_builder):
    keys = ["id", "created", "updated", "task"]
    data, builder = create_builder("example", "NESTED_WORKFLOW_2_MULTI_GROUP", {"foo": "bar"})
    assert data == {
        "name": "NESTED_WORKFLOW_2_MULTI_GROUP",
        "payload": {"foo": "bar"},
        "project": "example",
        "fullname": "example.NESTED_WORKFLOW_2_MULTI_GROUP",
        "status": "pending",
        "periodic": False,
    }

    # Check the Celery canvas
    assert len(builder.canvas.tasks) == 7
    assert builder.canvas.tasks[0].task == "director.tasks.workflows.start"
    assert builder.canvas.tasks[-1].task == "director.tasks.workflows.end"
    assert builder.canvas.tasks[1].task == "TASK_A"
    assert builder.canvas.tasks[2].task == "celery.group"
    assert builder.canvas.tasks[3].task == "TASK_C"
    assert builder.canvas.tasks[4].task == "celery.group"
    assert builder.canvas.tasks[5].task == "TASK_D"
    group_tasks = builder.canvas.tasks[2].tasks
    print(builder.canvas.tasks[2].tasks)
    assert len(group_tasks) == 2
    assert [group_tasks[0].task, group_tasks[1].task] == [
        "celery.chain",
        "celery.chain",
    ]
    chain_nested_tasks = group_tasks[0].tasks
    assert len(chain_nested_tasks) == 2
    assert [chain_nested_tasks[0].task, chain_nested_tasks[1].task] == [
        "TASK_1",
        "TASK_2",
    ]
    chain_nested_tasks = group_tasks[1].tasks
    assert len(chain_nested_tasks) == 2
    assert [chain_nested_tasks[0].task, chain_nested_tasks[1].task] == [
        "TASK_1",
        "TASK_2",
    ]
    # assert builder.canvas[3].phase.task == "TASK_D"
    # Check the tasks in database (including previouses ID)
    with app.app_context():
        tasks = Task.query.order_by(Task.created_at.asc()).all()
    assert len(tasks) == 11
    assert _remove_keys(tasks[0].to_dict(), keys) == {
        "key": "TASK_A",
        "previous": [],
        "result": None,
        "status": "pending",
    }
    
    # Group 1
    assert _remove_keys(tasks[1].to_dict(), keys) == {
        "key": "TASK_1",
        "previous": [str(tasks[0].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[2].to_dict(), keys) == {
        "key": "TASK_2",
        "previous": [str(tasks[1].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[3].to_dict(), keys) == {
        "key": "TASK_1",
        "previous": [str(tasks[0].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[4].to_dict(), keys) == {
        "key": "TASK_2",
        "previous": [str(tasks[3].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[5].to_dict(), keys) == {
        "key": "TASK_C",
        "previous": [str(tasks[2].to_dict()["id"]), str(tasks[4].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    
    # Group 2
    assert _remove_keys(tasks[6].to_dict(), keys) == {
        "key": "TASK_1",
        "previous": [str(tasks[5].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[7].to_dict(), keys) == {
        "key": "TASK_2",
        "previous": [str(tasks[6].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[8].to_dict(), keys) == {
        "key": "TASK_1",
        "previous": [str(tasks[5].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[9].to_dict(), keys) == {
        "key": "TASK_2",
        "previous": [str(tasks[8].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[10].to_dict(), keys) == {
        "key": "TASK_D",
        "previous": [str(tasks[7].to_dict()["id"]), str(tasks[9].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    


def test_build_nested_tasks_3_lvl(app, create_builder):
    keys = ["id", "created", "updated", "task"]
    data, builder = create_builder("example", "NETSTED_3LVL", {"foo": "bar"})
    assert data == {
        "name": "NETSTED_3LVL",
        "payload": {"foo": "bar"},
        "project": "example",
        "fullname": "example.NETSTED_3LVL",
        "status": "pending",
        "periodic": False,
    }

    # Check the Celery canvas
    assert len(builder.canvas.tasks) == 5
    assert builder.canvas.tasks[0].task == "director.tasks.workflows.start"
    assert builder.canvas.tasks[-1].task == "director.tasks.workflows.end"
    assert builder.canvas.tasks[1].task == "TASK_A"
    assert builder.canvas.tasks[2].task == "celery.group"
    assert builder.canvas.tasks[3].task == "TASK_D"
    group_tasks = builder.canvas.tasks[2].tasks
    # print(builder.canvas.tasks[2].tasks)
    assert len(group_tasks) == 2
    assert [group_tasks[0].task, group_tasks[1].task] == [
        "celery.chord",
        "TASK_C",
    ]
    
    chain_nested_tasks = group_tasks[0].tasks
    assert len(chain_nested_tasks) == 2
    assert [chain_nested_tasks[0].task, chain_nested_tasks[1].task] == [
        "TASK_B11",
        "TASK_B12",
    ]

    # Check the tasks in database (including previouses ID)
    with app.app_context():
        tasks = Task.query.order_by(Task.created_at.asc()).all()
    assert len(tasks) == 6
    assert _remove_keys(tasks[0].to_dict(), keys) == {
        "key": "TASK_A",
        "previous": [],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[1].to_dict(), keys) == {
        "key": "TASK_B11",
        "previous": [str(tasks[0].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[2].to_dict(), keys) == {
        "key": "TASK_B12",
        "previous": [str(tasks[0].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[3].to_dict(), keys) == {
        "key": "TASK_B2",
        "previous": [str(tasks[1].to_dict()["id"]), str(tasks[2].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[4].to_dict(), keys) == {
        "key": "TASK_C",
        "previous": [str(tasks[0].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
    assert _remove_keys(tasks[5].to_dict(), keys) == {
        "key": "TASK_D",
        "previous": [str(tasks[3].to_dict()["id"]), str(tasks[4].to_dict()["id"])],
        "result": None,
        "status": "pending",
    }
