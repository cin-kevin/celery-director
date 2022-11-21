from director import task


@task(name="TASK_A")
def task_a(*args, **kwargs):
    return "task_a"


@task(name="TASK_B")
def task_b(*args, **kwargs):
    return "task_b"


@task(name="TASK_C")
def task_c(*args, **kwargs):
    return "task_c"

@task(name="TASK_D")
def task_d(*args, **kwargs):
    return "task_d"

@task(name="TASK_1")
def task_1(*args, **kwargs):
    return "task_1"

@task(name="TASK_2")
def task_2(*args, **kwargs):
    return "task_2"


@task(name="TASK_B11")
def task_b11(*args, **kwargs):
    return "TASK_B11"


@task(name="TASK_B12")
def task_b12(*args, **kwargs):
    return "TASK_B12"

@task(name="TASK_B2")
def task_b2(*args, **kwargs):
    return "TASK_B2"
