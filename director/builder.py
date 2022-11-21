from celery import chain, group
from celery.utils import uuid

from director.exceptions import WorkflowSyntaxError
from director.extensions import cel, cel_workflows
from director.models import StatusType
from director.models.tasks import Task
from director.models.workflows import Workflow
from director.tasks.workflows import start, end

class CanvasPhase:
    def __init__(self, phase, previous) -> None:
        self.phase = phase
        self.previous = previous

class WorkflowBuilder(object):
    def __init__(self, workflow_id):
        self.workflow_id = workflow_id
        self._workflow = None
        
        self.root_type = cel_workflows.get_type(str(self.workflow))

        self.queue = cel_workflows.get_queue(str(self.workflow))
        self.custom_queues = {}

        self.tasks = cel_workflows.get_tasks(str(self.workflow))
        self.canvas = []

        # Pointer to the previous task(s)
        self.previous = []

    @property
    def workflow(self):
        if not self._workflow:
            self._workflow = Workflow.query.filter_by(id=self.workflow_id).first()
        return self._workflow

    def new_task(self, task_name, previous):
        
        task_id = uuid()

        queue = self.custom_queues.get(task_name, self.queue)

        # We create the Celery task specifying its UID
        signature = cel.tasks.get(task_name).subtask(
            kwargs={"workflow_id": self.workflow_id, "payload": self.workflow.payload},
            queue=queue,
            task_id=task_id,
        )

        if type(previous) != list:
            previous = [previous]
        # Director task has the same UID
        task = Task(
            id=task_id,
            key=task_name,
            previous=previous,
            workflow_id=self.workflow.id,
            status=StatusType.pending,
        )
        task.save()

        return signature

    def parse_queues(self):
        if type(self.queue) is dict:
            self.custom_queues = self.queue.get("customs", {})
            self.queue = self.queue.get("default", "celery")
        if type(self.queue) is not str or type(self.custom_queues) is not dict:
            raise WorkflowSyntaxError()

    def parse(self, tasks):
        canvas = []

        for task in tasks:
            print(task)
            if type(task) is str:
                signature = self.new_task(task)
                canvas.append(signature)
                print(f"Task Str: {canvas}")
            elif type(task) is dict:
                name = list(task)[0]
                if "type" not in task[name] and task[name]["type"] != "group":
                    raise WorkflowSyntaxError()

                sub_canvas_tasks = [
                    self.new_task(t, single=False) for t in task[name]["tasks"]
                ]

                sub_canvas = group(*sub_canvas_tasks, task_id=uuid())
                canvas.append(sub_canvas)
                self.previous = [s.id for s in sub_canvas_tasks]
            else:
                raise WorkflowSyntaxError()
        return canvas
    
    def parse_wf(self, tasks):
        print(f"<><><><>Parse WF<><><><> {tasks}")
        full_canvas = self.parse_recursive(tasks, None, None)
        return full_canvas
    
    def parse_recursive(self, tasks, parent_type, parent):
        # print(f"{parent_type} - {parent}")
        previous = parent.phase.id if parent!=None else []
        canvas_phase = []
        for task in tasks:  
            if type(task) is str:
                if len(canvas_phase) > 0 and parent_type!="group":
                    previous = canvas_phase[-1].previous
                signature = self.new_task(task, previous)
                canvas_phase.append(CanvasPhase(signature, signature.id))
                # print(f"str {parent_type} - {signature.id} - {signature.task} - {previous} -{canvas_phase}")
            elif type(task) is dict:
                task_name = list(task)[0]
                task_type = task[task_name]["type"]
                if "type" not in task[task_name] \
                    and (task[task_name]["type"] != "group" \
                        or task[task_name]["type"] != "chain"):
                    raise WorkflowSyntaxError()
                
                current = None
                if len(canvas_phase) > 0 and parent_type!="group":
                    current = canvas_phase[-1]
                    # print(f"current canvas - {current}")
                else:
                    current = parent
                    # print(f"current parent - {current}")
                
                
                canvas_phase.append(self.parse_recursive(task[task_name]["tasks"], task_type, current))   
                # print(f"dict {canvas_phase[-1].previous}")
            else:
                raise WorkflowSyntaxError()
                      
        if parent_type == "chain":
            chain_previous = canvas_phase[-1].phase.id
            return CanvasPhase(chain([ca.phase for ca in canvas_phase]), chain_previous)
        elif parent_type == "group":
            group_previous = [ca.previous for ca in canvas_phase]
            return CanvasPhase(group([ca.phase for ca in canvas_phase]), group_previous)
        else:
            return canvas_phase



    def build(self):
        self.parse_queues()
        # self.canvas = self.parse(self.tasks)
        print("<<><><><><><><><>")
        self.canvas_phase = self.parse_wf(self.tasks)
        self.canvas_phase.insert(0, CanvasPhase(
            start.si(self.workflow.id).set(queue=self.queue),
            []))
        self.canvas_phase.append(CanvasPhase(
            end.si(self.workflow.id).set(queue=self.queue),
            []))
                                 
        if self.root_type == "group":
            self.canvas = group([ca.phase for ca in self.canvas_phase], task_id=uuid())
        else:
            self.canvas = chain([ca.phase for ca in self.canvas_phase], task_id=uuid())
        print(self.canvas)
        print("<<><><><><><><><>")
        
        # print(self.canvas)
        # self.canvas = self.canvas_phase.phase
        # print(self.canvas)
        # self.canvas = chain(start.si(self.workflow.id).set(queue=self.queue), self.canvas)
        # print(self.canvas)
        # # self.canvas.insert(0, start.si(self.workflow.id).set(queue=self.queue))
        # self.canvas = chain(self.canvas, end.si(self.workflow.id).set(queue=self.queue))
        
        # print(self.canvas)
        # self.canvas.append(end.si(self.workflow.id).set(queue=self.queue))
        

    def run(self):
        if not self.canvas:
            self.build()

        # if self.root_type == "group":
        #     canvas = group(*self.canvas, task_id=uuid())
        # else:
        #     canvas = chain(*self.canvas, task_id=uuid())

        try:
            return self.canvas.apply_async()
        except Exception as e:
            self.workflow.status = StatusType.error
            self.workflow.save()
            raise e
