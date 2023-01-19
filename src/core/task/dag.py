import logging
import subprocess
from typing import List, Optional, Union
from src.core.util.factory import Factory
import networkx as nx
import yaml
from pipe import select, where
from src.core.task.base import Task
from src.core.util.params_interpeter import ParamsInterpreter

logger = logging.getLogger(__name__)


class TaskDag:
    def __init__(
        self,
        tasks: dict,
        edges: List[List[Union[int, str]]],
        startup: Optional[dict] = None,
        cleanup: Optional[dict] = None,
    ) -> None:
        """Costruttore

        Args:
            tasks (dict): Dizionari di coppie nome_task/definizione del task
            edges (List[List[Union[int, str]]]): relazione del dag
            startup_script (Optional[str], optional): script di startup da eseguire all'inizio dell'esecizione del dag. Defaults to None.
        """
        self.task_graph = nx.DiGraph()
        for task_name, task_dict_or_path in tasks.items():
            if isinstance(task_dict_or_path, str):
                # carico la configurazione da n altro yaml file
                with open(task_dict_or_path, "r") as f:
                    try:
                        task_dict = yaml.safe_load(f)
                        task_dict = ParamsInterpreter()(task_dict)
                    except yaml.YAMLError as e:
                        print(e)
            else:
                task_dict = task_dict_or_path
            # dag ricorsivo
            if "tasks" in task_dict:
                task = TaskDag(
                    tasks=task_dict["tasks"],
                    edges=task_dict["edges"],
                    startup=task_dict.pop("startup", None),
                    cleanup=task_dict.pop("cleanup", None),
                )
            else:
                task_dict.pop("plugins", None)
                task_dict.pop("defaults", None)
                task = Task(**task_dict)

            self.task_graph.add_node(
                task_name,
                task=task,
                visited=False,
                in_queue=False,
            )

        # aggiungo le dipendenze del grafo
        for edge in edges:
            self.task_graph.add_edge(*edge)
        factory = Factory()
        if startup is None:
            self.startup = lambda: None
        else:
            self.startup = factory.create(startup)

        if cleanup is None:
            self.cleanup = lambda: None
        else:
            self.cleanup = factory.create(cleanup)

    def run(self, **kwargs) -> None:
        self.startup()
        self.run_impl(**kwargs)
        self.cleanup()

    def run_impl(self, **kwargs) -> None:
        def run(node_id: Union[int, str]) -> None:
            """Run di un singolo nodo del dag

            Args:
                node_id (Union[int, str]): id del nodo del dag

            """
            print(f"Running node:{node_id}")
            self.task_graph.nodes[node_id]["task"].run()
            self.task_graph.nodes[node_id]["visited"] = True
            print("=" * 20)

        # get the starting task
        to_visit = list(
            list(self.task_graph.in_degree())
            | where(lambda x: x[1] == 0)
            | select(lambda x: x[0])
        )

        for v in to_visit:
            self.task_graph.nodes[v]["in_queue"] = True
        # bfs execution
        while to_visit:
            task = to_visit.pop(0)
            run(task)
            next_tasks = list(
                list(self.task_graph.successors(task))
                | where(
                    lambda x: not (
                        self.task_graph.nodes[x]["visited"]
                        or self.task_graph.nodes[x]["in_queue"]
                    )
                )
            )
            # rimuovo i nodi con dipendenze non ancora eseguite
            next_tasks = filter(
                lambda x: all(
                    [
                        self.task_graph.nodes[v].get("visited", False)
                        for v in self.task_graph.predecessors(x)
                    ]
                ),
                next_tasks,
            )

            for v in next_tasks:
                self.task_graph.nodes[v]["in_queue"] = True
                to_visit.append(v)
