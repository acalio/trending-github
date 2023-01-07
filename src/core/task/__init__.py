from typing import Protocol


class TaskIf(Protocol):
    """Interfaccia Comune a tutti i task"""

    def run(self, **kwargs) -> None:
        pass
