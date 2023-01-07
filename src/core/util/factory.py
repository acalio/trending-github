"""
Implementazione del factory pattern
"""
from dataclasses import dataclass, field
from typing import Any, Callable, Dict
import inspect
from functools import partial
from src.core.util.singleton import SingletonType


@dataclass
class Factory(metaclass=SingletonType):
    """Implementazione della factory"""

    # dizionario contenente coppie (tipo classe, costruttore della classe)
    constructor_dict: Dict[str, Callable[..., Any]] = field(
        default_factory=lambda: {}
    )

    def register(self, obj_type: str, constructor: Callable[..., Any]) -> None:
        """Registra un nuovo costruttore nella factory
        Parameters:
            obj_type (str) : tipo dell'oggetto
            constructor (Callable) : costruttore dell'oggetto
        """

        self.constructor_dict[obj_type] = constructor

    def unregister(self, obj_type: str) -> None:
        """Deregistra un costruttore dalla factory
        Parameters:
            obj_type (str) : tipo dell'oggetto
        """
        self.constructor_dict.pop(obj_type, None)

    def create(self, arguments: dict) -> Any:
        """Utilizza la factory per istanziare un oggetto
        registrato

        Parameters:
            arguments (dict):
                Dizionario contenente le istruzioni per istanziare
                l'oggetto di interesse
        Returns:
            nuova istanza dell'oggetto di interesse
        """
        args_copy = arguments.copy()
        # estraggo il tipo di oggetto
        if "type" not in args_copy:
            print(args_copy)
        obj_type = args_copy.pop("type")
        try:
            create_f = self.constructor_dict[obj_type]
            if inspect.isfunction(create_f):
                wrapped_f = partial(create_f, **args_copy)
                return wrapped_f
            else:
                return create_f(**args_copy)
        except KeyError as error:
            raise ValueError(f"Unkown factory:{obj_type}") from error
