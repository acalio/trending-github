import itertools
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Union

from pandas import DataFrame
from src.core.datamanager.base import DataReader, DataWriter
from src.core.util.singleton import SingletonType


@dataclass
class SharedMemory(metaclass=SingletonType):
    """Classe rappresentante una memoria condivisa accessibile
    da varie parti del codice per poter scrivere i risultati della elaborazione.
    TODO: Rendere thread safe l'accesso e la scrittura al dizionario - rimpiazzare con dizionari ad hoc
    """

    # dizionario in cui memorizzare i dataframe
    memory: Dict[str, Union[DataFrame, List[DataFrame]]] = field(
        default_factory=dict
    )

    def __getitem__(self, key: str) -> Union[DataFrame, List[DataFrame]]:
        try:
            return self.memory[key]
        except KeyError:
            raise ValueError(f"Unkown variable {key} in shared memory")

    def __setitem__(
        self, key: str, data: Union[DataFrame, List[DataFrame]]
    ) -> None:
        self.memory[key] = data

    def remove(self, key: str) -> None:
        self.memory.pop(key, None)


class SharedMemoryReader(DataReader):
    """Classe deputata alla lettura dalla memoria condivisare

    Attributes:
        variables (list of str): nome dei dataframe da leggere dalla memoria condivisa
        bh_manager (BheaviorManager): BehaviorManager
        remove_after_read (bool):  True se si vuole elimineare la variabile dopo la sua lettura
        shared_memory (SharedMemory): memoria condivisa
    """

    def __init__(
        self,
        *,
        variables: Union[str, List[str]],
        behaviors: Optional[Union[dict, List[dict]]] = None,
        remove_after_read: bool = True,
    ):
        """Costruttore

        Args:
            variables (Union[str, List[str]]): variabili da cui effettaure la lettura de
            behaviors (Optional[Union[dict, List[dict]]], optional): Dizionario,
                o lista di dizionari contenenti le istruzioni per istanziare i vari Behavior. Defaults to None.
            remove_after_read (bool, optional): True se si vuole elimninare la variabile
                in seguito alla sua lettura. Defaults to True.
        """
        super(SharedMemoryReader, self).__init__(behaviors=behaviors)
        if isinstance(variables, str):
            self.variables = [variables]
        else:
            self.variables = variables
        self.remove_after_read = remove_after_read
        self.shared_memory = SharedMemory()

        # controllo che sia stat istanziato correttamente
        if isinstance(self.variables, list):
            assert len(self.variables) == 1 or (
                len(self.variables) > 1 and self.bh_manager is not None
            ), "You must reduce the dataframe"

    def read(self) -> Iterator[DataFrame]:
        data = [self.shared_memory[v] for v in self.variables]

        # risolvo la presenza di eventuali list di list di dataframe
        data = list(itertools.chain.from_iterable((data,)))

        if self.bh_manager is not None:
            data = self.bh_manager.reduce(data)

        assert len(data) == 1, "Reduce must produce a single dataframe"
        df = data.pop(0)

        if self.remove_after_read:
            if not isinstance(self.variables, list):
                variables = [self.variables]
            else:
                variables = self.variables
            for v in variables:
                self.shared_memory.remove(v)
        yield df


class SharedMemoryWriter(DataWriter):
    """Classe per la scrittura nella memoria condivisa

    Attributes:
        variables (list of str): nome delle variabili in cui memorizzare i dataframe nella memoria condivisa
        bh_manager (BheaviorManager): BehaviorManager
        shared_memory (SharedMemory): memoria condivisa
    """

    def __init__(
        self,
        *,
        variables: Union[str, List[str]],
        behaviors: Optional[Union[dict, List[dict]]] = None,
    ):
        """Costruttore

        Args:
            variables (Union[str, List[str]]): variabili
                in cui memorizzare i dataframe nella memoria condivisa
            behaviors (Optional[Union[dict, List[dict]]], optional): Dizionario, o lista
                di dizionari contenenti le istruzioni per
                istanziare i vari Behavior. Defaults to None.
        """
        super(SharedMemoryWriter, self).__init__(behaviors=behaviors)
        if isinstance(variables, str):
            self.variables = [variables]
        else:
            self.variables = variables
        self.shared_memory = SharedMemory()

        if isinstance(self.variables, list):
            assert len(self.variables) == 1 or (
                len(self.variables) > 1 and self.bh_manager is not None
            ), "You must reduce the dataframe"

    def write(self, data: Union[DataFrame, List[DataFrame]]):
        if isinstance(data, list):
            if self.bh_manager is not None:
                self.bh_manager.reduce(data)
        else:
            data = [data]

        for i, v in enumerate(self.variables):
            self.shared_memory[v] = data[i]
