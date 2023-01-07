from typing import List, Optional, Union

from pandas import DataFrame
from src.core.util.factory import Factory

from .base import DataReader, DataWriter


class MultipleSourceReader(DataReader):
    """Class per la lettura di dataframe
    da sorgenti differenti, utilizzando datareader
    di tipo diverso
    TODO: Cambiare behaviors in Dict[dict]

    Attributes:
        readers (List[DataReader]): Lista di DataReader
        bh_manager (BehaviorManager): BehaviorManager

    """

    def __init__(
        self, *, readers: List[dict], behaviors: Union[dict, List[dict]]
    ) -> None:
        """Costruttore

        Args:
            readers (List[dict]): Lista di dizionari contenente le istruzioni per istanziare i vari DataReader
            behaviors (Union[dict, List[dict]]): Dizionario, o lista di dizionari contenenti le istruzioni per istanziare i vari Behavior
        """
        super(MultipleSourceReader, self).__init__(behaviors=behaviors)
        factory = Factory()
        self.readers: List[DataReader] = []
        for reader in readers:
            self.readers.append(factory.create(reader))

        # Controllo che ci sia almeno un data reader nella lista e che nel caso in cui
        # venga specificato piu di un data reader il behavior manager sia istanziato per restituire un singolo data frame
        assert (
            self.readers is not None and len(self.readers) > 0
        ), "You must provide at least one DataReader"
        if len(self.readers) > 0:
            assert (
                self.bh_manager is not None
            ), "You must provide a BehaviorManager"

    def read(self):
        """Legge dalle varie sorgenti

        Yields:
            DataFrame : dataframe risultato della lettura dei vari data reader
        """
        dfs = []
        for reader in self.readers:
            for df in reader.read():
                dfs.append(df)

        if self.bh_manager is not None:
            dfs = self.bh_manager.reduce(dfs)
        assert len(dfs) == 1, "Reduce must return a single dataframe"
        yield dfs.pop(0)


class MultipleSinksWriter(DataWriter):
    """DataWriter per la scrittura su sink differenti.
    Permette di scrivere un singolo dataframe su sink differenti,
    oppure di scrivere una lista di dataframe su sink differenti
    TODO: Cambiare behaviors in dict of dict

    Attributes:
        writers (List[DataWriter]): Lista di DataWriter
        bh_manager (BehaviorManager): BehaviorManager
    """

    def __init__(
        self,
        *,
        writers: List[dict],
        behaviors: Optional[Union[dict, List[dict]]] = None
    ) -> None:
        """Costruttore

        Args:
            writers (List[dict]):
                lista di dizionari contenenti le istruzioni per istanziare i vari data writer
            behaviors (Optional[Union[dict, List[dict]]], optional):
                lista di dizionari con le istruzioni per istanziare i vari behavior. Defaults to None.
        """
        super(MultipleSinksWriter, self).__init__(behaviors=behaviors)
        factory = Factory()
        self.writers: List[DataWriter] = []
        for reader in writers:
            self.writers.append(factory.create(reader))
        # Controllo che ci sia almeno un datawriter"""
        assert (
            self.writers is not None and len(self.writers) > 0
        ), "You must provide at least a DataWriter"

    def write(self, data: Union[DataFrame, List[DataFrame]]) -> None:
        """Scrive i dataframe in input sui vari sink

        Args:
            data (Union[DataFrame, List[DataFrame]]): dataframe o lista di dataframe da scrivere
        """
        if isinstance(data, list) and self.bh_manager is not None:
            data = self.bh_manager.reduce(data)

        if isinstance(data, list):
            _ = [w.write(data=data[i]) for i, w in enumerate(self.writers)]
        else:
            _ = [w.write(data=data) for w in self.writers]
