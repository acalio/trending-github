from typing import Any, Dict, Iterator, List, Optional, Union

from pandas import DataFrame

from src.core.util.behaviors import BehaviorManager


class DataReader:
    """Base class per tutti gli oggetti deputati alla sola lettura
    dei dataframe

    Attributes:
        bh_manager(BehaviorManager), optional:
            Oggetto per la gestione dell'applicazione dei behavior. Defaults to None
    """

    def __init__(
        self,
        *,
        behaviors: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None
    ) -> None:
        """Costruttore

        Args:
            behaviors (Optional[Union[Dict[str, Any], List[Dict[str, Any]]]], optional):
                Dizionario o lista di dizionari necessari alla creazione del behavior manager,
                per la manipolazione di liste di dataframe. Defaults to None.
        """
        if behaviors is None:
            self.bh_manager = None
        else:
            self.bh_manager = BehaviorManager(behaviors=behaviors)

    def read(self) -> Iterator[DataFrame]:  # type: ignore
        """Legge uno o piu dataframe sulla base

        Returns:
            un iteratore sulla collezione di dataframe
            restituita da ogni operazione di lettura della
            sorgente dati impostata in fase di creazione dell'oggetto.

            Ad esempio, se si legge una tabella in batch verranno
            restituiti tanti dataframe quanti sono i batch
            a disposizione nella tabella
        """
        raise NotImplementedError()


class DataWriter:
    """Base class per tutti gli oggetti deputati alla sola scrittura
    dei dataframe

    Attributes:
        bh_manager(BehaviorManager), optional:
            Oggetto per la gestione dell'applicazione dei behavior. Defaults to None
    """

    def __init__(
        self,
        *,
        behaviors: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None
    ) -> None:
        """Costruttore

        Args:
            behaviors (Optional[Union[Dict[str, Any], List[Dict[str, Any]]]], optional):
                Dizionario o lista di dizionari necessari alla creazione del behavior manager,
                per la manipolazione di liste di dataframe. Defaults to None.
        """
        if behaviors is None:
            self.bh_manager = None
        else:
            self.bh_manager = BehaviorManager(behaviors=behaviors)

    def write(self, data: Union[DataFrame, List[DataFrame]]) -> None:
        """Riceve un dataframe o una collezione di dataframe e
        li salva secondo le istruzioni fornite in fase di creazione
        dell'oggetto

        Args:
            data: singolo dataframe oppure una lista di dataframe
                da memorizzare nei sink specificati alla creazione dell'oggetto
        """
        raise NotImplementedError()


class DataReaderDecorator(DataReader):
    def __init__(
        self,
        *,
        wrapped_reader: DataReader,
        behaviors: Optional[
            Union[Dict[str, Any], List[Dict[str, Any]]]
        ] = None,
        **kwargs
    ):
        super(DataReaderDecorator, self).__init__(behaviors = behaviors)
        self.wrapped_reader = wrapped_reader


class DataWriterDecorator(DataWriter):
    def __init__(
        self,
        *,
        wrapped_writer: DataWriter,
        behaviors: Optional[
            Union[Dict[str, Any], List[Dict[str, Any]]]
        ] = None,
        **kwargs
        ):
        super(DataWriterDecorator, self).__init__(behaviors=behaviors)
        self.wrapped_writer = wrapped_writer
    