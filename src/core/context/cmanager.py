import logging
import re
from typing import List, Optional, Union
import contextlib
import fsspec
from src.core.util.factory import Factory

logger = logging.getLogger(__name__)


class TaskContextManager:
    """Manager for"""

    def __enter__(self):
        pass

    def __exit__(self, type, value, trace):
        pass


class DBContextManager(TaskContextManager):
    """
    Context manager che si occupa di creare le tabelle
    necessarie all'esecuzione del job
    """

    def __init__(
        self,
        enter_query: str,
        exit_query: Optional[str] = None,
        finalize_query: Optional[str] = None,
        **kwargs
    ) -> None:
        def read_text_file(path: str) -> str:
            # preno i pararmetri da utilizzare per la open
            # da kwargs
            if kwargs is not None:
                open_kwargs = {
                    k: v
                    for k, v in filter(
                        lambda x: x in fsspec.open.__code__.co_varnames,
                        kwargs.items(),
                    )
                }
            else:
                open_kwargs = {}
            with fsspec.open(path, mode="rt", **open_kwargs) as f:
                return f.read()

        super(TaskContextManager, self).__init__()
        if enter_query.startswith("path:"):
            self.enter_query = read_text_file(
                re.sub("^path:", "", enter_query)
            )
        else:
            self.enter_query = enter_query

        self.exit_query = exit_query
        if self.exit_query is not None:
            if self.exit_query.startswith("path:"):
                self.exit_query = read_text_file(
                    re.sub("^path:", "", self.exit_query)
                )
            else:
                self.exit_query = exit_query

        self.finalize_query = finalize_query
        if self.finalize_query is not None:
            if self.finalize_query.startswith("path:"):
                self.finalize_query = read_text_file(
                    re.sub("^path:", "", self.finalize_query)
                )
            else:
                self.finalize_query = finalize_query

    def __enter__(self):
        logger.info("Creating context...")
        self.execute_query(self.enter_query)

    def __exit__(self, type, value, trace):
        if type is None:
            # nessuna eccezione sollevata, posso finalizzare il risultato
            logger.info("Finalizing results in Big Query ...")
            if self.finalize_query is not None:
                self.execute_query(self.finalize_query)
        logger.info("Removing Big Query context...")
        if self.exit_query is not None:
            self.execute_query(self.exit_query)

    def execute_query(self, q: Union[str, List[str]]) -> None:
        """Esegue le query passate in input

        Args:
            q (Union[str, List[str]]): query da eseguire
        """
        if isinstance(q, str):
            q = [q]
        for query in q:
            try:
                self.execute_impl(query)
            except Exception as e:
                raise e

    def execute_impl(self, query: str) -> None:
        raise NotImplementedError()


class FunctionBasedContextManager(TaskContextManager):
    """ContextManager per eseguire delle funzioni
    di startup e di cleanup, prima e dopo l'esecuzione del task context

    Attributes:
        enter_function (callable): funzione di startup
        exit_function (callable): funzione di cleanup

    """

    def __init__(self, enter_function: dict, exit_function: dict):
        """Costruttore

        Args:
            enter_function (dict): dizionario per la creazione della funzione
                di startup
            exit_function (dict): dizionario per la creazione della funzione
                di cleanup
        """
        factory = Factory()
        self.enter_function = factory.create(enter_function)
        self.exit_function = factory.create(exit_function)

    def __enter__(self):
        self.enter_function()

    def __exit__(self, type, value, trace):
        self.exit_function()


class MultipleContextManager(TaskContextManager):
    """ContexManager per la gestione di nested context manager

    Args:
        TaskContextManager (_type_): _description_
    """

    def __init__(self, context_managers: List[dict]):
        """Costruttore

        Args:
            context_managers (List[dict]): lista di dizionario
                per la creazione dei context manager
        """
        self.context_managers = []
        factory = Factory()
        for mgr in context_managers:
            self.context_managers.append(factory.create(mgr))
        self.stack = contextlib.ExitStack()

    def __enter__(self):
        _ = [self.stack.enter_context(mgr) for mgr in self.context_managers]

    def __exit__(self, type, value, trace):
        self.stack.close()
