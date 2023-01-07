import re
from pandas import DataFrame
from typing import Any, Dict, Iterator, List, Optional, Union
import fsspec
from src.core.datamanager.base import DataReader, DataWriter


class BaseSQLReader(DataReader):
    """Classe per la lettura da big query

    Attributes:
        query (List[str]): query per scaricare il dataframe
        bh_manager (BheaviorManager): BehaviorManager
    """

    def __init__(
        self,
        *,
        query_or_path: Union[str, List[str]],
        behaviors: Optional[
            Union[Dict[str, Any], List[Dict[str, Any]]]
        ] = None,
        **kwargs,
    ) -> None:
        """Costruttore

        Args:
            query_or_path (str, List[str]): path o query da eseguire per il download del dataframe.
            behaviors (Optional[Union[Dict[str, Any], List[Dict[str, Any]]]], optional): dizionario,
                o lista di dizionari contenenti le istruzioni per instanziare i vari BehaviorManager. Defaults to None.
        """

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

        super(BaseSQLReader, self).__init__(behaviors=behaviors)
        self.query = []
        if isinstance(query_or_path, list):
            for q in query_or_path:
                if q.startswith("path:"):
                    q = read_text_file(re.sub("^path:", "", q))
                self.query.append(q)
        elif query_or_path.startswith("path:"):
            self.query.append(
                read_text_file(re.sub("^path:", "", query_or_path))
            )
        else:
            self.query.append(query_or_path)

    def read(self) -> Iterator[DataFrame]:
        dfs = []
        for q in self.query:
            dfs.append(self.execute_read(q))

        if len(dfs) > 1:
            dfs = self.bh_manager.reduce(dfs)
        assert len(dfs) == 1
        data = dfs.pop(0)
        yield data

    def execute_read(self, query: str) -> DataFrame:
        raise NotImplementedError()


class BaseSQLBatchReader(DataReader):
    """Reader per la lettura in modalità batch da big query

    Attributes:
        table (str): tabella da cui leggere
        columns (str, optional): campi da inserire nella clausola SELECT della query. Defaults to "*".
        batch_field (str, optional): campo in cui è contenuto l'id progressivo dei batch. Defaults to "batch".
    """

    def __init__(
        self,
        *,
        table: str,
        columns: Union[str, List[str]] = "*",
        batch_field: str = "batch",
        **kwargs,
    ) -> None:
        """Costruttore

        Args:
            table (str): tabella da cui leggere
            columns (str, optional): campi da inserire nella clausola SELECT della query. Defaults to "*".
            batch_field (str, optional): campo in cui è contenuto l'id progressivo dei batch. Defaults to "batch".
        """
        self.table = table
        self.batch_field = batch_field

        if isinstance(columns, list):
            self.columns = ",".join(columns)
        else:
            self.columns = columns

    def read(self) -> Iterator[DataFrame]:
        for batch_id in self.get_batches():
            yield self.execute_read(
                f"""
                select {self.columns}
                from {self.table}
                where {self.batch_field}={batch_id}"""
            )

    def execute_read(self, query: str) -> DataFrame:
        raise NotImplementedError()

    def get_batches(self) -> Iterator[Any]:
        raise NotImplementedError()


class BaseSQLWriter(DataWriter):
    """_summary_
    Args:
        tables (Union[List[str], str]): tabelle in cui scrivere
        bh_manager (BheaviorManager): BehaviorManager
        write_args (Optional[Union[dict, List[dict]]], optional): argomenti da passare alla funzione pd.to_gbq. Defaults to None.
    """

    def __init__(
        self,
        *,
        tables: Union[List[str], str],
        behaviors: Optional[
            Union[Dict[str, Any], List[Dict[str, Any]]]
        ] = None,
        write_args: Optional[Union[dict, List[dict]]] = None,
        **kwargs,
    ) -> None:
        """Costruttore

        Args:
            tables (Union[List[str], str]): tabelle in cui scrivere
            behaviors (Optional[Union[Dict[str, Any], List[Dict[str, Any]]]], optional): dizionario,
                o lista di dizionari contenenti le istruzioni per instanziare i vari BehaviorManager. Defaults to None.
            credentials_path (Optional[str], optional): path al service account. Defaults to None.
            write_args (Optional[Union[dict, List[dict]]], optional): argomenti da passare alla funzione pd.to_gbq. Defaults to None.
        """
        super(BaseSQLWriter, self).__init__(behaviors=behaviors)
        self.tables = tables
        self.write_args = write_args if write_args is not None else {}

    def write(self, data: Union[DataFrame, List[DataFrame]]) -> None:
        def write_f(df, t, pos=None):
            if isinstance(self.write_args, list):
                write_args = self.write_args[pos]
            else:
                write_args = self.write_args

            if write_args:
                self.execute_write(df, destination_table=t, **(write_args))
            else:
                # df.to_gbq(t)
                self.execute_write(df, destination_table=t)

        if isinstance(data, list) and isinstance(self.tables, list):
            for pos, (d, p) in enumerate(zip(data, self.tables)):
                write_f(d, p, pos)
        else:
            if isinstance(data, list):
                data = self.bh_manager.reduce(data)
            write_f(data, self.tables)

    def execute_write(
        self, data: DataFrame, destination_table: str, **kwargs
    ) -> None:
        raise NotImplementedError()
