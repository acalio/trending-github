from typing import Any, Dict, Iterator, List, Optional, Union
import pandas as pd
import sqlalchemy as sa
from src.core.datamanager.sql import (
    BaseSQLReader,
    BaseSQLBatchReader,
    BaseSQLWriter,
)
import logging

logger = logging.getLogger(__name__)


class SQLReader(BaseSQLReader):
    """Classe per la lettura da big query

    Attributes:
        query (str): query per scaricare il dataframe
        reading_func (callable): wrapper per la funzione pd.read_gbq
        bh_manager (BheaviorManager): BehaviorManager
        dtypes_dict (dict): dizionario campo->tipo per effettuare la
            conversione dei tipi sul dataframe restituito dalla query
    """

    def __init__(
        self,
        *,
        query_or_path: str,
        behaviors: Optional[
            Union[Dict[str, Any], List[Dict[str, Any]]]
        ] = None,
        **kwargs,
    ) -> None:
        """Costruttore

        Args:
            query_or_path (str): path o query da eseguire per il download del dataframe.
            credentials_path (Optional[str], optional): path al service account credentials. Defaults to None.
            behaviors (Optional[Union[Dict[str, Any], List[Dict[str, Any]]]], optional): dizionario,
                o lista di dizionari contenenti le istruzioni per instanziare i vari BehaviorManager. Defaults to None.
            dtypes_dict (Optional[dict], optional): dizionario per settare i tipi dele colonne. Defaults to None.
        """
        super(SQLReader, self).__init__(
            query_or_path=query_or_path, behaviors=behaviors
        )

        self.engine = sa.create_engine(**kwargs)

    def execute_read(self, query: str) -> pd.DataFrame:
        with self.engine.connect() as conn:
            data = pd.read_sql(sa.sql.text(query), con=conn)
            return data


class SQLBatchReader(BaseSQLBatchReader):
    """Reader per la lettura in modalità batch da big query

    Attributes:
        table (str): tabella da cui leggere
        columns (str, optional): campi da inserire nella clausola SELECT della query. Defaults to "*".
        batch_field (str, optional): campo in cui è contenuto l'id progressivo dei batch. Defaults to "batch".
        dtypes_dict (Optional[dict], optional): dizionario per settare i tipi dele colonne. Defaults to None.
        bh_manager (BheaviorManager): BehaviorManager
        reading_func (callable): wrapper per la funzione pd.read_gbq
    """

    def __init__(
        self,
        *,
        table: str,
        columns: str = "*",
        batch_field: str = "batch",
        **kwargs,
    ) -> None:
        """Costruttore

        Args:
            table (str): tabella da cui leggere
            columns (str, optional): campi da inserire nella clausola SELECT della query. Defaults to "*".
            batch_field (str, optional): campo in cui è contenuto l'id progressivo dei batch. Defaults to "batch".
            credentials_path (Optional[str], optional): path al service account. Defaults to None.
            dtypes_dict (Optional[dict], optional): dizionario per settare i tipi dele colonne. Defaults to None.
            behaviors (Optional[Union[Dict[str, Any], List[Dict[str, Any]]]], optional): dizionario,
                o lista di dizionari contenenti le istruzioni per instanziare i vari BehaviorManager. Defaults to None.
        """
        super(SQLBatchReader, self).__init__(
            table=table, columns=columns, batch_field=batch_field
        )
        self.engine = sa.create_engine(**kwargs)

    def get_batches(self) -> Iterator[Any]:
        query = f"""
            select distinct {self.batch_field}
            from {self.table}
            order by {self.batch_field} asc
            """
        with self.engine.connect() as conn:
            for batch in list(
                map(
                    lambda x: x[self.batch_field],
                    map(
                        lambda x: dict(x),
                        conn.execute(sa.text(query)).all(),
                    ),
                )
            ):
                yield batch

    def execute_read(self, query: str) -> pd.DataFrame:
        with self.engine.connect() as conn:
            data = pd.read_sql(sa.sql.text(query), con=conn)
            return data


class SQLWriter(BaseSQLWriter):
    """Classe per la scrittura su server sql
    Args:
        tables (Union[List[str], str]): tabelle in cui scrivere
        writing_func (callable): wrapper per la funzione pd.to_gbq
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

        super(SQLWriter, self).__init__(
            tables=tables,
            behaviors=behaviors,
            write_args=write_args,
        )
        self.engine = sa.create_engine(**kwargs)

    def execute_write(
        self, data: pd.DataFrame, destination_table: str, **kwargs
    ) -> None:
        data.to_sql(name=destination_table, con=self.engine, **kwargs)
