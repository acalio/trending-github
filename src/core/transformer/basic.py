from typing import List, Optional, Union

import pandas as pd

from src.core.transformer.base import BaseTransformer


class DummyTransformer(BaseTransformer):
    """Transformer che non esegue nessuna trasformazione."""

    def __init__(
        self,
        *,
        print_shape: bool = False,
        check_condition: Optional[str] = None,
        condition_message: Optional[str] = None,
    ) -> None:
        """Costruttore

        Args:
            print_shape (bool, optional): True se si vuole solo
                stampare la shape del dataframe. Defaults to False.
            check_condition (Optional[str], optional): string su cui
                applicare l'eval. Defaults to None.
            condition_message (Optional[str], optional): messaggio da stampare
                a video. Defaults to None.
        """
        self.print_shape = print_shape
        self.check_condition = check_condition
        self.condition_message = condition_message

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        if self.print_shape:
            print(f"{__class__.__name__}: data.shape: {data.shape}")
            return data
        if self.check_condition:
            if self.condition_message:
                print(self.condition_message)
            print(eval(self.check_condition))
        return data


class SwissKnife(BaseTransformer):
    """Classe per eseguire operazioni comuni sui dataframe

    Attributes:

        dataframe_attr (str): nome di un metodo
            della classe dataframe
        col_to_apply (str, List[str], optional): colonne
            coinvolte nella chiamata al metodo. Defaults to None.
        col_to_store (str, optional): colonna in cui
            salvare il risultato. Defaults to None.
        fargs (dict, optional): argomenti da passare
            alla chiamata al metodo
    """

    def __init__(
        self,
        dataframe_attr: str,
        col_to_apply: Optional[Union[str, List[str]]] = None,
        col_to_store: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Costruttore

        Args:
            dataframe_attr (str): metodo della
                classe dataframe da richiamare
            col_to_apply (Optional[Union[str, List[str]]],
                optional): colonne su cui applicare
                il metodo. Defaults to None.
            col_to_store (Optional[str], optional): colonna in cui
                salvare il risultato. Defaults to None.
        """
        # Mettere controllo per verificare che esista
        assert dataframe_attr in dir(
            pd.DataFrame
        ), f"{__name__}: {dataframe_attr} is not available"
        self.dataframe_attr = dataframe_attr
        self.fargs = kwargs
        self.col_to_apply = col_to_apply
        self.col_to_store = col_to_store
        # verifico se nel dizionario fargs ci sono delle lambda
        # su cui fare l'eval per trasformare in oggetti python
        for k, v in self.fargs.items():
            if isinstance(v, str) and v.startswith("lambda"):
                self.fargs[k] = eval(v)

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        if isinstance(self.col_to_apply, str):
            if self.col_to_store is None:
                data = getattr(data[self.col_to_apply], self.dataframe_attr)(
                    **(self.fargs)
                )
            else:
                data[self.col_to_store] = getattr(
                    data[self.col_to_apply], self.dataframe_attr
                )(**(self.fargs))

            return data
        if self.col_to_apply is None:
            col_to_apply = data.columns
        else:
            col_to_apply = self.col_to_apply
        if self.col_to_store:
            data[self.col_to_store] = getattr(
                data[col_to_apply], self.dataframe_attr
            )(**(self.fargs))
        else:
            data = getattr(data[col_to_apply], self.dataframe_attr)(
                **(self.fargs)
            )
        return data


class GroupByTransformer(BaseTransformer):
    """Transformer per eseguire operazioni di groupby

    Attributes:
        group_columns (Union[str, List[str]]): colonne su cui eseguire la groupby
        agg_function (Union[str, dict]): funzione di aggregazione
        main_columns (Optional[Union[List[str], str]], optional): colonne su cui applicare l'aggregazione. Defaults to None.
        as_index (bool, optional): True se le colonne nelle groupby
            devono essere rappresentate come indice. Defaults to False.
        dropna (bool, optional): True se si vogliono eliminare le entry completamente a nan. Defaults to True.
    """

    def __init__(
        self,
        group_columns: Union[str, List[str]],
        agg_function: Union[str, dict],
        main_columns: Optional[Union[List[str], str]] = None,
        as_index: bool = False,
        dropna: bool = True,
    ):
        """Costruttore

        Args:
            group_columns (Union[str, List[str]]): colonne su cui eseguire la groupby
            agg_function (Union[str, dict]): funzione di aggregazione
            main_columns (Optional[Union[List[str], str]], optional): colonne su cui applicare l'aggregazione. Defaults to None.
            as_index (bool, optional): True se le colonne nelle groupby
                devono essere rappresentate come indice. Defaults to False.
            dropna (bool, optional): True se si vogliono eliminare le entry completamente a nan. Defaults to True.
        """
        self.group_columns = group_columns
        self.agg_function = agg_function
        self.main_columns = main_columns
        self.as_index = as_index
        self.dropna = dropna

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        if self.agg_function == "list":
            if self.main_columns is not None:
                return data.groupby(self.group_columns)[
                    self.main_columns
                ].apply(list)
            else:
                return data.groupby(self.group_columns).apply(list)

        grouped_df = data.groupby(
            self.group_columns, as_index=self.as_index, dropna=self.dropna
        )

        if self.main_columns:
            columns = self.main_columns
        else:
            columns = list(
                filter(lambda x: x not in self.group_columns, data.columns)
            )

        return grouped_df[columns].agg(self.agg_function)


class ProjectionTransformer(BaseTransformer):
    """Transformer per eseguire una proiezione sul datasets

    Attributes:
        columns (list[str]): colonne oggetto della proiezione
        to_include (bool, optional): True se le colonne sono da includere. Defaults to True.
        fill_missing_with (dict or any, optional): valore costante o dizionario di coppie
            nome colonna -> valore di default. Defaults to None.
    """

    def __init__(
        self,
        columns,
        to_include=True,
        fill_missing_with=None,
    ):
        """Costruttore

        Args:
            columns (list[str]): colonne oggetto della proiezione
            to_include (bool, optional): True se le colonne sono da includere. Defaults to True.
            fill_missing_with (dict or any, optional): valore costante o dizionario di coppie
                nome colonna -> valore di default. Defaults to None.
        """
        self.columns = columns
        self.to_include = to_include
        self.fill_missing_with = fill_missing_with

    def add_missing_columns(self, data):
        for col in self.columns:
            if col in data.columns:
                continue
            if isinstance(self.fill_missing_with, dict):
                try:
                    data[col] = (
                        self.fill_missing_with[col]
                        if self.fill_missing_with[col] != "nan"
                        else None
                    )
                except KeyError:
                    pass
            else:
                data[col] = self.fill_missing_with
        return data

    def transform(self, data):
        if self.to_include:
            if self.fill_missing_with is not None:
                data = self.add_missing_columns(data)
            return data[self.columns]
        columns = list(filter(lambda x: x not in self.columns, data.columns))
        return data[columns]
