import os
from shutil import copy, move
from typing import Any, Dict, Iterator, List, Optional, Union

from pandas import (
    DataFrame,
    ExcelWriter,
    read_csv,
    read_excel,
    read_parquet,
    read_pickle,
)

from src.core.datamanager.base import DataReader, DataWriter


class FileReader(DataReader):
    """Classe per la lettura dei dataframe da file
    Sono supportati diversi formati che vengono inferiti dall'estensione
    del file passato in input

    Attributes:
        input_path (Union[str, List[str]]): path al file contenente i dati
        read_args (Optional[dict], optional): Dizionario,
            o lista di dizionari contenenti le istruzioni da usare in fase di read. Defaults to None.
        bh_manager (BheaviorManager): BehaviorManager
    """

    def __init__(
        self,
        *,
        input_path: Union[str, List[str]],
        read_args: Optional[dict] = None,
        behaviors: Optional[
            Union[Dict[str, Any], List[Dict[str, Any]]]
        ] = None,
    ) -> None:
        """Constructor

        Args:
            input_path (Union[str, List[str]]): path al file contenente i dati
            read_args (Optional[dict], optional): Dizionario,
                o lista di dizionari contenenti le istruzioni da usare in fase di read. Defaults to None.
            behaviors (Optional[Union[Dict[str, Any], List[Dict[str, Any]]]], optional): Dizionario,
                o lista di dizionari contenenti le istruzioni per istanziare i vari Behavior. Defaults to None.
        """
        super(FileReader, self).__init__(behaviors=behaviors)
        self.input_path = input_path
        self.read_args = {} if read_args is None else read_args

    def read(self) -> Iterator[DataFrame]:
        reading_f = {
            "xlsx": read_excel,
            "csv": read_csv,
            "pickle": read_pickle,
            "parquet": read_parquet,
        }
        if isinstance(self.input_path, str):
            df = reading_f[self.input_path.split(".")[-1]](
                self.input_path, **(self.read_args)
            )
        else:
            read_args_values = list(self.read_args.values())
            if len(read_args_values) > 0 and not isinstance(
                read_args_values[0], dict
            ):
                # gli stessi argomenti devono essere utilizzati per
                # tutte le letture
                read_args = {
                    str(pos): self.read_args
                    for pos, _ in enumerate(self.input_path)
                }
            else:
                read_args = self.read_args

            dfs = []
            for pos, path in enumerate(self.input_path):
                path_read_args = read_args.get(str(pos), {})
                dfs.append(
                    reading_f[path.split(".")[-1]](path, **path_read_args)
                )

            # mutliple dataset - they must be merged into a single one
            if self.bh_manager:
                self.bh_manager.reduce(dfs)
            assert len(dfs) == 1, "You must provide a single dataframe"
            df = dfs.pop(0)
        yield df


class FileWriter(DataWriter):
    """Classe per la scrittura su file

    Attributes:
        output_path (Union[str, List[str]]): path al file(s) in cui srivere
        write_args (Optional[dict], optional): Dizionario,
            o lista di dizionari contenenti le istruzioni da usare in fase di scrittura. Defaults to None.
        bh_manager (BehaviorManager): BehaviorManager
    """

    def __init__(
        self,
        *,
        output_path: Union[str, List[str]],
        write_args: Optional[dict] = None,
        behaviors: Optional[
            Union[Dict[str, Any], List[Dict[str, Any]]]
        ] = None,
    ) -> None:
        """Costruttore

        Args:
            output_path (Union[str, List[str]]): path al file(s) in cui srivere
            write_args (Optional[dict], optional): Dizionario,
                o lista di dizionari contenenti le istruzioni da usare in fase di scrittura. Defaults to None.
            behaviors (Optional[Union[Dict[str, Any], List[Dict[str, Any]]]], optional): Dizionario,
                o lista di dizionari contenenti le istruzioni per istanziare i vari Behavior. Defaul
        """
        super(FileWriter, self).__init__(behaviors=behaviors)
        self.output_path = output_path
        self.write_args = write_args if write_args is not None else {}

        assert self.output_path is not None, "You must provide the output path"

    def write(self, data: Union[DataFrame, List[DataFrame]]) -> None:
        def write_f(df, p):
            writing_f = {
                "xlsx": df.to_excel,
                "csv": df.to_csv,
                "pickle": df.to_pickle,
                "parquet": df.to_parquet,
            }[p.split(".")[-1]]

            if self.write_args:
                writing_f(p, **(self.write_args))
            else:
                writing_f(p)

        if self.bh_manager:
            data = self.bh_manager.reduce(data)

        if isinstance(data, list) and isinstance(self.output_path, list):
            for d, p in zip(data, self.output_path):
                write_f(d, p)
        else:
            if isinstance(data, list):
                assert len(data) == 1
                data = data.pop(0)
            write_f(data, self.output_path)


class TemplateFileWriter(DataWriter):
    """Classe per la scrittura dei dataframe su un
    template file di excel

    Attributes:
        template_path (str): path al file di template
        output_path (str): path in cui salvare il template compilator
        if_sheet_exists (str, optional): Comportamento da adottare nel caso
            in cui il foglio in cui si scrive dovesse esistere. Defaults to "overlay".
        values_dict (Optional[str], optional): Dizionario con la seguente
            struttura {nome_foglio: {posizione: testo da scrivere}}. Defaults to None.
        write_args (Optional[dict], optional): parametri da usare in fase
            di scrittura sul template. Defaults to None.
        bh_manager (BehaviorManager): BehaviorManager
    """

    def __init__(
        self,
        *,
        template_path: str,
        output_path: str,
        if_sheet_exists: str = "overlay",
        values_dict: Optional[str] = None,
        write_args: Optional[dict] = None,
        behaviors: Optional[
            Union[Dict[str, Any], List[Dict[str, Any]]]
        ] = None,
    ):
        """Costruttore

        Args:
            template_path (str): path al file di template
            output_path (str): path in cui salvare il template compilator
            if_sheet_exists (str, optional): Comportamento da adottare nel caso
                in cui il foglio in cui si scrive dovesse esistere. Defaults to "overlay".
            values_dict (Optional[str], optional): Dizionario con la seguente
                struttura {nome_foglio: {posizione: testo da scrivere}}. Defaults to None.
            write_args (Optional[dict], optional): parametri da usare in fase
                di scrittura sul template. Defaults to None.
            behaviors (Optional[Union[Dict[str, Any], List[Dict[str, Any]]]], optional): Dizionario,
                o lista di dizionari contenenti le istruzioni per istanziare i vari Behavior. Defaul
        """
        super(TemplateFileWriter, self).__init__(behaviors=behaviors)

        self.template_path = template_path
        self.output_path = output_path
        self.if_sheet_exists = if_sheet_exists
        self.values_dict = values_dict
        self.write_args = write_args

    def write(self, data: Union[DataFrame, List[DataFrame]]) -> None:
        """Doc."""
        # copy the template to a temporary folder
        copy(self.template_path, "/tmp")
        tmp_path = os.path.join("/tmp", os.path.basename(self.template_path))
        # load the workbook
        with ExcelWriter(
            tmp_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists=self.if_sheet_exists,
        ) as writer:
            if not isinstance(list(self.write_args.values())[0], dict):
                # i dati devono essere scritti in un unico foglio
                if isinstance(data, list):
                    # se in input ricevo una lista di dataframe allora devono
                    # essere ridotti ad uno solo
                    data = self.bh_manager.reduce(data)
                # scrivo il dataframe nel foglio indicato dal template
                data.to_excel(writer, **(self.write_args))

            else:
                # i dati devono essere scritti in fogli differenti
                # self.sheetnames è un dizionario che ha come chiave
                # una posizione che si riferisce alla lista di daframe in input
                # e come valore il nome di un foglio del template
                if not isinstance(data, list):
                    # converto il dataframe in una lista di dataframe
                    data = [data]

                for pos, wargs in self.write_args.items():
                    pos_int = int(pos)
                    # vedo se esiste il pos_dict e prendo nrow a
                    data[pos_int].to_excel(writer, **wargs)

            wb = writer.book
            # inserisci ulteriori valori per popolare il template
            if self.values_dict:
                for sheet_name, sheet_values_dict in self.values_dict.items():
                    for pos, value in sheet_values_dict.items():
                        wb[sheet_name][pos] = value
        # move the workbook to the final location
        move(tmp_path, self.output_path)
