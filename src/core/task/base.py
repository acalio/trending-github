from multiprocessing import Pool
from typing import Optional
from pandas import DataFrame
from tqdm import tqdm

from src.core.transformer.base import BaseTransformer
from src.core.util.factory import Factory


def process(transformer: BaseTransformer, df: DataFrame) -> DataFrame:
    """Funzione da utilizzare all'interno del pool executor

    Args:
        transformer (BaseTransformer): trasformazione da applicare il
        df (DataFrame): dataframe su cui applicare la trasformazione del

    Returns:
        DataFrame: dataframe risultato dell'applicazione del trasformer
    """
    tdf = transformer(df)
    return tdf


class Task:
    """Classe che rappresenta l'esecuzione di un task di trasformazione

    Attributes:
        data_reader (DataReader): oggetto per la lettura dei dataframe
        data_writer (DataReader): oggetto per la scrittura dei dataframe
        transformer (BaseTransformer): oggetto contenente le istruzioni della trasformazione
        num_of_processes (int, optional): numero di processi da utilizzare per il multiprocessing. Default to 1
        ctx_manager (ContextManager, optional): manager di contesto. Default to None

    """

    def __init__(
        self,
        data_reader: dict,
        data_writer: dict,
        transformer: dict,
        num_of_processes: int = 1,
        ctx_manager: Optional[dict] = None,
    ) -> None:
        """Costruttore

        Args:
            task_args (dict): Dizionario contenente le istruzione per istanziare il task
        """
        factory = Factory()
        # data_reader_args = task_args["data_reader"]
        # data_writer_args = task_args["data_writer"]
        # transformer_args = task_args["transformer"]

        self.num_of_processes = (
            num_of_processes  # task_args.pop("num_of_processes", 1)
        )

        # ctx_args = task_args.pop("ctx_manager", {"type": "core.context.dummy"})
        if ctx_manager is None:
            self.ctx_manager = factory.create({"type": "core.context.dummy"})
        else:
            self.ctx_manager = factory.create(ctx_manager)

        self.data_reader = factory.create(data_reader)
        self.data_writer = factory.create(data_writer)

        self.transformer = factory.create(transformer)

    def run(self, **kwargs) -> None:
        def run_sequentially():
            with self.ctx_manager as _:
                for df in tqdm(self.data_reader.read()):
                    transformed_df = self.transformer(df)
                    self.data_writer.write(data=transformed_df)

        def run_multiprocessing():
            dataframe_pool = []
            with self.ctx_manager as _:
                for i, df in enumerate(self.data_reader.read()):
                    dataframe_pool.append(df)
                    if len(dataframe_pool) == self.num_of_processes:
                        with Pool(self.num_of_processes) as p:
                            # get slice
                            slice_args = [
                                (self.transformer, data)
                                for data in dataframe_pool
                            ]
                            transformed_dfs = p.starmap(process, slice_args)
                            print("Transformation Done")
                            _ = [
                                self.data_writer.write(data=tdf)
                                for tdf in transformed_dfs
                            ]
                        print(f"Processed: {i+1} batches so far")
                        dataframe_pool = []

                # gestisco i residuo
                if len(dataframe_pool) > 0:
                    with Pool(len(dataframe_pool)) as p:
                        # get slice
                        slice_args = [
                            (self.transformer, data) for data in dataframe_pool
                        ]
                        transformed_dfs = p.starmap(process, slice_args)
                        print(
                            f"Transformed last {len(dataframe_pool)} batches"
                        )
                        _ = [
                            self.data_writer.write(data=tdf)
                            for tdf in transformed_dfs
                        ]

        if self.num_of_processes == 1:
            print("Run Sequentially")
            run_sequentially()
        else:
            print("Run Parallel")
            run_multiprocessing()
