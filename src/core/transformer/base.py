"""
Definizione dell' interfaccia comune ad ogni transformer
e definizione della classe pipeline
"""

from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import List, Optional, Union

import pandas as pd
from tqdm import tqdm

import src.core.util.behaviors as bh
from src.core.util.factory import Factory


class BaseTransformer(ABC):
    """Base class per ogni oggetto transfomer"""

    def __call__(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return self.transform(data, **kwargs)

    @abstractmethod
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Metodo astratto in cui definire le trasformazioni di ogni
        classe erede
        Parameters:
            data (DataFrame) : dataframe su cui applicare le trasformazioni
        """


class Pipeline(BaseTransformer):
    """Classe per l'esecuzione di una collezione di trasformazioni
    in pipeline

    Attributes:
        transformers (list of BaseTransformer): lista di trasformazioni
        bh_manager (BehaviorManager): BehaviorManager
        mode (Mode): modalita di esecuzione. Se 1 le trasformazioni vengono eseguite in parallelo
            (ovvero il dataframe in ingresso al meotodo transform viene dato in input a tutte le trasformazioni successive).
            Se 2 l'applicazione delle trasformazioni avviene in pipeline (l'output dello step n diventa l'input dello step n+1)

    """

    class Mode(Enum):
        """Modalita di esecuzione"""

        PARALLEL = (
            auto()
        )  # 1 - il primo dataframe viene dato in ingresso a tutti i trasformer successivi
        CASCADE = auto()  # 2 - applicazione delle trasformazioni in cascata

    def __init__(
        self,
        *,
        transformers: List[dict],
        behaviors: Optional[Union[dict, List[dict]]] = None,
        mode=2,
    ):
        factory = Factory()
        # rimuovo i transformer con debug.ignore = True
        self.transformers = []
        for t in transformers:
            to_ignore = t.pop("debug.ignore", False)
            if not to_ignore:
                self.transformers.append(factory.create(t))

        self.bh_manager = (
            None
            if behaviors is None
            else bh.BehaviorManager(behaviors=behaviors)
        )
        self.mode = Pipeline.Mode(mode)

    def transform(self, data, **kwargs):
        def get_name(trans):
            if hasattr(trans, "func"):
                return f"{trans.func.__module__}.{trans.func.__name__}"
            else:
                return trans.__class__.__name__

        pbar = tqdm(
            self.transformers, desc="Transformer", leave=True, position=0
        )
        if self.mode == Pipeline.Mode.PARALLEL:
            dfs = []
            for t in pbar:
                pbar.set_description(f"Transformer: {get_name(t)}")
                dfs.append(t(data))

            # valuto la necessita di eseguire operazioni di
            # merging e d concatenazione
            if self.bh_manager is not None:
                dfs = self.bh_manager.reduce(dfs)
            return dfs

        else:
            for t in pbar:
                pbar.set_description(f"Transformer: {get_name(t)}")
                data = t(data)
            return data
