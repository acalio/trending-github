"""
Definizione dei behavior per l'esecuzione
di operazioni di merging e concatenazione tra dataframe
"""
from dataclasses import dataclass
from typing import List, Union

import pandas as pd
from pandas import DataFrame

from src.core.util.factory import Factory


@dataclass
class Behavior:
    """BaseClass per la manipolazione (concatenazione, merging) di liste di dataframe"""

    # dizionario che contiene le istruzioni per istanziare e behavior concreti
    instructions: Union[dict, List[dict]]

    @dataclass
    class DataFrameFlagPair:
        """Pair DataFrame, flag false se è stato processato"""

        data: pd.DataFrame
        flg: bool

    def apply(self, dfs: List[pd.DataFrame]) -> List[pd.DataFrame]:
        """Applica le istruzioni specificate nel campo instructions sulla lista di dataframe input

        Args:
            dfs (List[pd.DataFrame]): lista di dataframe su cui applicare le behavior

        Returns:
            List[pd.DataFrame]: lista di dataframe a seguito dell'applicazione dei behavior
        """
        # aggiungo il flag ad ogni data frame in ingresso
        dfs_pairs = [Behavior.DataFrameFlagPair(x, True) for x in dfs]
        if isinstance(self.instructions, dict):
            instructions = [self.instructions]
        else:
            instructions = self.instructions
        # applico il behavior (merge/concat)
        for instructions_dict in instructions:
            self.__call__(dfs_pairs, instructions_dict)

        # prendo soltanto i dataframe con flag true, ovvero i dataframe che non
        # sono stati mergiati con altri dataframe
        dfs = list(map(lambda x: x.data, filter(lambda x: x.flg, dfs_pairs)))
        return dfs

    def __call__(
        self, dfs: List[DataFrameFlagPair], instructions_dict: dict
    ) -> None:
        raise NotImplementedError()


class MergeBehavior(Behavior):
    """Classe per la definizioni di operazioni di merging tra dataframe"""

    def __call__(
        self, dfs: List[Behavior.DataFrameFlagPair], instructions_dict: dict
    ) -> None:
        """Implementazione dell'operazione di merge. L'operazione è guidata dal dizionario instructions_dict

        Example:
            Supponiamo di ricevere in input la seguente lista: [[data=df1, flg=True], [data=df2, flg=False]]
            il dizionario con le istruzioni deve contenere le seguente informazioni:
            - left:int, indice del dataframe di sinistra
            - right: int, indice del dataframe di destra
            - kwargs: coppie chiave/valore da passare al metodo pd.merge
            Se abbiamo il dizionario {left:0, right:1, on=col1, how=left}
            verra eseguita una left join sul campo col tra il primo e il secondo dataframe.
            Il risultato viene inserito in coda alla lista di input, cambiando il flg dei dizionari
            coinvolti nella merge:
            - [[data=df1, flg=False], [data=df2, flg=False],[data=df3, flg=True]],
            dove df3 è il risultato del merge tra df1 e df2

        Args:
            dfs (List[Behavior.DataFrameFlagPair]): lista di dataframe da trasforrmare
            instructions_dict (dict): dizionario che contiene le informazioni per eseguire il merging
        """
        # prendo l'indice dei dataframe oggetto del merging dal dizionario
        left_idx = instructions_dict.pop("left")
        right_idx = instructions_dict.pop("right")
        left_df = dfs[left_idx].data
        right_df = dfs[right_idx].data
        df = pd.merge(left_df, right_df, **instructions_dict)  # type: ignore
        # setto il flag dei dataset originali a false, indica che sono stati
        # mergiati con altri dataframe
        dfs[left_idx].flg = False
        dfs[right_idx].flg = False
        # appendo il dataframe risultante dal merging alla fine della lista
        dfs.append(MergeBehavior.DataFrameFlagPair(df, True))


class ConcatBehavior(Behavior):
    """Classe per la definizione delle operazioni di concat"""

    def __call__(
        self, dfs: List[Behavior.DataFrameFlagPair], instructions_dict: dict
    ) -> None:
        """Implementazione dell'operazione di concat. L'operazione è guidata dal dizionario instructions_dict

        Example:
            Supponiamo di ricevere in input la seguente lista: [[data=df1, flg=True], [data=df2, flg=False]]
            il dizionario con le istruzioni deve contenere le seguente informazioni:
            - start:int, indice del primo dataframe da concatenare (default 0)
            - end: int, indice dell'ultimo dataframe da concatenare (defaul l'ultimo datafram della lista)
            - kwargs: coppie chiave/valore da passare al metodo pd.concat
            Se abbiamo il dizionario {axis=0} verra eseguita una concat lungo l'asse 0 di tutti
            id dataframe contenuti nella lista
            verra eseguita una left join sul campo col tra il primo e il secondo dataframe.
            Il risultato viene inserito in coda alla lista di input, cambiando il flg dei dizionari
            coinvolti nella merge:
            - [[data=df1, flg=False], [data=df2, flg=False],[data=df3, flg=True]],
            dove df3 è il risultato del merge tra df1 e df2

        Args:
            dfs (List[Behavior.DataFrameFlagPair]): lista di dataframe da trasforrmare
            instructions_dict (dict): dizionario che contiene le informazioni per eseguire il merging
        """
        start_idx = instructions_dict.pop("start", None)
        end_idx = instructions_dict.pop("end", None)
        if start_idx is None and end_idx is None:
            # vuol dire che la concat deve essere eseguita
            # sull'intera lista di dataframe
            start_idx, end_idx = 0, len(dfs)

        # recupero la lista di dataframe
        data = [x.data for x in dfs[start_idx:end_idx]]
        dfs.append(
            MergeBehavior.DataFrameFlagPair(
                pd.concat(data, **(instructions_dict)), True
            )
        )
        # setto a false il flag dei dataframe coinvolti
        for df_pair in dfs[start_idx:end_idx]:
            df_pair.flg = False


class BehaviorManager:
    """Classe che gestisce l'applicazione dei
    behavior (merge, concat) sui dataframe
    """

    def __init__(self, *, behaviors: Union[dict, List[dict]]) -> None:
        # istruzioni per le operazioni di merging
        factory = Factory()
        self.behaviors: List[Behavior] = []
        if isinstance(behaviors, dict):
            behaviors = [behaviors]
        for bh_dict in behaviors:
            self.behaviors.append(factory.create(bh_dict))

    def reduce(self, dfs: List[DataFrame]) -> List[DataFrame]:
        """Riceve in input una lista di dataframe e applica
        delle operazioni di merging o concatenazione sulla base
        delle specifiche definite all'interno di merge_behavior
        e concat_behavior

        Args:
            dfs: lista di dataframe su cui
                applicare le trasformazioni

        Returns:
            il dataframe ottenuto in seguito all'applicazione
            del merge e concat behavior
        """
        for beh in self.behaviors:
            dfs = beh.apply(dfs)
        return dfs
