"""
Caricamento dinamico dei plugins
"""
import importlib
import logging
from types import ModuleType
from typing import List


logger = logging.getLogger(__name__)


class PluginInterface:
    """Interfaccia per ogni modulo"""

    @staticmethod
    def initialize() -> None:
        """Metodo per eseguire operazioni di inizializzazione del plugin"""


def import_module(name: str) -> ModuleType:
    """Funzione per l'import del modulo
    Parameters:
        name (str): nome del modulo
    """
    return importlib.import_module(name)


def load_plugins(plugins: List[str]) -> None:
    """Funzione per il caricamento del plugin
    Parameters:
        plugins
    """
    for pname in plugins:
        pname = f"src.{pname}"
        logger.info("importing %s", pname)
        plugin = import_module(pname)
        plugin.initialize()
