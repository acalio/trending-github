"""Definizione della metaclass per instanziare
classi singleton
"""


class SingletonType(type):
    """Metaclass per classi singleton

    Attributes:
        _instances (dict) : dizionario contenente coppie chiave valore
                            del tipo: class-> istanza singleton
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """Instanzia un oggetto di tipo cls
        e memorizza nel dizionario _instances che
        contiene le istanze di tipo di singleton

        Returns:
            _type_: _description_
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance

        return cls._instances[cls]
