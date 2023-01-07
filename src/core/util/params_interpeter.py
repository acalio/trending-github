import re
from dataclasses import dataclass, field
from typing import List


@dataclass
class ParamsInterpreter:
    """Classe che legge il file di configurazione e sostituisce i placeholder"""

    params: List[str] = field(default_factory=lambda: [])

    def update_conf(self, run_conf: dict) -> dict:
        self.update_conf_impl(run_conf, run_conf, [])

    def update_conf_impl(self, run_conf: dict, conf: dict, keys: list) -> None:
        for k, value in conf.items():
            if isinstance(value, dict):
                self.update_conf_impl(run_conf, value, keys + [k])
            elif not isinstance(value, list):
                value = [value]

            for v in value:
                if not isinstance(v, str):
                    continue
                if "<runtime>" in v:
                    # eval della stringa e sostituisci il valore
                    # con il risultato della eval
                    commands = re.findall("(<runtime>(.*?)</runtime>)", v)
                    for match, command in commands:
                        command_result = eval(command)
                        v = re.sub(re.escape(match), command_result, v)
                    # aggiorno il valore nel dizionario originale run_conf
                    self.update_dict(run_conf, keys + [k], v)

    def update_dict(self, conf, path, value):
        current_dict = conf
        for key in path[:-1]:
            current_dict = current_dict[key]
        current_dict[path[-1]] = value

    def __call__(self, run_conf: dict) -> dict:
        run_conf_copy = run_conf.copy()
        if len(self.params) == 0:
            self.update_conf(run_conf_copy)
        else:
            # iterate over hte parameters
            for param in self.params:
                p, v = param.split("=")
                # transform the param into a path of the config file
                path = p.split(".")
                # check if the last entry is a number, thus a placeholder
                try:
                    int(path[-1])
                    placeholder_substitution = 1
                except ValueError:
                    placeholder_substitution = 0

                tmp_dict = run_conf_copy
                while len(path) - placeholder_substitution > 1:
                    k = path.pop(0)
                    try:
                        k = int(k)
                    except ValueError:
                        pass
                    tmp_dict = tmp_dict[k]

                if placeholder_substitution:
                    k, pos = path
                    tmp_dict[k] = re.sub(f"::{pos}", v, tmp_dict[k])
                else:
                    tmp_dict[path.pop()] = v

        return run_conf_copy
