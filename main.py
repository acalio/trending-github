"""
Main file
"""


import hydra
from omegaconf import DictConfig, OmegaConf, open_dict

from src.core.util import loader
from src.core.util.factory import Factory
from src.core.util.params_interpeter import ParamsInterpreter


@hydra.main(config_path="config", config_name="main", version_base="1.1")
def main(conf: DictConfig):
    with open_dict(conf) as conf_dict:
        conf_dict = OmegaConf.to_container(conf_dict)
        conf_dict = ParamsInterpreter()(conf_dict)
        plugins = conf_dict.pop("plugins", None)
        plugins.append("core")
        loader.load_plugins(plugins)
        factory = Factory()
        if "tasks" in conf:
            dag = factory.create(dict(type="core.dag", **conf_dict))
            dag.run()
        else:
            task = factory.create(dict(type="core.task", **conf_dict))
            task.run()


if __name__ == "__main__":
    main()
