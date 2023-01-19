import unittest

import hydra
from omegaconf import OmegaConf, open_dict

import src.core.util.loader as loader
from src.core.util.factory import Factory


class Test_1(unittest.TestCase):
    """Doc"""

    def test_1(self):
        with hydra.initialize(
            version_base=None,
            config_path="../config/top_repo",
        ):
            # config is relative to a module
            conf = hydra.compose(config_name="main")
            with open_dict(conf) as conf_dict:
                conf_dict = OmegaConf.to_container(conf_dict)
                # conf_dict = ParamsInterpreter()(conf_dict)
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
