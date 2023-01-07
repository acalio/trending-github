from src.core.util.factory import Factory
import src.github.gitapi as g


def initialize():
    factory = Factory()
    factory.register("github.read", g.GitRepoReader)
    factory.register("github.write", g.GitRepoWriter)
    factory.register("github.add_date", g.add_date)
