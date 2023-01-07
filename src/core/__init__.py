import src.core.util.behaviors as bh
from src.core.context.cmanager import (
    TaskContextManager,
    FunctionBasedContextManager,
    MultipleContextManager,
)
from src.core.datamanager.multiple import (
    MultipleSinksWriter,
    MultipleSourceReader,
)
from src.core.datamanager.sharedmemory import (
    SharedMemoryReader,
    SharedMemoryWriter,
)
from src.core.task.base import Task
from src.core.task.dag import TaskDag
from src.core.transformer.base import Pipeline
from src.core.transformer.basic import (
    DummyTransformer,
    GroupByTransformer,
    ProjectionTransformer,
    SwissKnife,
)
from src.core.util.factory import Factory


def initialize():
    factory = Factory()
    factory.register("core.concat", bh.ConcatBehavior)
    factory.register("core.merge", bh.MergeBehavior)
    factory.register("core.task", Task)
    factory.register("core.dag", TaskDag)
    factory.register("core.context.dummy", TaskContextManager)
    factory.register("core.context.multiple", MultipleContextManager)
    factory.register("core.context.fbased", FunctionBasedContextManager)
    factory.register("core.multiplereader", MultipleSourceReader)
    factory.register("core.multiplewriter", MultipleSinksWriter)
    factory.register("core.sharedmemoryreader", SharedMemoryReader)
    factory.register("core.sharedmemorywriter", SharedMemoryWriter)
    factory.register("core.transformers.dummy", DummyTransformer)
    factory.register("core.transformers.swissknife", SwissKnife)
    factory.register("core.transformers.groupby", GroupByTransformer)
    factory.register("core.transformers.projection", ProjectionTransformer)
    factory.register("core.transformers.pipeline", Pipeline)
