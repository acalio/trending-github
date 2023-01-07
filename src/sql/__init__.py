from src.sql.datamanager import SQLWriter, SQLReader, SQLBatchReader
from src.sql.cmanager import SQLContextManager
from src.core.util.factory import Factory


def initialize():
    factory = Factory()
    factory.register("sql.reader", SQLReader)
    factory.register("sql.batchreader", SQLBatchReader)
    factory.register("sql.writer", SQLWriter)
    factory.register("sql.cmanager", SQLContextManager)
