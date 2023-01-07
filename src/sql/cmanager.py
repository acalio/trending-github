import logging
from typing import Optional
import sqlalchemy as sa
from src.core.context.cmanager import DBContextManager

logger = logging.getLogger(__name__)


class SQLContextManager(DBContextManager):
    def __init__(
        self,
        enter_query: str,
        exit_query: Optional[str] = None,
        finalize_query: Optional[str] = None,
        **kwargs,
    ) -> None:
        super(SQLContextManager, self).__init__(
            enter_query=enter_query,
            exit_query=exit_query,
            finalize_query=finalize_query,
        )
        self.engine = sa.create_engine(**kwargs)

    def execute_impl(self, query: str) -> None:
        with self.engine.connect() as conn:
            conn.execute(query)
