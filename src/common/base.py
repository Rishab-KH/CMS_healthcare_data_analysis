"""Pipeline stage base class (Template Method pattern).

Every medallion step follows the same shape: read input(s), transform, validate,
write output. Encoding that as a template keeps each concrete stage focused on
*business logic* and guarantees consistent logging and error handling across the
pipeline. Concrete stages override :meth:`run` (or the helpers) and lean on the
injected ``DeltaStore`` / ``Validation`` collaborators.

This is a deliberate upgrade over the original design, where each module
re-implemented the same try/except/logging scaffolding by hand.
"""
from __future__ import annotations

import abc

from healthcare_pipeline.common.config import Config
from healthcare_pipeline.common.logger import get_logger
from healthcare_pipeline.common.storage import DeltaStore
from healthcare_pipeline.common.validation import Validation

logger = get_logger(__name__)


class Stage(abc.ABC):
    """Base class for a medallion pipeline stage."""

    layer: str = "stage"

    def __init__(self, config: Config, store: DeltaStore):
        self.config = config
        self.store = store
        self.validation = Validation()
        self._logger = get_logger(self.__class__.__module__)

    @abc.abstractmethod
    def run(self) -> None:
        """Execute the stage. Implementations should be idempotent."""

    def execute(self) -> None:
        """Wrap :meth:`run` with uniform logging and error propagation."""
        name = self.__class__.__name__
        self._logger.info("[%s] Starting %s ...", self.layer, name)
        try:
            self.run()
        except Exception as exp:
            self._logger.error(
                "[%s] %s failed. Check the stack trace. %s",
                self.layer,
                name,
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("[%s] %s completed successfully.", self.layer, name)
