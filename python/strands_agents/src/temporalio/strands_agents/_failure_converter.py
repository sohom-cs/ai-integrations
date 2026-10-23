"""Failure converter for Strands-specific exceptions."""

import functools

from strands.interrupt import InterruptException
from strands.types.exceptions import (
    ContextWindowOverflowException,
    MaxTokensReachedException,
    SessionException,
    StructuredOutputException,
)

import temporalio.api.failure.v1
from temporalio.converter import (
    DefaultFailureConverter,
    FailureConverter,
    PayloadConverter,
)
from temporalio.exceptions import ApplicationError

# Activity-side: when a Strands ``InterruptException`` would otherwise be
# serialized by the default converter, the ``Interrupt`` payload on
# ``exc.interrupt`` is dropped (it lives on the instance, not in the
# serialized ApplicationError). We translate to a typed ApplicationError so
# the interrupt data survives the activity boundary and the workflow side
# can rebuild a real ``Interrupt``.
STRANDS_INTERRUPT_TYPE = "StrandsInterrupt"

# Strands' model/session exceptions that are deterministic failures (token
# limits, context overflow, structured-output validation, session I/O). They
# won't succeed on retry, so they cross the boundary as non-retryable typed
# ApplicationErrors. TemporalAgent.invoke_async rewraps these as
# StrandsWorkflowError on the workflow side so users can `except` cleanly.
_TERMINAL_EXCEPTIONS: tuple[type[BaseException], ...] = (
    MaxTokensReachedException,
    ContextWindowOverflowException,
    StructuredOutputException,
    SessionException,
)


def _to_strands_error(exception: BaseException) -> BaseException:
    """Return the typed ApplicationError for a Strands exception, else the exception."""
    if isinstance(exception, InterruptException):
        return ApplicationError(
            f"interrupt:{exception.interrupt.name}",
            exception.interrupt.to_dict(),
            type=STRANDS_INTERRUPT_TYPE,
            non_retryable=True,
        )
    if isinstance(exception, _TERMINAL_EXCEPTIONS):
        return ApplicationError(
            str(exception),
            type=type(exception).__name__,
            non_retryable=True,
        )
    return exception


class StrandsFailureConverter(DefaultFailureConverter):
    """Failure converter that preserves Strands exception payloads and retryability."""

    def to_failure(
        self,
        exception: BaseException,
        payload_converter: PayloadConverter,
        failure: temporalio.api.failure.v1.Failure,
    ) -> None:
        """Translate Strands exceptions to typed ApplicationErrors."""
        super().to_failure(_to_strands_error(exception), payload_converter, failure)


@functools.cache
def with_strands_failures(
    base: type[FailureConverter],
) -> type[FailureConverter]:
    """Return ``base`` with the Strands exception handling added.

    The SDK default becomes ``StrandsFailureConverter``. Any other class, such as
    ``DefaultFailureConverterWithEncodedAttributes``, is kept and wrapped, so its
    own encoding still applies.
    """
    if base is DefaultFailureConverter:
        return StrandsFailureConverter
    if issubclass(base, StrandsFailureConverter) or getattr(
        base, "_adds_strands_failures", False
    ):
        return base

    def to_failure(
        self: FailureConverter,
        exception: BaseException,
        payload_converter: PayloadConverter,
        failure: temporalio.api.failure.v1.Failure,
    ) -> None:
        base.to_failure(self, _to_strands_error(exception), payload_converter, failure)

    return type(
        f"Strands{base.__name__}",
        (base,),
        {
            "to_failure": to_failure,
            "_adds_strands_failures": True,
            "__module__": __name__,
        },
    )
