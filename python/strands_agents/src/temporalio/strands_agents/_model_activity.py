from collections.abc import AsyncIterable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from strands.models import Model
from strands.types.streaming import StreamEvent

from temporalio import activity
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.strands_agents._heartbeat_decorator import auto_heartbeater


# Fields are typed as Any because strands TypedDicts (Message, ToolSpec) use
# NotRequired, which Python < 3.11's get_type_hints leaks through unchanged
# and the default JSON converter then fails to deserialize. Values flow
# through unchanged to ``Model.stream`` which accepts the raw dicts.
@dataclass
class _InvokeModelInput:
    model_name: str | None
    messages: Any
    invocation_state: dict[str, Any] = field(default_factory=dict)
    tool_specs: Any = None
    system_prompt: str | None = None
    tool_choice: Any = None
    system_prompt_content: Any = None
    # Sent only for a stateful worker model: the agent's model state, such as
    # the last response id.
    model_state: dict[str, Any] | None = None


@dataclass
class _InvokeModelResult:
    """A stateful model's events and its updated model state."""

    events: list[Any]
    model_state: dict[str, Any]


@dataclass
class _StreamingInvokeModelInput(_InvokeModelInput):
    streaming_topic: str = ""
    streaming_batch_interval_seconds: float = 0.1


@dataclass
class _ModelConfigInput:
    model_name: str | None


@dataclass
class _ModelConfig:
    """The Worker model's settings that Strands reads in Workflow code.

    Only these fields cross into History: a provider config can hold
    credentials or clients.
    """

    model_id: str | None = None
    context_window_limit: int | None = None
    stateful: bool = False


class ModelActivity:
    """Holds the registered model factories and exposes the model activities."""

    def __init__(
        self,
        factories: dict[str, Callable[[], Model]],
        *,
        default_name: str | None = None,
    ) -> None:
        """Store the factories; models are constructed lazily on first use.

        ``default_name`` is set only by the plugin's own auto-registered
        ``BedrockModel`` default. User-supplied ``models`` leave it ``None``,
        which forces every ``TemporalAgent`` to specify ``model=`` explicitly.
        """
        self._factories = factories
        self._default_name = default_name
        self._models: dict[str, Model] = {}

    def _get_model(self, name: str | None) -> Model:
        if name is None:
            if self._default_name is None:
                raise ValueError(
                    f"TemporalAgent was constructed without an explicit `model`, "
                    f"but the plugin was configured with user-supplied `models=`. "
                    f"Pass model='...' to TemporalAgent. "
                    f"Known: {sorted(self._factories)}"
                )
            name = self._default_name
        if name not in self._models:
            if name not in self._factories:
                raise ValueError(
                    f"Unknown model name {name!r}. Known: {sorted(self._factories)}"
                )
            self._models[name] = self._factories[name]()
        return self._models[name]

    @activity.defn(name="strands_get_model_config")
    async def get_model_config(self, input: _ModelConfigInput) -> _ModelConfig:
        """Return the named model's id and context window limit."""
        model = self._get_model(input.model_name)
        config = model.get_config()
        model_id = (
            config.get("model_id")
            if isinstance(config, dict)
            else getattr(config, "model_id", None)
        )
        limit = model.context_window_limit
        return _ModelConfig(
            model_id=model_id if isinstance(model_id, str) else None,
            context_window_limit=limit if isinstance(limit, int) else None,
            stateful=bool(model.stateful),
        )

    @activity.defn
    @auto_heartbeater
    async def invoke_model(
        self, input: _InvokeModelInput
    ) -> list[StreamEvent] | _InvokeModelResult:
        """Run the named model and return its stream events as a list.

        When the input carries model state, also return the updated state.
        """
        model = self._get_model(input.model_name)
        return _result([event async for event in _stream(model, input)], input)

    @activity.defn
    @auto_heartbeater
    async def invoke_model_streaming(
        self, input: _StreamingInvokeModelInput
    ) -> list[StreamEvent] | _InvokeModelResult:
        """Run the named model and publish each stream event to a WorkflowStream."""
        model = self._get_model(input.model_name)
        events: list[StreamEvent] = []
        stream = WorkflowStreamClient.from_within_activity(
            batch_interval=timedelta(seconds=input.streaming_batch_interval_seconds),
        )
        topic = stream.topic(input.streaming_topic)
        async with stream:
            async for event in _stream(model, input):
                events.append(event)
                topic.publish(event)
        return _result(events, input)


def _stream(model: Model, input: _InvokeModelInput) -> AsyncIterable[StreamEvent]:
    kwargs: dict[str, Any] = {}
    if input.model_state is not None:
        kwargs["model_state"] = input.model_state
    return model.stream(
        input.messages,
        input.tool_specs,
        input.system_prompt,
        tool_choice=input.tool_choice,
        system_prompt_content=input.system_prompt_content,
        invocation_state=input.invocation_state,
        **kwargs,
    )


def _result(
    events: list[StreamEvent], input: _InvokeModelInput
) -> list[StreamEvent] | _InvokeModelResult:
    # Without model state the result stays a plain list, as before.
    if input.model_state is None:
        return events
    return _InvokeModelResult(events=list(events), model_state=input.model_state)
