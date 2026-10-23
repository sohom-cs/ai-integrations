from datetime import timedelta
from typing import Any
from uuid import uuid4

from temporalio import workflow
from temporalio.client import Client
from temporalio.strands_agents import StrandsPlugin, TemporalAgent
from temporalio.worker import Replayer, Worker
from tests.common import get_activities
from tests.mock_model import MockModel


@workflow.defn
class ModelWorkflow:
    def __init__(self) -> None:
        self.agent = TemporalAgent(
            model="mock",
            start_to_close_timeout=timedelta(seconds=15),
        )

    @workflow.run
    async def run(self, prompt: str) -> str:
        result = await self.agent.invoke_async(prompt)
        return str(result)


async def test_model(client: Client):
    task_queue = f"test_model-{uuid4()}"
    plugin = StrandsPlugin(models={"mock": lambda: MockModel(["Done!"])})

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ModelWorkflow],
        plugins=[plugin],
        max_cached_workflows=0,
    ):
        handle = await client.start_workflow(
            ModelWorkflow.run,
            "Hello",
            id=f"test_model_{uuid4()}",
            task_queue=task_queue,
        )
        assert await handle.result() == "Done!\n"

    history = await handle.fetch_history()
    assert get_activities(history) == ["strands_get_model_config", "invoke_model"]

    await Replayer(
        workflows=[ModelWorkflow],
        plugins=[plugin],
    ).replay_workflow(history)


class ConfiguredMockModel(MockModel):
    def get_config(self) -> dict[str, Any]:
        return {"model_id": "mock-1", "context_window_limit": 1000, "api_key": "k"}


@workflow.defn
class ModelConfigWorkflow:
    @workflow.run
    async def run(self, prompt: str) -> dict[str, Any]:
        agent = TemporalAgent(
            model="configured", start_to_close_timeout=timedelta(seconds=15)
        )
        await agent.invoke_async(prompt)
        return {
            "context_window_limit": agent.model.context_window_limit,
            "config": agent.model.get_config(),
        }


async def test_model_config_reaches_workflow(client: Client):
    """Workflow code sees the worker model's id and context window limit."""
    task_queue = f"test_model_config_reaches_workflow-{uuid4()}"
    plugin = StrandsPlugin(
        models={"configured": lambda: ConfiguredMockModel(["Done!"])}
    )

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ModelConfigWorkflow],
        plugins=[plugin],
        max_cached_workflows=0,
    ):
        handle = await client.start_workflow(
            ModelConfigWorkflow.run,
            "Hello",
            id=f"test_model_config_reaches_workflow_{uuid4()}",
            task_queue=task_queue,
        )
        assert await handle.result() == {
            "context_window_limit": 1000,
            "config": {"model_id": "mock-1", "context_window_limit": 1000},
        }

    await Replayer(
        workflows=[ModelConfigWorkflow],
        plugins=[plugin],
    ).replay_workflow(await handle.fetch_history())
