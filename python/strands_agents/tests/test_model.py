from datetime import timedelta
from uuid import uuid4

from strands import Agent

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
    assert get_activities(history) == ["invoke_model"]

    await Replayer(
        workflows=[ModelWorkflow],
        plugins=[plugin],
    ).replay_workflow(history)


@workflow.defn
class SyncCallWorkflow:
    @workflow.run
    async def run(self) -> str:
        # A plain Strands agent with its own model, called with the sync form.
        agent = Agent(model=MockModel(["sync answer"]), callback_handler=None)
        try:
            return str(agent("Hello")).strip()
        except RuntimeError as err:
            return f"refused: {err}"


async def test_sync_agent_call_is_refused(client: Client):
    """The sync form would run the model on a thread outside the sandbox, unrecorded."""
    task_queue = f"test_sync_agent_call_is_refused-{uuid4()}"
    plugin = StrandsPlugin(models={"mock": lambda: MockModel([])})

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[SyncCallWorkflow],
        plugins=[plugin],
        max_cached_workflows=0,
    ):
        handle = await client.start_workflow(
            SyncCallWorkflow.run,
            id=f"test_sync_agent_call_is_refused_{uuid4()}",
            task_queue=task_queue,
        )
        assert (await handle.result()).startswith(
            "refused: Agent.__call__() runs the call on a new thread"
        )

    await Replayer(
        workflows=[SyncCallWorkflow],
        plugins=[plugin],
    ).replay_workflow(await handle.fetch_history())
