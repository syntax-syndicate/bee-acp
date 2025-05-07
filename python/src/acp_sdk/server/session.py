import uuid
from types import TracebackType
from typing import Self

from acp_sdk.models import SessionId
from acp_sdk.server.resource import Resource, ResourceStorage


class Session:
    def __init__(
        self,
        storage: ResourceStorage,
        id: SessionId | None = None,
    ) -> None:
        self.id: SessionId = id or uuid.uuid4()
        self.storage = storage

    def add(self, resource: Resource) -> None:
        self.history.append(resource)

    async def __aenter__(self) -> Self:
        self.history: list[Resource] = []
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        for resource in self.history:
            await self.storage.store(resource)
