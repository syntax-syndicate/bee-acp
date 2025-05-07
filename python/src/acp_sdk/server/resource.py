import uuid

from pydantic import AnyHttpUrl

from acp_sdk.models import ResourceUrl


class Resource:
    def __init__(self, *, id: uuid.UUID | None = None, mime_type: str, content: bytes) -> None:
        self.id = id or uuid.uuid4()
        self.mime_type = mime_type
        self.content = content

    @property
    def url(self) -> AnyHttpUrl:
        return AnyHttpUrl(url=f"http://foobar/{self.id}")


class ResourceStorage:
    def __init__(self) -> None:
        pass

    async def load(self, url: ResourceUrl) -> Resource:
        pass

    async def store(self, resource: Resource) -> None:
        pass
