from pydantic import BaseModel

class Job(BaseModel):
    id: int
    media_url: str
