from pydantic import BaseModel

class EcommerceMessage(BaseModel):
    text: str

class PublisherMessage(BaseModel):
    text: str
