from pydantic import BaseModel

class EcommerceMessage(BaseModel):
    event_type: str
    time: str
    user_id: str
    url: str
    text: str
