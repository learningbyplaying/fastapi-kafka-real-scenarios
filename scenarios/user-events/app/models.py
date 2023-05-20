from pydantic import BaseModel

class KafkaTopic(BaseModel):
    topic: str
    num_partitions: int = 1
    replication_factor: int = 1
