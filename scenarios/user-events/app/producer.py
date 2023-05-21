from fastapi import FastAPI
from pydantic import BaseModel

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

## Settings
from dotenv import load_dotenv
import os
load_dotenv()
admin_config = {'bootstrap.servers': os.getenv("bootstrap.servers")}

## Application
app = FastAPI()

from admin import app as admin
app.include_router(admin.router)

## Sources
from sources.ecommerce.api_gateway import app as ecommerce
app.include_router(ecommerce.router)
