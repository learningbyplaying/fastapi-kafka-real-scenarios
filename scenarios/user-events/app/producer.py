from fastapi import FastAPI

## Application
app = FastAPI()

from admin import app as admin
app.include_router(admin.router)

## Sources
from sources.ecommerce.api_gateway import app as ecommerce
app.include_router(ecommerce.router)
