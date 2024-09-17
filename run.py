from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import routes
from app.routes.routes import app as routes_app
from config import development
from app.models import models
import asyncio
import uvicorn

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(routes_app, prefix="/api/v1")


@app.get("/")
async def root():
    return {"message": "Hello World"}