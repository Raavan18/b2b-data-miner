from fastapi import FastAPI
from pydantic import BaseModel
import asyncio

from src.main import run_company_intelligence

app = FastAPI(title="B2B Lead Intelligence API")

class MineRequest(BaseModel):
    domain: str
    company_name: str = ""

@app.post("/mine")
async def mine(req: MineRequest):
    result = await run_company_intelligence(req.domain, req.company_name)
    return result