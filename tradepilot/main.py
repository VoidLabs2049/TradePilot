from contextlib import asynccontextmanager
from importlib import import_module

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from tradepilot.api import analysis, briefing, collector, market, portfolio, signal, summary, trade_plan
from tradepilot.scheduler.engine import start_scheduler, stop_scheduler

scheduler_api = import_module("tradepilot.api.scheduler_api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    start_scheduler()
    yield
    stop_scheduler()

app = FastAPI(title="TradePilot", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(summary.router, prefix="/api/summary", tags=["summary"])
app.include_router(market.router, prefix="/api/market", tags=["market"])
app.include_router(portfolio.router, prefix="/api/portfolio", tags=["portfolio"])
app.include_router(analysis.router, prefix="/api/analysis", tags=["analysis"])
app.include_router(signal.router, prefix="/api/signal", tags=["signal"])
app.include_router(trade_plan.router, prefix="/api/trade_plan", tags=["trade_plan"])
app.include_router(collector.router, prefix="/api/collector", tags=["collector"])
app.include_router(briefing.router, prefix="/api/briefing", tags=["briefing"])
app.include_router(scheduler_api.router, prefix="/api/scheduler", tags=["scheduler"])


@app.get("/api/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("tradepilot.main:app", host="0.0.0.0", port=8000, reload=True)
