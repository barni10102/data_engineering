"""FastAPI application entrypoint and router composition."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import assets
from app.routers import top_movers
from app.routers import analytics


def create_app() -> FastAPI:
    # App factory keeps initialization in one place for both local run and container startup.
    fastapp = FastAPI(
        title="Assets API",
        version="1.0.0",
    )

    # Grafana/Infinity and local tools can call the API from different origins during development.
    fastapp.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Keep domain endpoints separated by router to avoid a monolithic API module.
    fastapp.include_router(assets.router)
    fastapp.include_router(top_movers.router)
    fastapp.include_router(analytics.router)

    return fastapp


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
