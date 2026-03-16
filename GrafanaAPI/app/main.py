from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import assets
from app.routers import top_movers


def create_app() -> FastAPI:
    fastapp = FastAPI(
        title="Assets API",
        version="1.0.0",
    )

    fastapp.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    fastapp.include_router(assets.router)
    fastapp.include_router(top_movers.router)

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
