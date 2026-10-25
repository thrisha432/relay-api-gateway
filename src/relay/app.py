"""ASGI application factory for Relay."""

from fastapi import FastAPI


def create_app() -> FastAPI:
    """Build the Relay ASGI application."""
    app = FastAPI(title="Relay", docs_url=None, redoc_url=None, openapi_url=None)

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "alive"}

    return app
