import httpx

from helpers import build_config
from relay.app import create_app


async def test_liveness_endpoint_returns_alive() -> None:
    app = create_app(build_config(["http://127.0.0.1:9"]))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            response = await client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "alive"}
