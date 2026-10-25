from fastapi.testclient import TestClient

from relay.app import create_app


def test_liveness_endpoint_returns_alive() -> None:
    with TestClient(create_app()) as client:
        response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "alive"}
