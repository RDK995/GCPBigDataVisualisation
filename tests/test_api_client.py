import json
from urllib.error import HTTPError

from src.clients.api_client import APIClient


class DummyResponse:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def read(self):
        return json.dumps({"data": []}).encode("utf-8")


def test_api_client_get(monkeypatch):
    client = APIClient(base_url="https://api.example.com", token="abc")

    def fake_urlopen(request, timeout):
        assert timeout == 30
        assert request.full_url.startswith("https://api.example.com/v1/test")
        assert request.headers["Authorization"] == "Bearer abc"
        return DummyResponse()

    monkeypatch.setattr("src.clients.api_client.urlopen", fake_urlopen)
    payload = client.get("/v1/test", {"limit": 10})
    assert payload == {"data": []}


def test_api_client_skips_auth_header_without_token(monkeypatch):
    client = APIClient(base_url="https://api.example.com")

    def fake_urlopen(request, timeout):
        assert timeout == 30
        assert "Authorization" not in request.headers
        return DummyResponse()

    monkeypatch.setattr("src.clients.api_client.urlopen", fake_urlopen)
    client.get("/v1/test", {"limit": 10})


def test_api_client_retries_rate_limited_requests(monkeypatch):
    client = APIClient(base_url="https://api.example.com")
    calls = {"count": 0}

    def fake_urlopen(request, timeout):
        calls["count"] += 1
        if calls["count"] == 1:
            raise HTTPError(
                url=request.full_url,
                code=429,
                msg="rate limited",
                hdrs={"Retry-After": "0"},
                fp=None,
            )
        return DummyResponse()

    monkeypatch.setattr("src.clients.api_client.urlopen", fake_urlopen)
    monkeypatch.setattr("src.clients.api_client.time.sleep", lambda _: None)

    payload = client.get("/v1/test", {"limit": 10})

    assert payload == {"data": []}
    assert calls["count"] == 2
