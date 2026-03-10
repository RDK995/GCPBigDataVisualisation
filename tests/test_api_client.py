import json

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
