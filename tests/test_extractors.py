from src.config import OpenMeteoLocation
from src.extract.extractor import OpenMeteoExtractor, WorldBankExtractor
from src.extract.paginator import build_world_bank_page_params


class StubAPIClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, endpoint, params):
        self.calls.append((endpoint, params))
        return self.responses.pop(0)


def test_world_bank_extractor_fetches_all_pages():
    client = StubAPIClient(
        [
            [
                {"page": 1, "pages": 2, "per_page": "2", "total": 3},
                [
                    {"countryiso3code": "GBR", "indicator": {"id": "SP.POP.TOTL"}, "date": "2022"},
                    {"countryiso3code": "USA", "indicator": {"id": "SP.POP.TOTL"}, "date": "2021"},
                ],
            ],
            [
                {"page": 2, "pages": 2, "per_page": "2", "total": 3},
                [
                    {"countryiso3code": "FRA", "indicator": {"id": "SP.POP.TOTL"}, "date": "2020"},
                ],
            ],
        ]
    )

    result = WorldBankExtractor(api_client=client, page_size=2).fetch_indicator("SP.POP.TOTL")

    assert len(result.records) == 3
    assert result.max_cursor_value == "2022"
    assert client.calls[0][0] == "/country/all/indicator/SP.POP.TOTL"
    assert client.calls[0][1] == build_world_bank_page_params(page_number=1, page_size=2)
    assert client.calls[1][1]["page"] == 2


def test_open_meteo_extractor_supports_forecast_and_archive():
    forecast_client = StubAPIClient(
        [
            {
                "hourly": {
                    "time": ["2025-01-01T00:00", "2025-01-01T01:00"],
                    "temperature_2m": [4.1, 3.8],
                }
            }
        ]
    )
    archive_client = StubAPIClient(
        [
            {
                "hourly": {
                    "time": ["2024-12-31T22:00", "2024-12-31T23:00"],
                    "temperature_2m": [5.1, 4.9],
                }
            }
        ]
    )
    extractor = OpenMeteoExtractor(
        forecast_client=forecast_client,
        archive_client=archive_client,
    )
    location = OpenMeteoLocation(location_id="london", latitude=51.5072, longitude=-0.1276)

    archive_result = extractor.fetch_archive(
        location=location,
        hourly_variables=["temperature_2m"],
        archive_days=2,
        timezone="UTC",
    )
    forecast_result = extractor.fetch_forecast(
        location=location,
        hourly_variables=["temperature_2m"],
        timezone="UTC",
        forecast_days=2,
    )

    assert archive_result.max_cursor_value == "2024-12-31T23:00"
    assert forecast_result.max_cursor_value == "2025-01-01T01:00"
    assert archive_client.calls[0][0] == "/archive"
    assert archive_client.calls[0][1]["timezone"] == "UTC"
    assert forecast_client.calls[0][0] == "/forecast"
    assert forecast_client.calls[0][1]["forecast_days"] == 2
