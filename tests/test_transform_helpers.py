from src.extract.paginator import PageState, build_page_params


def test_build_page_params_with_updated_since():
    params = build_page_params(
        page_state=PageState(page_number=3),
        page_size=200,
        incremental_field="updated_at",
        updated_since="2026-01-01T00:00:00Z",
    )
    assert params["page"] == 3
    assert params["limit"] == 200
    assert params["updated_at_gte"] == "2026-01-01T00:00:00Z"
