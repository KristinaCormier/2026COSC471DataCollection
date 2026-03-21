import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from utils import collector_shared as mod


pytestmark = pytest.mark.unit


TZ = ZoneInfo("America/New_York")


def _valid_row(**overrides):
    row = {
        "date": "2026-03-19 10:05:00",
        "open": "100.0",
        "high": "101.0",
        "low": "99.5",
        "close": "100.5",
        "volume": "1500",
    }
    row.update(overrides)
    return row


def test_coerce_float_accepts_numeric_and_rejects_invalid_values():
    # Given: representative numeric and non-numeric raw values.
    # When: coercing each with `_coerce_float`.
    # Then: numeric inputs convert successfully and invalid inputs return `(None, False)`.

    assert mod._coerce_float(100)[0] == 100.0
    assert mod._coerce_float(100.25)[0] == 100.25
    assert mod._coerce_float("101.5") == (101.5, True)

    assert mod._coerce_float(None) == (None, False)
    assert mod._coerce_float(True) == (None, False)
    assert mod._coerce_float("bad-number") == (None, False)


def test_validate_and_parse_row_rejects_all_fields_empty(monkeypatch):
    # Given: a row where all expected OHLCV/date fields are empty strings.
    # When: validating/parsing with `_validate_and_parse_row`.
    # Then: no parsed row is returned, hard-invalid is set, and `AllFieldsEmpty` is logged.

    logged = []
    monkeypatch.setattr(mod.lu, "log_db_error", lambda **kwargs: logged.append(kwargs))

    row = _valid_row(date="", open="", high="", low="", close="", volume="")
    ts, parsed, hard_invalid = mod._validate_and_parse_row(
        row=row,
        symbol="AAPL",
        hard_invalid_found=False,
        last_ts=None,
        now_local=dt.datetime(2026, 3, 19, 10, 30, tzinfo=TZ),
        tz=TZ,
    )

    assert ts is None
    assert parsed is None
    assert hard_invalid is True
    assert logged[0]["error_type"] == "AllFieldsEmpty"


def test_validate_and_parse_row_infers_missing_date_when_not_hard_invalid(monkeypatch):
    # Given: a row missing `date` and no prior hard-invalid condition.
    # When: parsing with an injected timestamp inference helper.
    # Then: inferred timestamp is used and numeric fields are parsed into a valid row.

    inferred_ts = dt.datetime(2026, 3, 19, 10, 10, tzinfo=TZ)
    monkeypatch.setattr(mod.tu, "infer_timestamp", lambda *args, **kwargs: (inferred_ts, "date_missing_prev_row"))

    ts, parsed, hard_invalid = mod._validate_and_parse_row(
        row=_valid_row(date=""),
        symbol="AAPL",
        hard_invalid_found=False,
        last_ts=dt.datetime(2026, 3, 19, 10, 5, tzinfo=TZ),
        now_local=dt.datetime(2026, 3, 19, 10, 30, tzinfo=TZ),
        tz=TZ,
    )

    assert ts == inferred_ts
    assert parsed is not None
    assert parsed["open"] == 100.0
    assert parsed["high"] == 101.0
    assert parsed["low"] == 99.5
    assert parsed["close"] == 100.5
    assert parsed["volume"] == 1500.0
    assert hard_invalid is False


def test_validate_and_parse_row_rejects_missing_date_after_hard_invalid(monkeypatch):
    # Given: a row missing `date` after a previous hard-invalid row has occurred.
    # When: validating/parsing again.
    # Then: row is rejected and `MissingDate` is logged without inference.

    logged = []
    monkeypatch.setattr(mod.lu, "log_db_error", lambda **kwargs: logged.append(kwargs))

    ts, parsed, hard_invalid = mod._validate_and_parse_row(
        row=_valid_row(date=""),
        symbol="AAPL",
        hard_invalid_found=True,
        last_ts=None,
        now_local=dt.datetime(2026, 3, 19, 10, 30, tzinfo=TZ),
        tz=TZ,
    )

    assert ts is None
    assert parsed is None
    assert hard_invalid is True
    assert logged[0]["error_type"] == "MissingDate"


def test_validate_and_parse_row_rejects_invalid_timestamp(monkeypatch):
    # Given: a row with an unparsable timestamp string.
    # When: running `_validate_and_parse_row`.
    # Then: parsing fails, no row is returned, and `InvalidTimestamp` is logged.

    logged = []
    monkeypatch.setattr(mod.lu, "log_db_error", lambda **kwargs: logged.append(kwargs))

    ts, parsed, hard_invalid = mod._validate_and_parse_row(
        row=_valid_row(date="not-a-timestamp"),
        symbol="AAPL",
        hard_invalid_found=False,
        last_ts=None,
        now_local=dt.datetime(2026, 3, 19, 10, 30, tzinfo=TZ),
        tz=TZ,
    )

    assert ts is None
    assert parsed is None
    assert hard_invalid is True
    assert logged[0]["error_type"] == "InvalidTimestamp"


def test_validate_and_parse_row_rejects_schema_type_mismatch(monkeypatch):
    # Given: a row with a non-numeric value in an OHLC field.
    # When: validating/parsing it.
    # Then: the row is rejected and a `SchemaTypeMismatch` error is recorded.

    logged = []
    monkeypatch.setattr(mod.lu, "log_db_error", lambda **kwargs: logged.append(kwargs))

    ts, parsed, hard_invalid = mod._validate_and_parse_row(
        row=_valid_row(open="bad"),
        symbol="AAPL",
        hard_invalid_found=False,
        last_ts=None,
        now_local=dt.datetime(2026, 3, 19, 10, 30, tzinfo=TZ),
        tz=TZ,
    )

    assert ts is None
    assert parsed is None
    assert hard_invalid is True
    assert logged[0]["error_type"] == "SchemaTypeMismatch"
    assert "open" in logged[0]["error_message"]


def test_construct_source_url_includes_expected_query_parameters():
    # Given: symbol plus start/end datetimes spanning two dates.
    # When: building URL with `_construct_source_url`.
    # Then: URL includes expected endpoint and query parameters (`from`, `to`, `extended`).

    start = dt.datetime(2026, 3, 18, 15, 0, tzinfo=TZ)
    end = dt.datetime(2026, 3, 19, 9, 35, tzinfo=TZ)

    url = mod._construct_source_url("AAPL", start, end)

    assert url.startswith("https://financialmodelingprep.com/api/v3/historical-chart/5min/AAPL")
    assert "from=2026-03-18" in url
    assert "to=2026-03-19" in url
    assert "extended=true" in url


def test_fetch_api_data_returns_payload_when_response_has_content(monkeypatch):
    # Given: an HTTP 200 response mock with non-empty body and JSON list.
    # When: fetching data with `_fetch_api_data`.
    # Then: JSON payload is returned unchanged.

    class FakeResponse:
        status_code = 200
        content = b"x"

        def raise_for_status(self):
            return None

        def json(self):
            return [{"date": "2026-03-19 10:05:00"}]

    monkeypatch.setattr(mod.requests, "get", lambda *args, **kwargs: FakeResponse())

    data = mod._fetch_api_data(
        symbol="AAPL",
        start=dt.datetime(2026, 3, 19, 10, 0, tzinfo=TZ),
        end=dt.datetime(2026, 3, 19, 10, 5, tzinfo=TZ),
        api_key="test-key",
        tz=TZ,
    )

    assert data == [{"date": "2026-03-19 10:05:00"}]


def test_fetch_api_data_returns_empty_list_for_empty_response_content(monkeypatch):
    # Given: an HTTP response mock whose body is empty.
    # When: `_fetch_api_data` is called.
    # Then: it returns an empty list rather than parsing JSON content.

    class FakeResponse:
        status_code = 200
        content = b""

        def raise_for_status(self):
            return None

        def json(self):
            return [{"ignored": True}]

    monkeypatch.setattr(mod.requests, "get", lambda *args, **kwargs: FakeResponse())

    data = mod._fetch_api_data(
        symbol="AAPL",
        start=dt.datetime(2026, 3, 19, 10, 0, tzinfo=TZ),
        end=dt.datetime(2026, 3, 19, 10, 5, tzinfo=TZ),
        api_key="test-key",
        tz=TZ,
    )

    assert data == []


def test_fetch_api_data_logs_and_raises_http_error(monkeypatch):
    # Given: a response mock that raises `HTTPError` on `raise_for_status`.
    # When: invoking `_fetch_api_data`.
    # Then: the error is logged with status code details and re-raised.

    logged = []

    class FakeResponse:
        status_code = 404
        content = b"x"

        def raise_for_status(self):
            raise mod.requests.exceptions.HTTPError("404 Not Found")

        def json(self):
            return []

    monkeypatch.setattr(mod.lu, "log_api_error", lambda **kwargs: logged.append(kwargs))
    monkeypatch.setattr(mod.requests, "get", lambda *args, **kwargs: FakeResponse())

    with pytest.raises(mod.requests.exceptions.HTTPError):
        mod._fetch_api_data(
            symbol="AAPL",
            start=dt.datetime(2026, 3, 19, 10, 0, tzinfo=TZ),
            end=dt.datetime(2026, 3, 19, 10, 5, tzinfo=TZ),
            api_key="test-key",
            tz=TZ,
        )

    assert logged[0]["error_type"] == "HTTPError"
    assert logged[0]["status_code"] == 404


def test_fetch_api_data_logs_and_raises_timeout(monkeypatch):
    # Given: requests.get patched to raise a timeout exception.
    # When: calling `_fetch_api_data`.
    # Then: timeout is logged and propagated to the caller.

    logged = []
    monkeypatch.setattr(mod.lu, "log_api_error", lambda **kwargs: logged.append(kwargs))

    def raise_timeout(*args, **kwargs):
        raise mod.requests.exceptions.Timeout("request timed out")

    monkeypatch.setattr(mod.requests, "get", raise_timeout)

    with pytest.raises(mod.requests.exceptions.Timeout):
        mod._fetch_api_data(
            symbol="AAPL",
            start=dt.datetime(2026, 3, 19, 10, 0, tzinfo=TZ),
            end=dt.datetime(2026, 3, 19, 10, 5, tzinfo=TZ),
            api_key="test-key",
            tz=TZ,
        )

    assert logged[0]["error_type"] == "Timeout"


def test_fetch_api_data_logs_and_raises_generic_request_error(monkeypatch):
    # Given: requests.get patched to raise a generic `RequestException`.
    # When: invoking `_fetch_api_data`.
    # Then: request failure is logged and re-raised.

    logged = []
    monkeypatch.setattr(mod.lu, "log_api_error", lambda **kwargs: logged.append(kwargs))

    def raise_request_error(*args, **kwargs):
        raise mod.requests.exceptions.RequestException("network down")

    monkeypatch.setattr(mod.requests, "get", raise_request_error)

    with pytest.raises(mod.requests.exceptions.RequestException):
        mod._fetch_api_data(
            symbol="AAPL",
            start=dt.datetime(2026, 3, 19, 10, 0, tzinfo=TZ),
            end=dt.datetime(2026, 3, 19, 10, 5, tzinfo=TZ),
            api_key="test-key",
            tz=TZ,
        )

    assert logged[0]["error_type"] == "RequestException"