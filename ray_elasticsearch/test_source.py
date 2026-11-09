from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from pytest import mark

from ray_elasticsearch import ElasticsearchDatasource
from ray_elasticsearch.conftest import FakeElasticsearch, read_table

_UTC = timezone.utc


@mark.parametrize(
    ("format", "value", "expected"),
    [
        (None, "2024-01-01T10:00:00Z", datetime(2024, 1, 1, 10, tzinfo=_UTC)),
        (None, "2024-01-01T10:00:00", datetime(2024, 1, 1, 10, tzinfo=_UTC)),
        (None, "2024-01-01", datetime(2024, 1, 1, tzinfo=_UTC)),
        (
            None,
            "2024-01-01T10:00:00.123456+02:00",
            datetime(2024, 1, 1, 8, 0, 0, 123456, tzinfo=_UTC),
        ),
        (
            None,
            "2024-01-01T10:00:00.500-01:30",
            datetime(2024, 1, 1, 11, 30, 0, 500000, tzinfo=_UTC),
        ),
        (None, 1704103200000, datetime(2024, 1, 1, 10, tzinfo=_UTC)),
        (None, "1704103200123", datetime(2024, 1, 1, 10, 0, 0, 123000, tzinfo=_UTC)),
        (None, -1000, datetime(1970, 1, 1, tzinfo=_UTC) - timedelta(seconds=1)),
        (
            "strict_date_time_no_millis",
            "2024-01-01T10:00:00Z",
            datetime(2024, 1, 1, 10, tzinfo=_UTC),
        ),
        ("strict_date", "2024-01-01", datetime(2024, 1, 1, tzinfo=_UTC)),
        ("epoch_millis", 1704103200000, datetime(2024, 1, 1, 10, tzinfo=_UTC)),
        (None, None, None),
    ],
)
def test_read_date(
    fake_elasticsearch: type[FakeElasticsearch],
    format: Optional[str],
    value: Any,
    expected: Any,
) -> None:
    prop: dict[str, Any] = {"type": "date"}
    if format is not None:
        prop["format"] = format
    fake_elasticsearch.mappings = {
        "properties": {
            "date": prop,
            "object": {"properties": {"date": prop}},
            "nested": {"type": "nested", "properties": {"date": prop}},
        }
    }
    fake_elasticsearch.hits = [
        {
            "_id": "1",
            "_source": {
                "date": value,
                "object": {"date": value},
                "nested": [{"date": value}, {}],
            },
        },
        {"_id": "2", "_source": {}},
    ]
    datasource = ElasticsearchDatasource(index="index")

    actual = read_table(datasource).select(["date", "object", "nested"]).to_pylist()

    assert actual == [
        {
            "date": expected,
            "object": {"date": expected},
            "nested": [{"date": expected}, {"date": None}],
        },
        {"date": None, "object": None, "nested": None},
    ]


def test_read_unparseable_date_fails(
    fake_elasticsearch: type[FakeElasticsearch],
) -> None:
    fake_elasticsearch.mappings = {"properties": {"date": {"type": "date"}}}
    fake_elasticsearch.hits = [{"_id": "1", "_source": {"date": "yesterday"}}]
    datasource = ElasticsearchDatasource(index="index")

    try:
        read_table(datasource)
    except ValueError as e:
        assert "yesterday" in str(e)
    else:
        raise AssertionError("Expected reading an unparseable date to fail.")
