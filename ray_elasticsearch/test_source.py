from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from pytest import mark, raises, skip

from ray_elasticsearch import ElasticsearchDatasource
from ray_elasticsearch._compat import Document, Text
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


def _validating_document(index_name: Optional[str]) -> type[Document]:
    """
    Create a document class that fails validation when created without any arguments (like, e.g., Pydantic documents with required fields).
    """
    if Document is NotImplemented:
        skip("Elasticsearch DSL is not installed.")

    class _Document(Document):
        title = Text(required=True)

        def __init__(self, **kwargs: Any) -> None:
            if not kwargs:
                raise ValueError("Document validation failed: missing field 'title'.")
            super().__init__(**kwargs)

        class Index:
            name = index_name

    return _Document


def test_validating_document_index_name() -> None:
    datasource = ElasticsearchDatasource(index=_validating_document("documents"))

    assert datasource.get_name() == "Elasticsearch(documents)"


def test_validating_document_without_index_name() -> None:
    datasource = ElasticsearchDatasource(index=_validating_document(None))

    with raises(ValueError, match="has no index name"):
        datasource.get_name()
