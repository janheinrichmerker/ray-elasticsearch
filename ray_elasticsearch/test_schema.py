from re import escape
from typing import Any, Iterable, cast, Iterator, Union, Optional

from pyarrow import (
    schema,
    field,
    string,
    int64,
    float64,
    timestamp,
    struct,
    list_,
    Schema,
    Field,
    DataType,
    StructType,
    ListType,
)
from pytest import mark, raises, skip

from ray_elasticsearch import ElasticsearchDatasource
from ray_elasticsearch._compat import Document, InnerDoc, Text, Object, Nested
from ray_elasticsearch.conftest import FakeElasticsearch


def _sort_fields(fields: Iterable[Field]) -> Iterator[Field]:
    for f in sorted(fields, key=lambda f: f.name):
        yield _sort_field(f)


def _sort_field(field: Field) -> Field:
    return field.with_type(_sort_data_type(field.type))


def _sort_data_type(field: DataType) -> DataType:
    if isinstance(field, StructType):
        return struct(_sort_fields(field.fields))
    if isinstance(field, ListType):
        return list_(_sort_data_type(field.value_type))
    return field


def _sort_schema(s: Schema) -> Schema:
    fields = _sort_fields(s)
    metadata = cast(dict[Union[bytes, str], Union[bytes, str]], s.metadata)
    return schema(fields, metadata)


def _assert_schema_equals(a: Optional[Schema], b: Optional[Schema]) -> None:
    if a is None or b is None:
        assert a == b
        return
    assert _sort_schema(a) == _sort_schema(b)


def test_given_schema() -> Any:
    datasource = ElasticsearchDatasource(
        schema=schema(
            [
                field("field1", string(), nullable=False),
                field("field2", string(), nullable=True),
            ]
        ),
    )

    actual = datasource.schema()
    expected = schema(
        [
            field("field1", string(), nullable=False),
            field("field2", string(), nullable=True),
        ]
    )
    _assert_schema_equals(expected, actual)


def test_dsl_schema() -> Any:
    if Document is NotImplemented:
        skip("Elasticsearch DSL is not installed.")

    class _InnerDoc(InnerDoc):
        field1 = Text(required=True)
        field2 = Text(required=False, multi=True)

    class _Document(Document):
        field1 = Text(required=True)
        field2 = Text(required=False, multi=True)
        object1 = Object(_InnerDoc, required=True)
        nested1 = Nested(_InnerDoc, required=True)

    datasource = ElasticsearchDatasource(index=_Document)

    actual = datasource.schema()
    expected_fields: Iterable[Field] = [
        field("field1", string(), nullable=False),
        field("field2", list_(string()), nullable=True),
        field(
            "object1",
            struct(
                [
                    field("field1", string(), nullable=False),
                    field("field2", list_(string()), nullable=True),
                ]
            ),
            nullable=False,
        ),
        field(
            "nested1",
            list_(
                struct(
                    [
                        field("field1", string(), nullable=False),
                        field("field2", list_(string()), nullable=True),
                    ]
                )
            ),
            nullable=False,
        ),
        field("_index", string()),
        field("_type", string()),
        field("_id", string()),
        field("_version", int64()),
        field("_seq_no", int64()),
        field("_primary_term", int64()),
        field("_score", float64(), nullable=True),
    ]
    expected = schema(expected_fields)
    _assert_schema_equals(expected, actual)


def test_dsl_schema_with_source_fields() -> Any:
    if Document is NotImplemented:
        skip("Elasticsearch DSL is not installed.")

    class _InnerDoc(InnerDoc):
        field1 = Text(required=True)
        field2 = Text(required=False, multi=True)

    class _Document(Document):
        field1 = Text(required=True)
        field2 = Text(required=False, multi=True)
        object1 = Object(_InnerDoc, required=True)
        nested1 = Nested(_InnerDoc, required=True)

    datasource = ElasticsearchDatasource(
        index=_Document,
        source_fields=["field1", "object1", "nested1"],
    )

    actual = datasource.schema()
    expected_fields: Iterable[Field] = [
        field("field1", string(), nullable=False),
        field(
            "object1",
            struct(
                [
                    field("field1", string(), nullable=False),
                    field("field2", list_(string()), nullable=True),
                ]
            ),
            nullable=False,
        ),
        field(
            "nested1",
            list_(
                struct(
                    [
                        field("field1", string(), nullable=False),
                        field("field2", list_(string()), nullable=True),
                    ]
                )
            ),
            nullable=False,
        ),
        field("_index", string()),
        field("_type", string()),
        field("_id", string()),
        field("_version", int64()),
        field("_seq_no", int64()),
        field("_primary_term", int64()),
        field("_score", float64(), nullable=True),
    ]
    expected = schema(expected_fields)
    _assert_schema_equals(expected, actual)


def test_pydantic_schema() -> Any:
    try:
        from elasticsearch_pydantic import BaseDocument
    except ImportError:
        skip("Did not find elasticsearch-pydantic package.")

    class _Document(BaseDocument):
        field1: str
        field2: Optional[list[str]]

    datasource = ElasticsearchDatasource(index=_Document)

    actual = datasource.schema()
    expected_fields: Iterable[Field] = [
        field("field1", string(), nullable=False),
        field("field2", list_(string()), nullable=True),
        field("_index", string()),
        field("_type", string()),
        field("_id", string()),
        field("_version", int64()),
        field("_seq_no", int64()),
        field("_primary_term", int64()),
        field("_score", float64(), nullable=True),
    ]
    expected = schema(expected_fields)
    _assert_schema_equals(expected, actual)


def test_pydantic_schema_with_source_fields() -> Any:
    try:
        from elasticsearch_pydantic import BaseDocument
    except ImportError:
        skip("Did not find elasticsearch-pydantic package.")

    class _Document(BaseDocument):
        field1: str
        field2: Optional[list[str]]

    datasource = ElasticsearchDatasource(
        index=_Document,
        source_fields=["field1"],
    )

    actual = datasource.schema()
    expected_fields: Iterable[Field] = [
        field("field1", string(), nullable=False),
        field("_index", string()),
        field("_type", string()),
        field("_id", string()),
        field("_version", int64()),
        field("_seq_no", int64()),
        field("_primary_term", int64()),
        field("_score", float64(), nullable=True),
    ]
    expected = schema(expected_fields)
    _assert_schema_equals(expected, actual)


@mark.parametrize(
    "prop",
    [
        {"type": "date"},
        {"type": "date", "format": "strict_date_optional_time"},
        {"type": "date", "format": "strict_date_time_no_millis"},
        {"type": "date", "format": "strict_date"},
        {"type": "date", "format": "epoch_millis"},
        {"type": "date_nanos"},
        {"type": "date_nanos", "format": "strict_date_optional_time_nanos"},
    ],
)
def test_elasticsearch_date_schema(
    fake_elasticsearch: type[FakeElasticsearch],
    prop: dict[str, Any],
) -> Any:
    fake_elasticsearch.mappings = {
        "properties": {
            "date": prop,
            "object": {"properties": {"date": prop}},
        }
    }
    datasource = ElasticsearchDatasource(index="index", meta_fields=[])

    actual = datasource.schema()
    assert actual is not None
    assert actual.field("date").type == timestamp("us", tz="UTC")
    assert actual.field("object").type.field("date").type == timestamp("us", tz="UTC")


@mark.parametrize(
    "format",
    [
        "strict_date_optional_time||epoch_millis",
        "yyyy-MM-dd",
        "yyyy/MM/dd HH:mm:ss",
        "epoch_second",
        "basic_date_time",
        "strict_week_date",
    ],
)
def test_elasticsearch_date_schema_unsupported_format(
    fake_elasticsearch: type[FakeElasticsearch],
    format: str,
) -> Any:
    fake_elasticsearch.mappings = {
        "properties": {"date": {"type": "date", "format": format}}
    }
    datasource = ElasticsearchDatasource(index="index")

    with raises(NotImplementedError, match=escape(format)):
        datasource.schema()
