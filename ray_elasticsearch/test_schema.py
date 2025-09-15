from typing import Any, Iterable, cast, Iterator

from pyarrow import (
    schema,
    field,
    string,
    int64,
    float64,
    struct,
    list_,
    Schema,
    Field,
    DataType,
    StructType,
    ListType,
)
from pytest import skip

from ray_elasticsearch import ElasticsearchDatasource
from ray_elasticsearch._compat import Document, InnerDoc, Text, Object, Nested


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
    metadata = cast(dict[bytes | str, bytes | str], s.metadata)
    return schema(fields, metadata)


def _assert_schema_equals(a: Schema, b: Schema) -> None:
    assert _sort_schema(a) == _sort_schema(b)


def test_given_schema() -> Any:
    datasource = ElasticsearchDatasource(
        index="test_index",
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
        skip("Did not find elasticsearch-dsl package.")

    class _InnerDoc(InnerDoc):
        field1 = Text(required=True)
        field2 = Text(required=False)

    class _Document(Document):
        field1 = Text(required=True)
        field2 = Text(required=False)
        object1 = Object(_InnerDoc, required=True)
        nested1 = Nested(_InnerDoc, required=True)

        class Index:
            name = "test_index"

    datasource = ElasticsearchDatasource(index=_Document)

    actual = datasource.schema()
    expected_fields: Iterable[Field] = [
        field("field1", string(), nullable=False),
        field("field2", string(), nullable=True),
        field(
            "object1",
            struct(
                [
                    field("field1", string(), nullable=False),
                    field("field2", string(), nullable=True),
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
                        field("field2", string(), nullable=True),
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
        skip("Did not find elasticsearch-dsl package.")

    class _InnerDoc(InnerDoc):
        field1 = Text(required=True)
        field2 = Text(required=False)

    class _Document(Document):
        field1 = Text(required=True)
        field2 = Text(required=False)
        object1 = Object(_InnerDoc, required=True)
        nested1 = Nested(_InnerDoc, required=True)

        class Index:
            name = "test_index"

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
                    field("field2", string(), nullable=True),
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
                        field("field2", string(), nullable=True),
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


# def test_pydantic_schema() -> Any:
#     try:
#         from elasticsearch_pydantic import BaseDocument
#     except ImportError:
#         skip("Did not find elasticsearch-pydantic package.")

#     class _Document(BaseDocument):
#         field1: str
#         field2: str | None

#         class Index:
#             name = "test_index"

#     datasource = ElasticsearchDatasource(index=_Document)

#     actual = datasource.schema()
#     expected_fields: Iterable[Field] = [
#         field("field1", string(), nullable=False),
#         field("field2", string(), nullable=True),
#         field("_index", string()),
#         field("_type", string()),
#         field("_id", string()),
#         field("_version", int64()),
#         field("_seq_no", int64()),
#         field("_primary_term", int64()),
#         field("_score", float64(), nullable=True),
#     ]
#     expected = schema(expected_fields)
#     _assert_schema_equals(expected, actual)


# def test_pydantic_schema_with_source_fields() -> Any:
#     try:
#         from elasticsearch_pydantic import BaseDocument
#     except ImportError:
#         skip("Did not find elasticsearch-pydantic package.")

#     class _Document(BaseDocument):
#         field1: str
#         field2: str | None

#         class Index:
#             name = "test_index"

#     datasource = ElasticsearchDatasource(
#         index=_Document,
#         source_fields=["field1"],
#     )

#     actual = datasource.schema()
#     expected_fields: Iterable[Field] = [
#         field("field1", string(), nullable=False),
#         field("_index", string()),
#         field("_type", string()),
#         field("_id", string()),
#         field("_version", int64()),
#         field("_seq_no", int64()),
#         field("_primary_term", int64()),
#         field("_score", float64(), nullable=True),
#     ]
#     expected = schema(expected_fields)
#     _assert_schema_equals(expected, actual)
