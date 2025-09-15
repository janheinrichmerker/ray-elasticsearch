from typing import Any

from pyarrow import schema, field, string
from pytest import skip

from ray_elasticsearch import ElasticsearchDatasource
from ray_elasticsearch._compat import BaseDocument


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
    assert actual == expected


def test_pydantic_schema() -> Any:
    if BaseDocument is NotImplemented:
        skip("Did not find elasticsearch-pydantic package.")

    class _Document(BaseDocument):
        field1: str
        field2: str | None

        class Index:
            name = "test_index"

    datasource = ElasticsearchDatasource(index=_Document)

    actual = datasource.schema()
    expected = schema(
        [
            field("field1", string(), nullable=False),
            field("field2", string(), nullable=True),
        ]
    )
    assert actual == expected


def test_pydantic_schema_with_source_fields() -> Any:
    if BaseDocument is NotImplemented:
        skip("Did not find elasticsearch-pydantic package.")

    class _Document(BaseDocument):
        field1: str
        field2: str | None

        class Index:
            name = "test_index"

    datasource = ElasticsearchDatasource(
        index=_Document,
        source_fields=["field1"],
    )

    actual = datasource.schema()
    expected = schema(
        [
            field("field1", string(), nullable=False),
        ]
    )
    assert actual == expected
