from typing import Any, Sequence

from pandas import DataFrame
from pytest import skip

from ray_elasticsearch import unwrap_document, unwrap_documents
from ray_elasticsearch._compat import Document, Text


def test_unwrap_document() -> None:
    if Document is NotImplemented:
        skip("Elasticsearch DSL is not installed.")

    class _Document(Document):
        field = Text(required=True)

    def _expected(row: dict[str, Any]) -> dict[str, Any]:
        return {**row, "custom": row["_source"]["field"]}

    @unwrap_document(_Document)
    def _actual(row: dict[str, Any], document: _Document) -> dict[str, Any]:
        return {**row, "custom": document.field}

    data = _Document(field="example").to_dict(include_meta=True)

    expected = _expected(data)
    actual = _actual(data)

    assert expected == actual


def test_unwrap_documents() -> None:
    if Document is NotImplemented:
        skip("Elasticsearch DSL is not installed.")

    class _Document(Document):
        field = Text(required=True)

    def _expected(batch: DataFrame) -> DataFrame:
        batch = batch.copy()
        batch["custom"] = [row["_source"]["field"] for _, row in batch.iterrows()]
        return batch

    @unwrap_documents(_Document)
    def _actual(batch: DataFrame, documents: Sequence[_Document]) -> DataFrame:
        batch = batch.copy()
        batch["custom"] = [document.field for document in documents]
        return batch

    data = DataFrame(
        [_Document(field=f"example{i}").to_dict(include_meta=True) for i in range(5)]
    )

    expected = _expected(data)
    actual = _actual(data)

    assert expected.equals(actual)
