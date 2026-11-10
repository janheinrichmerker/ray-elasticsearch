from typing import Any, Sequence

from pandas import DataFrame
from pyarrow import Table
from pytest import mark, skip
from ray.data.block import BlockAccessor, DataBatch

from ray_elasticsearch import unwrap_document, unwrap_documents
from ray_elasticsearch._compat import Document, InnerDoc, Integer, Nested, Text


def test_unwrap_document() -> None:
    if Document is NotImplemented:
        skip("Elasticsearch DSL is not installed.")

    class _Document(Document):
        field = Text(required=True)

    def _expected(row: dict[str, Any]) -> dict[str, Any]:
        return {**row, "custom": row["field"]}

    @unwrap_document(_Document)
    def _actual(row: dict[str, Any], document: _Document) -> dict[str, Any]:
        return {**row, "custom": document.field}

    data = _Document(field="example").to_dict(include_meta=True)
    data = {
        **{k: v for k, v in data.items() if k != "_source"},
        **data["_source"],
    }

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
        batch["custom"] = [row["field"] for _, row in batch.iterrows()]
        return batch

    @unwrap_documents(_Document)
    def _actual(batch: DataFrame, documents: Sequence[_Document]) -> DataFrame:
        batch = batch.copy()
        batch["custom"] = [document.field for document in documents]
        return batch

    data = [_Document(field=f"example{i}").to_dict(include_meta=True) for i in range(5)]
    data = [
        {**{k: v for k, v in doc.items() if k != "_source"}, **doc["_source"]}
        for doc in data
    ]
    df = DataFrame(data)

    expected = _expected(df)
    actual = _actual(df)

    assert expected.equals(actual)


@mark.parametrize("batch_format", ["pyarrow", "pandas", "numpy"])
def test_unwrap_documents_batch_formats(batch_format: str) -> None:
    if Document is NotImplemented:
        skip("Elasticsearch DSL is not installed.")

    class _Item(InnerDoc):
        name = Text(required=True)

    class _Document(Document):
        title = Text(required=True)
        items = Nested(_Item)
        views = Integer()

    # Lists of objects become NumPy arrays, and missing numbers become NaN in pandas and NumPy batches.
    table = Table.from_pylist(
        [
            {
                "_id": "1",
                "title": "First",
                "items": [{"name": "x"}, {"name": "y"}],
                "views": 3,
            },
            {"_id": "2", "title": "Second", "items": [], "views": None},
        ]
    )
    batch = BlockAccessor.for_block(table).to_batch_format(batch_format)

    actual: list[tuple[str, dict[str, Any]]] = []

    @unwrap_documents(_Document)
    def _map(batch: DataBatch, documents: Sequence[_Document]) -> DataBatch:
        actual.extend((document.meta.id, document.to_dict()) for document in documents)
        return batch

    assert _map(batch) is batch
    assert actual == [
        ("1", {"title": "First", "items": [{"name": "x"}, {"name": "y"}], "views": 3}),
        ("2", {"title": "Second"}),
    ]


def test_unwrap_document_null_as_missing() -> None:
    if Document is NotImplemented:
        skip("Elasticsearch DSL is not installed.")

    sources: list[dict[str, Any]] = []

    class _Item(InnerDoc):
        name = Text(required=True)
        note = Text()

    class _Document(Document):
        title = Text(required=True)
        items = Nested(_Item)
        views = Integer()

        @classmethod
        def from_es(cls, hit: Any) -> Any:
            # Record the decoded source, as nulls would, e.g., prevent Pydantic defaults.
            sources.append(hit["_source"])
            return super().from_es(hit)

    # Fields missing from the `_source` are read as nulls due to the fixed table schema.
    table = Table.from_pylist(
        [
            {
                "_id": "1",
                "title": "First",
                "items": [{"name": "x", "note": "y"}],
                "views": 3,
            },
            {
                "_id": "2",
                "title": "Second",
                "items": [{"name": "z", "note": None}],
                "views": None,
            },
        ]
    )

    @unwrap_document(_Document, null_as_missing=True)
    def _map(row: dict[str, Any], document: _Document) -> dict[str, Any]:
        return row

    for row in table.to_pylist():
        _map(row)

    assert sources == [
        {"title": "First", "items": [{"name": "x", "note": "y"}], "views": 3},
        {"title": "Second", "items": [{"name": "z"}]},
    ]


@mark.parametrize("batch_format", ["pyarrow", "pandas", "numpy"])
def test_unwrap_documents_null_as_missing(batch_format: str) -> None:
    if Document is NotImplemented:
        skip("Elasticsearch DSL is not installed.")

    sources: list[dict[str, Any]] = []

    class _Item(InnerDoc):
        name = Text(required=True)
        note = Text()

    class _Document(Document):
        title = Text(required=True)
        items = Nested(_Item)
        views = Integer()

        @classmethod
        def from_es(cls, hit: Any) -> Any:
            # Record the decoded source, as nulls would, e.g., prevent Pydantic defaults.
            sources.append(hit["_source"])
            return super().from_es(hit)

    # Fields missing from the `_source` are read as nulls due to the fixed table schema.
    table = Table.from_pylist(
        [
            {
                "_id": "1",
                "title": "First",
                "items": [{"name": "x", "note": "y"}],
                "views": 3,
            },
            {
                "_id": "2",
                "title": "Second",
                "items": [{"name": "z", "note": None}],
                "views": None,
            },
        ]
    )

    @unwrap_documents(_Document, null_as_missing=True)
    def _map(batch: DataBatch, documents: Sequence[_Document]) -> DataBatch:
        return batch

    _map(BlockAccessor.for_block(table).to_batch_format(batch_format))

    assert sources == [
        {"title": "First", "items": [{"name": "x", "note": "y"}], "views": 3},
        {"title": "Second", "items": [{"name": "z"}]},
    ]


def test_unwrap_document_null_as_none() -> None:
    if Document is NotImplemented:
        skip("Elasticsearch DSL is not installed.")

    sources: list[dict[str, Any]] = []

    class _Item(InnerDoc):
        name = Text(required=True)
        note = Text()

    class _Document(Document):
        title = Text(required=True)
        items = Nested(_Item)
        views = Integer()

        @classmethod
        def from_es(cls, hit: Any) -> Any:
            # Record the decoded source, as nulls would, e.g., prevent Pydantic defaults.
            sources.append(hit["_source"])
            return super().from_es(hit)

    # Fields missing from the `_source` are read as nulls due to the fixed table schema.
    table = Table.from_pylist(
        [
            {
                "_id": "1",
                "title": "First",
                "items": [{"name": "x", "note": "y"}],
                "views": 3,
            },
            {
                "_id": "2",
                "title": "Second",
                "items": [{"name": "z", "note": None}],
                "views": None,
            },
        ]
    )

    @unwrap_document(_Document, null_as_missing=False)
    def _map(row: dict[str, Any], document: _Document) -> dict[str, Any]:
        return row

    for row in table.to_pylist():
        _map(row)

    assert sources == [
        {"title": "First", "items": [{"name": "x", "note": "y"}], "views": 3},
        {"title": "Second", "items": [{"name": "z", "note": None}], "views": None},
    ]


@mark.parametrize("batch_format", ["pyarrow", "pandas", "numpy"])
def test_unwrap_documents_null_as_none(batch_format: str) -> None:
    if Document is NotImplemented:
        skip("Elasticsearch DSL is not installed.")

    sources: list[dict[str, Any]] = []

    class _Item(InnerDoc):
        name = Text(required=True)
        note = Text()

    class _Document(Document):
        title = Text(required=True)
        items = Nested(_Item)
        views = Integer()

        @classmethod
        def from_es(cls, hit: Any) -> Any:
            # Record the decoded source, as nulls would, e.g., prevent Pydantic defaults.
            sources.append(hit["_source"])
            return super().from_es(hit)

    # Fields missing from the `_source` are read as nulls due to the fixed table schema.
    table = Table.from_pylist(
        [
            {
                "_id": "1",
                "title": "First",
                "items": [{"name": "x", "note": "y"}],
                "views": 3,
            },
            {
                "_id": "2",
                "title": "Second",
                "items": [{"name": "z", "note": None}],
                "views": None,
            },
        ]
    )

    @unwrap_documents(_Document, null_as_missing=False)
    def _map(batch: DataBatch, documents: Sequence[_Document]) -> DataBatch:
        return batch

    _map(BlockAccessor.for_block(table).to_batch_format(batch_format))

    assert sources == [
        {"title": "First", "items": [{"name": "x", "note": "y"}], "views": 3},
        {"title": "Second", "items": [{"name": "z", "note": None}], "views": None},
    ]
