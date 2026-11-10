from typing import TypeVar, Callable, Any, Mapping, Sequence

from pandas import DataFrame
from pyarrow import Table, array
from ray.data.block import DataBatch

from ray_elasticsearch._compat import Document


def _batch_to_table(batch: DataBatch) -> Table:
    # Use Arrow as common ground, as it keeps nested lists and objects, and converts missing values (e.g., NaN) to null.
    if isinstance(batch, Table):
        return batch
    elif isinstance(batch, DataFrame):
        return Table.from_pandas(batch, preserve_index=False)
    else:
        return Table.from_pydict(
            {key: array(column, from_pandas=True) for key, column in batch.items()}
        )


def _null_as_missing(value: Any) -> Any:
    """
    Recursively remove `None` values from mappings and sequences, so that missing fields are not decoded as explicit nulls.
    """
    if isinstance(value, Mapping):
        return {
            key: _null_as_missing(item) for key, item in value.items() if item is not None
        }
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [_null_as_missing(item) for item in value]
    return value


if Document is not NotImplemented:
    _D = TypeVar("_D", bound=Document)

    def _unwrap_document(
        doc_type: type[_D],
        row: dict[str, Any],
        null_as_missing: bool,
    ) -> _D:
        # Arrow tables have a fixed schema, so fields missing from the `_source` are read as nulls.
        # Optionally treat these as missing, e.g., to use the default values of fields.
        if null_as_missing:
            row = _null_as_missing(row)
        doc = {
            **{k: v for k, v in row.items() if k.startswith("_")},
            "_source": {k: v for k, v in row.items() if not k.startswith("_")},
        }
        return doc_type.from_es(doc)

    def unwrap_document(
        doc_type: type[_D],
        null_as_missing: bool = True,
    ) -> Callable[
        [Callable[[dict[str, Any], _D], dict[str, Any]]],
        Callable[[dict[str, Any]], dict[str, Any]],
    ]:
        """
        Convenience decorator for functions passed to `Dataset.map()` to directly use the decoded Elasticsearch DSL document.

        By default, null values are treated as missing fields (e.g., so that defaults apply). Set `null_as_missing=False` to keep them as explicit nulls.
        """

        def _decorate(
            map: Callable[[dict[str, Any], _D], dict[str, Any]],
        ) -> Callable[[dict[str, Any]], dict[str, Any]]:
            def _map(row: dict[str, Any]) -> dict[str, Any]:
                return map(row, _unwrap_document(doc_type, row, null_as_missing))

            return _map

        return _decorate

    _B = TypeVar("_B", bound=DataBatch)

    def unwrap_documents(
        doc_type: type[_D],
        null_as_missing: bool = True,
    ) -> Callable[
        [Callable[[_B, Sequence[_D]], _B]],
        Callable[[_B], _B],
    ]:
        """
        Convenience decorator for functions passed to `Dataset.map_batches()` to directly use the sequence of decoded Elasticsearch DSL documents.

        By default, null values are treated as missing fields (e.g., so that defaults apply). Set `null_as_missing=False` to keep them as explicit nulls.
        """

        def _decorate(
            map: Callable[[_B, Sequence[_D]], _B],
        ) -> Callable[[_B], _B]:
            def _map(batch: _B) -> _B:
                documents: Sequence[_D] = [
                    _unwrap_document(doc_type, row, null_as_missing)
                    for row in _batch_to_table(batch).to_pylist()
                ]
                return map(batch, documents)

            return _map

        return _decorate
else:
    unwrap_document = NotImplemented  # type: ignore[assignment]
    unwrap_documents = NotImplemented  # type: ignore[assignment]
