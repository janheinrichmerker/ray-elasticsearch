from typing import TypeVar, Callable, Any, Sequence

from pandas import DataFrame
from pyarrow import Table
from ray.data.block import DataBatch

from ray_elasticsearch._compat import Document


if Document is not NotImplemented:
    _D = TypeVar("_D", bound=Document)

    def _unwrap_document(
        doc_type: type[_D],
        row: dict[str, Any],
    ) -> _D:
        doc = {
            **{k: v for k, v in row.items() if k.startswith("_")},
            "_source": {k: v for k, v in row.items() if not k.startswith("_")},
        }
        return doc_type.from_es(doc)

    def unwrap_document(
        doc_type: type[_D],
    ) -> Callable[
        [Callable[[dict[str, Any], _D], dict[str, Any]]],
        Callable[[dict[str, Any]], dict[str, Any]],
    ]:
        """
        Convenience decorator for functions passed to `Dataset.map()` to directly use the decoded Elasticsearch DSL document.
        """

        def _decorate(
            map: Callable[[dict[str, Any], _D], dict[str, Any]],
        ) -> Callable[[dict[str, Any]], dict[str, Any]]:
            def _map(row: dict[str, Any]) -> dict[str, Any]:
                return map(row, _unwrap_document(doc_type, row))

            return _map

        return _decorate

    _B = TypeVar("_B", bound=DataBatch)

    def unwrap_documents(
        doc_type: type[_D],
    ) -> Callable[
        [Callable[[_B, Sequence[_D]], _B]],
        Callable[[_B], _B],
    ]:
        """
        Convenience decorator for functions passed to `Dataset.map_batches()` to directly use the sequence of decoded Elasticsearch DSL documents.
        """

        def _decorate(
            map: Callable[[_B, Sequence[_D]], _B],
        ) -> Callable[[_B], _B]:
            def _map(batch: _B) -> _B:
                df: DataFrame
                if isinstance(batch, Table):
                    df = batch.to_pandas()
                elif isinstance(batch, DataFrame):
                    df = batch
                else:
                    df = DataFrame(batch)
                documents: Sequence[_D] = [
                    _unwrap_document(doc_type, row.to_dict())
                    for _, row in df.iterrows()
                ]
                return map(batch, documents)

            return _map

        return _decorate
else:
    unwrap_document = NotImplemented  # type: ignore[assignment]
    unwrap_documents = NotImplemented  # type: ignore[assignment]
