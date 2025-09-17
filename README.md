<!-- markdownlint-disable MD041 -->

[![PyPi](https://img.shields.io/pypi/v/ray-elasticsearch?style=flat-square)](https://pypi.org/project/ray-elasticsearch/)
[![CI](https://img.shields.io/github/actions/workflow/status/janheinrichmerker/ray-elasticsearch/ci.yml?branch=main&style=flat-square)](https://github.com/janheinrichmerker/ray-elasticsearch/actions/workflows/ci.yml)
[![Code coverage](https://img.shields.io/codecov/c/github/janheinrichmerker/ray-elasticsearch?style=flat-square)](https://codecov.io/github/janheinrichmerker/ray-elasticsearch/)
[![Python](https://img.shields.io/pypi/pyversions/ray-elasticsearch?style=flat-square)](https://pypi.org/project/ray-elasticsearch/)
[![Issues](https://img.shields.io/github/issues/janheinrichmerker/ray-elasticsearch?style=flat-square)](https://github.com/janheinrichmerker/ray-elasticsearch/issues)
[![Commit activity](https://img.shields.io/github/commit-activity/m/janheinrichmerker/ray-elasticsearch?style=flat-square)](https://github.com/janheinrichmerker/ray-elasticsearch/commits)
[![Downloads](https://img.shields.io/pypi/dm/ray-elasticsearch?style=flat-square)](https://pypi.org/project/ray-elasticsearch/)
[![License](https://img.shields.io/github/license/janheinrichmerker/ray-elasticsearch?style=flat-square)](LICENSE)

# ☀️ ray-elasticsearch

Ray data source and sink for Elasticsearch.

Use this minimal library if you plan to read or write data from/to [Elasticsearch](https://elastic.co/guide/en/elasticsearch/reference/current/index.html) massively parallel for data processing in [Ray](https://docs.ray.io/en/latest/data/data.html). Internally, the library uses parallelized [sliced point-in-time search](https://elastic.co/guide/en/elasticsearch/reference/current/point-in-time-api.html#search-slicing) for reading and parallelized [bulk requests](https://elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html) for writing data, the two most efficient ways to read/write data to/from Elasticsearch. Note, that this library does _not_ guarantee any specific ordering of the results, though, the scores are returned.

## Installation

Install the package from PyPI:

```shell
pip install ray-elasticsearch
```

## Usage

This library makes use of Ray's [`Datasource`](https://docs.ray.io/en/latest/data/api/doc/ray.data.Datasource.html#ray.data.Datasource) and [`Datasink`](https://docs.ray.io/en/latest/data/api/doc/ray.data.Datasink.html#ray.data.Datasink) APIs.
For [reading](#read-documents), use [`ElasticsearchDatasource`](#read-documents) and, for [writing](#write-documents), use [`ElasticsearchDatasink`](#write-documents).

### Read documents

You can read results from a specified index by using an `ElasticsearchDatasource` with Ray's [`read_datasource()`](https://docs.ray.io/en/latest/data/api/doc/ray.data.read_datasource.html#ray.data.read_datasource). Considering you have an index named `test` that stores some numeric value in the `value` field, you can efficiently compute the sum of all values like so:

```python
from ray import init
from ray.data import read_datasource
from ray_elasticsearch import ElasticsearchDatasource

init()
source = ElasticsearchDatasource(index="test")
res = read_datasource(source)\
    .sum("value")
print(f"Read complete. Sum: {res}")
```

Use an Elasticsearch [query](https://elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html) to filter the results:

```python
source = ElasticsearchDatasource(
    index="test",
    query={
        "match": {
            "text": "foo bar",
        },
    },
)
```

Note that the parallel read does not enforce any ordering of the results even though the results are scored by Elasticsearch.
With the default settings, you can still access the retrieved score from the Ray `Dataset`'s `_score` column.

You do not need to set a fixed maximum concurrency level. But it can often be a good idea to limit concurrency (and hence, simultaneous requests to the Elasticsearch cluster) by setting the `override_num_blocks` parameter in Ray's [`read_datasource()`](https://docs.ray.io/en/latest/data/api/doc/ray.data.read_datasource.html#ray.data.read_datasource):

```python
source = ElasticsearchDatasource(index="test")
ds = read_datasource(source, override_num_blocks=100)
```

The `override_num_blocks` parameter will determine the number of slices for the sliced point-in-time request. In typical scenarios, this number should not be much larger than 1000. Even with hundreds or thousands of slices, you can still limit how many requests are sent to the Elasticsearch cluster in parallel with Ray's `concurrency` parameter:

```python
source = ElasticsearchDatasource(index="test")
ds = read_datasource(source, override_num_blocks=1000, concurrency=100)
```

Normally, it suffices to just set `override_num_blocks` reasonably small, e.g., to `100` or to the number of Elasticsearch data nodes in the cluster, and to keep the `concurrency` unchanged.

### Write documents

Writing documents works similarly by using the `ElasticsearchDatasink` with Ray's [`write_datasink()`](https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.write_datasink.html#ray.data.Dataset.write_datasink):

```python
from ray import init
from ray.data import range
from ray_elasticsearch import ElasticsearchDatasink

init()
sink = ElasticsearchDatasink(index="test")
range(10_000) \
    .rename_columns({"id": "value"}) \
    .write_datasink(sink)
print("Write complete.")
```

Write concurrency can be limited by specifying the `concurrency` parameter in Ray's [`write_datasink()`](https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.write_datasink.html#ray.data.Dataset.write_datasink).
It is advisable to keep the concurrency at or below the number of data nodes in the Elasticsearch cluster, e.g., at 100.

### Elasticsearch connection and authentication

Per default, the data source and sink access Elasticsearch on `localhost:9200`, the default of the [`elasticsearch` Python library](https://elastic.co/guide/en/elasticsearch/client/python-api/current/index.html).
However, in most cases, you would instead want to continue to some remote Elasticsearch instance.
To do so, specify the client like in the example below, and use the same parameters as in the [`Elasticsearch()`](https://elasticsearch-py.readthedocs.io/en/latest/api/elasticsearch.html#elasticsearch.Elasticsearch) constructor:

```python
source = ElasticsearchDatasource(
    index="test",
    hosts="<HOST>",
    http_auth=("<USERNAME>", "<PASSWORD>"),
    max_retries=10,
)
```

All client related keyword arguments to the `ElasticsearchDatasource` or `ElasticsearchDatasink` are passed on as is to the [`Elasticsearch()`](https://elasticsearch-py.readthedocs.io/en/latest/api/elasticsearch.html#elasticsearch.Elasticsearch) constructor. Refer to the [documentation](https://elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html) for an overview of the supported connection settings.

### Data schema auto-guessing

The `ElasticsearchDatasource` will internally get the [mapping](https://elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-mapping) for the given index from Elasticsearch, and guess the [PyArrow data schema](https://arrow.apache.org/docs/python/generated/pyarrow.schema.html) based on Elasticsearch's [mapping field types](https://elastic.co/docs/reference/elasticsearch/mapping-reference/field-data-types).

### Elasticsearch DSL

This library integrates well with the [Elasticsearch DSL](https://elasticsearch-dsl.readthedocs.io/en/latest/) library, to simplify [building queries](#query-dsl), to derive a more accurate [data schema](#document-mapping-and-index) from a `Document` class, or to simplify [transforming data](#simplified-data-transformations) from Elasticsearch data sources in Ray.

#### Query DSL

To simplify query construction, just use any of the [query classes](https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html#queries) from the Elasticsearch DSL library:

```python
from elasticsearch_dsl.query import Exists
from ray_elasticsearch import ElasticsearchDatasource

source = ElasticsearchDatasource(
    index="foo",
    query=Exists(field="doi"),
)
```

All usual [operators to combine queries](https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html#query-combination) are supported.

#### Document mapping and index

This library can also improve the schema auto-guessing capabilities, by using an Elasticsearch DSL [`Document`](https://elasticsearch-dsl.readthedocs.io/en/latest/persistence.html#document) class:

```python
from elasticsearch_dsl import Document
from ray_elasticsearch import ElasticsearchDatasource

class Foo(Document):
    text = Text(required=True)
    class Index:
        name = "test_foo"

source = ElasticsearchDatasource(index=Foo)
```

Most importantly, this will make the schema reflect the [`required` and `multi` properties](https://elasticsearch-dsl.readthedocs.io/en/latest/api.html#mappings) of the `Document`'s fields to set PyArrow's [`nullable` argument](https://arrow.apache.org/docs/python/generated/pyarrow.field.html) and/or wrap schema field types as [lists](https://arrow.apache.org/docs/python/generated/pyarrow.list_.html).

Note that, the rows returned by an `ElasticsearchDatasource`, even if using a `Document` class as `index` or `schema`, will still be dictionaries. Due to the way Ray stores the data internally (in [PyArrow format](https://arrow.apache.org/docs/python/index.html)), we cannot directly return instances of the given `Document` class. Use the provided [function decorators](#simplified-data-transformations) to still easily transform the data.

#### Simplified data transformations

Two function decorators are provided that help you with transforming the data from an `ElasticsearchDatasource`:

```python
@unwrap_document(Foo)
def add_custom_field(row: dict[str, Any], document: Foo) -> dict[str, Any]:
    return {**row, "custom": document.text}

ds = ds.map(add_custom_field)
```

Or to map batches of data:

```python
@unwrap_documents(Foo)
def add_custom_field_batch(batch: DataFrame, documents: Sequence[Foo]) -> DataFrame:
    batch["custom"] = [document.text for document in documents]
    return batch

ds = ds.map_batches(add_custom_field_batch)
```

### Elasticsearch Pydantic

Instead of the standard Elasticsearch DSL `Document` class, you can also use the `BaseDocument` class from the [`elasticsearch-pydantic`](https://pypi.org/project/elasticsearch-pydantic/) library, to add Pydantic validation and type-checking to your Elasticsearch models. As that library is fully compatible with Elasticsearch DSL, its model classes can be used as a drop-in replacement and still support the more accurate [data schema guessing](#document-mapping-and-index) (from a `BaseDocument` class), or [simplified data transformations](#simplified-data-transformations).
These features are included in our test suite to regularly check compatibility of both libraries.

### Selecting source and meta fields

In Elasticsearch, any document returned from a search request keeps the actual data nested in the `_source` field, and has some metadata (e.g., `_id` and `_index`) on the top level. However, working with nested columns is tricky with Ray (e.g., nested columns cannot be renamed). The `ray-elasticsearch` library automatically unwraps the `source` field. For example, consider the following Elasticsearch record:

```json
{
  "_index": "test",
  "_type": "_doc",
  "_id": "1",
  "_score": null,
  "_source": {
    "value": 1
  }
}
```

Using the default settings, the corresponding row in the Ray dataset will look like this:

```python
{
    "_index" : "test",
    "_type" : "_doc",
    "_id" : "1",
    "_score" : None,
    "value" : 1
}
```

You can also select the source and metadata fields explicitly, using the `source_fields` and `meta_fields` arguments:

```python
source = ElasticsearchDatasource(
    index="test",
    source_fields=["value"],
    meta_fields=["id"],
)
```

With the above setting, just the ID and value will be stored in the Ray `Dataset`'s blocks:

```python
{
    "_id" : "1",
    "value" : 1
}
```

### Examples

More examples can be found in the [`examples`](examples/) directory.

### Compatibility

This library works fine with any of the following Pip packages installed:

- [`elasticsearch`](https://pypi.org/project/elasticsearch/)
- [`elasticsearch7`](https://pypi.org/project/elasticsearch7/)
- [`elasticsearch8`](https://pypi.org/project/elasticsearch8/)
- [`elasticsearch-dsl<8.12.0`](https://pypi.org/project/elasticsearch-dsl/)
- [`elasticsearch7-dsl`](https://pypi.org/project/elasticsearch7-dsl/)
- [`elasticsearch8-dsl<8.12.0`](https://pypi.org/project/elasticsearch8-dsl/)
- [`elasticsearch-pydantic`](https://pypi.org/project/elasticsearch-pydantic/)

The `ray-elasticsearch` library will automatically detect if the Elasticsearch DSL or Pydantic helpers are installed, and add support for additional features accordingly.

## Development

To build this package and contribute to its development you need to install the `build`, `setuptools` and `wheel` packages:

```shell
pip install build setuptools wheel
```

(On most systems, these packages are already pre-installed.)

### Development installation

Install package and test dependencies:

```shell
pip install -e .[tests,tests-es7]                # For elasticsearch~=7.0
pip install -e .[tests,tests-es7-major]          # For elasticsearch7
pip install -e .[tests,tests-es8]                # For elasticsearch~=8.0
pip install -e .[tests,tests-es8-major]          # For elasticsearch8
pip install -e .[tests,tests-es7-dsl]            # For elasticsearch-dsl~=7.0
pip install -e .[tests,tests-es7-dsl-major]      # For elasticsearch7-dsl
pip install -e .[tests,tests-es8-dsl]            # For elasticsearch-dsl~=8.0
pip install -e .[tests,tests-es8-dsl-major]      # For elasticsearch8-dsl
pip install -e .[tests,tests-es7-pydantic]       # For elasticsearch-pydantic and elasticsearch-dsl~=7.0
pip install -e .[tests,tests-es7-pydantic-major] # For elasticsearch-pydantic and elasticsearch7-dsl
pip install -e .[tests,tests-es8-pydantic]       # For elasticsearch-pydantic and elasticsearch-dsl~=8.0
pip install -e .[tests,tests-es8-pydantic-major] # For elasticsearch-pydantic and elasticsearch8-dsl
```

### Testing

Verify your changes against the test suite to verify.

```shell
ruff check .  # Code format and LINT
mypy .        # Static typing
pytest .      # Unit tests
```

Please also add tests for your newly developed code.

### Build wheels

Wheels for this package can be built with:

```shell
python -m build
```

## Support

If you have any problems using this package, please file an [issue](https://github.com/janheinrichmerker/ray-elasticsearch/issues/new).
We're happy to help!

## License

This repository is released under the [MIT license](LICENSE).
