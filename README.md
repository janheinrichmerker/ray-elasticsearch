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

Use this minimal library if you plan to read or write data from/to [Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html) massively parallel for data processing in [Ray](https://docs.ray.io/en/latest/data/data.html). Internally, the library uses parallelized [sliced point-in-time search](https://www.elastic.co/guide/en/elasticsearch/reference/current/point-in-time-api.html#search-slicing) for reading and parallelized [bulk requests](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html) for writing data, the two most efficient ways to read/write data to/from Elasticsearch. Note, that this library does _not_ guarantee any specific ordering of the results, though, the scores are returned.

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

Use an Elasticsearch [query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html) to filter the results:

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

You do not need to set a fixed maximum concurrency level. But it can often be a good idea to limit concurrency (and hence, simultaneous requests to the Elasticsearch cluster) by setting the `concurrency` parameter in Ray's [`read_datasource()`](https://docs.ray.io/en/latest/data/api/doc/ray.data.read_datasource.html#ray.data.read_datasource):

```python
source = ElasticsearchDatasource(index="test")
ds = read_datasource(source, concurrency=100)\
```

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

Concurrency can again be limited by specifying the `concurrency` parameter in Ray's [`write_datasink()`](https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.write_datasink.html#ray.data.Dataset.write_datasink).

### Elasticsearch connection and authentication

Per default, the data source and sink access Elasticsearch on `localhost:9200`, the default of the [`elasticsearch` Python library](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/index.html).
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

All client related keyword arguments to the `ElasticsearchDatasource` or `ElasticsearchDatasink` are passed on to the [`Elasticsearch()`](https://elasticsearch-py.readthedocs.io/en/latest/api/elasticsearch.html#elasticsearch.Elasticsearch) constructor. Refer to the [documentation](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html) for an overview of the supported connection settings.

### Elasticsearch DSL

To simplify query construction, you can also use the [Elasticsearch DSL](https://elasticsearch-dsl.readthedocs.io/en/latest/) like this:

```python
from elasticsearch_dsl import Document
from elasticsearch_dsl.query import Exists
from ray_elasticsearch import ElasticsearchDatasource, ElasticsearchDatasink

class Foo(Document):
    class Index:
        name = "test_foo"
    text: str = Text()

source = ElasticsearchDatasource(
    index=Foo,
    query=Exists(field="doi"),
)
sink = ElasticsearchDslDatasink(index=Foo)
```

Note that, unlike in [Elasticsearch DSL](https://elasticsearch-dsl.readthedocs.io/en/latest/), the results are not parsed as Python objects but instead are returned as columns of the Ray `Dataset` (which internally uses the [Arrow format](https://arrow.apache.org/docs/python/index.html)).

### Selecting source and meta fields

Any document returned from or to-be-stored in Elasticsearch consists of the actual data nested in the `_source` field, and some metadata (e.g., `_id` and `_index`) on the top level. However, working with nested columns can sometimes be tricky with Ray (e.g., nested columns cannot be renamed easily). Because you are likely most interested in the contents of the `_source` field, i.e., the indexed fields of the Elasticsearch index, the `ray-elasticsearch` library automatically unwraps the `source` field. For example, consider the following Elasticsearch record:

```json
{
    "_index" : "test",
    "_type" : "_doc",
    "_id" : "1",
    "_score" : null,
    "_source" : {
        "value" : 1
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

The metadata field prefix can be changed with the `meta_prefix` argument (the default is an underscore, `_`, just like with Elasticsearch).

### Examples

More examples can be found in the [`examples`](examples/) directory.

### Compatibility

This library works fine with any of the following Pip packages installed:

- `elasticsearch`
- `elasticsearch7`
- `elasticsearch8`
- `elasticsearch-dsl`
- `elasticsearch7-dsl`
- `elasticsearch8-dsl`

The `ray-elasticsearch` library will automatically detect if the Elasticsearch DSL is installed, and add support for [DSL-style queries](#elasticsearch-dsl) accordingly.

## Development

To build this package and contribute to its development you need to install the `build`, `setuptools` and `wheel` packages:

```shell
pip install build setuptools wheel
```

(On most systems, these packages are already pre-installed.)

### Development installation

Install package and test dependencies:

```shell
pip install -e .[tests]
```

### Testing

Verify your changes against the test suite to verify.

```shell
ruff check .  # Code format and LINT
mypy .        # Static typing
bandit -c pyproject.toml -r .  # Security
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
