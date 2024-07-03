<!-- markdownlint-disable MD041 -->
[![PyPi](https://img.shields.io/pypi/v/ray-elasticsearch?style=flat-square)](https://pypi.org/project/ray-elasticsearch/)
[![CI](https://img.shields.io/github/actions/workflow/status/heinrichreimer/ray-elasticsearch/ci.yml?branch=main&style=flat-square)](https://github.com/heinrichreimer/ray-elasticsearch/actions/workflows/ci.yml)
[![Code coverage](https://img.shields.io/codecov/c/github/heinrichreimer/ray-elasticsearch?style=flat-square)](https://codecov.io/github/heinrichreimer/ray-elasticsearch/)
[![Python](https://img.shields.io/pypi/pyversions/ray-elasticsearch?style=flat-square)](https://pypi.org/project/ray-elasticsearch/)
[![Issues](https://img.shields.io/github/issues/heinrichreimer/ray-elasticsearch?style=flat-square)](https://github.com/heinrichreimer/ray-elasticsearch/issues)
[![Commit activity](https://img.shields.io/github/commit-activity/m/heinrichreimer/ray-elasticsearch?style=flat-square)](https://github.com/heinrichreimer/ray-elasticsearch/commits)
[![Downloads](https://img.shields.io/pypi/dm/ray-elasticsearch?style=flat-square)](https://pypi.org/project/ray-elasticsearch/)
[![License](https://img.shields.io/github/license/heinrichreimer/ray-elasticsearch?style=flat-square)](LICENSE)

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

You can read results from a specified index by using an `ElasticsearchDatasource` with Ray's [`read_datasource()`](https://docs.ray.io/en/latest/data/api/doc/ray.data.read_datasource.html#ray.data.read_datasource) like so:

```python
from ray import init
from ray.data import read_datasink
from ray_elasticsearch import ElasticsearchDatasource

init()
source = ElasticsearchDslDatasource(index="test")
res = read_datasource(source)\
    .map(lambda x: x["_source"])\
    .sum("id")
print(f"Read complete. Sum: {res}")
```

Use an Elasticsearch [query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html) to filter the results:

```python
source = ElasticsearchDslDatasource(
    index="test",
    query={
        "match": {
            "text": "foo bar",
        },
    },
)
```

Note that the parallel read does not enforce any ordering of the results even though the results are scored by Elasticsearch.

Normally, it is not necessary to specify a fixed concurrency level.
The data source will automatically determine the optimal concurrency based on the disk size of the Elasticsearch index and the Ray cluster capabilities.
You can, however, override the concurrency by setting the `concurrency` parameter in Ray's [`read_datasource()`](https://docs.ray.io/en/latest/data/api/doc/ray.data.read_datasource.html#ray.data.read_datasource).

### Write documents

Writing documents works similarly by using the `ElasticsearchDatasink` with Ray's [`write_datasink()`](https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.write_datasink.html#ray.data.Dataset.write_datasink):

```python
from ray import init
from ray.data import range
from ray_elasticsearch import ElasticsearchDatasink

init()
sink = ElasticsearchDatasink(index="test")
range(10_000)\
    .map(lambda x: {"_source": x})\
    .write_datasink(sink)
print("Write complete.")
```

Concurrency can again be limited by specifying the `concurrency` parameter in Ray's [`write_datasink()`](https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.write_datasink.html#ray.data.Dataset.write_datasink).

### Elasticsearch connection

Per default, the data source and sink access Elasticsearch on `localhost:9200`.
However, in most cases, you would instead want to continue to some remote Elasticsearch instance.
To do so, specify the client like in the example below, and use the same parameters as in the [`Elasticsearch()`](https://elasticsearch-py.readthedocs.io/en/latest/api/elasticsearch.html#elasticsearch.Elasticsearch) constructor:

```python
source = ElasticsearchDatasource(
    index="test",
    client_kwargs=dict(
        hosts="<HOST>",
        http_auth=("<USERNAME>", "<PASSWORD>"),
        max_retries=10,
    ),
)
```

For the full list of allowed arguments in the `client_kwargs` dictionary, refer to the documentation of the [`Elasticsearch()`](https://elasticsearch-py.readthedocs.io/en/latest/api/elasticsearch.html#elasticsearch.Elasticsearch) constructor.

### Elasticsearch DSL

To simplify query construction, you can also use the [Elasticsearch DSL](https://elasticsearch-dsl.readthedocs.io/en/latest/) and its corresponding data source (`ElasticsearchDslDatasource`) and sink (`ElasticsearchDslDatasink`):

```python
from elasticsearch7_dsl import Document
from elasticsearch7_dsl.query import Exists
from ray_elasticsearch import ElasticsearchDslDatasource, ElasticsearchDslDatasink

class Foo(Document):
    class Index:
        name = "test_foo"
    text: str = Text()

source = ElasticsearchDslDatasource(
    index=Foo,
    query=Exists(field="doi"),
)
sink = ElasticsearchDslDatasink(index=Foo)
```

Note that, unlike in [Elasticsearch DSL](https://elasticsearch-dsl.readthedocs.io/en/latest/), the results are not parsed as Python objects but instead remain Python dictionaries, due to Ray internally transforming everything in [Arrow format](https://arrow.apache.org/docs/python/index.html).

### Examples

More examples can be found in the [`examples`](examples/) directory.

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

If you have any problems using this package, please file an [issue](https://github.com/heinrichreimer/ray-elasticsearch/issues/new).
We're happy to help!

## License

This repository is released under the [MIT license](LICENSE).
