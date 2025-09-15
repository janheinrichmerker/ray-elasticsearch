from typing import cast, Optional, Iterable
from warnings import warn

from pydantic_to_pyarrow import get_pyarrow_schema as _get_schema_from_pydantic
from pyarrow import (
    Schema,
    Field,
    DataType,
    schema,
    field,
    string,
    struct,
    list_,
    map_,
    float16,
    float32,
    float64,
    int8,
    int16,
    int32,
    int64,
    bool_,
    uint64,
    date32,
    date64,
    fixed_shape_tensor,
    unify_schemas,
)
from typing_extensions import TypeAlias, TypedDict, NotRequired  # type: ignore[import]

from ray_elasticsearch._compat import Elasticsearch, BaseDocument
from ray_elasticsearch.model import IndexType

_META_SCHEMA = schema(
    [
        field("_index", string()),
        field("_type", string()),
        field("_id", string()),
        field("_version", int64()),
        field("_seq_no", int64()),
        field("_primary_term", int64()),
        field("_score", float64(), nullable=True),
    ]
)


def derive_schema(
    index: IndexType,
    index_name: str,
    elasticsearch: Elasticsearch,
    source_fields: Optional[Iterable[str]] = None,
    meta_fields: Optional[Iterable[str]] = None,
) -> Schema:
    base_schema: Schema
    if (
        BaseDocument is not NotImplemented
        and isinstance(index, type)
        and issubclass(index, BaseDocument)
    ):
        base_schema = _get_schema_from_pydantic(index)  # type: ignore
    else:
        base_schema = get_schema_from_elasticsearch(elasticsearch, index_name)

    source_field_names = (
        set(source_fields) if source_fields is not None else set(base_schema.names)
    )
    source_schema = schema([base_schema.field(name) for name in source_field_names])

    meta_field_names = (
        set(meta_fields) if meta_fields is not None else set(_META_SCHEMA.names)
    )
    meta_schema = schema([_META_SCHEMA.field(name) for name in meta_field_names])

    return unify_schemas([source_schema, meta_schema])


class _ElasticsearchProperty(TypedDict):
    type: str
    properties: NotRequired["_ElasticsearchMapping"]


_ElasticsearchMapping: TypeAlias = dict[str, _ElasticsearchProperty]


def get_schema_from_elasticsearch(elasticsearch: Elasticsearch, index: str) -> Schema:
    """
    Fetch the mapping for the given index from Elasticsearch and convert it to a PyArrow schema.

    In Elasticsearch, any field can also hold arrays of values. Because that is rarely used and we cannot represent unions of values and/or arrays in PyArrow, this function always maps to single values.
    """
    mapping_response = elasticsearch.indices.get_mapping(index=index)
    if (
        index not in mapping_response
        or "mappings" not in mapping_response[index]
        or "properties" not in mapping_response[index]["mappings"]
    ):
        raise ValueError(f"No mapping found for index '{index}'.")
    mapping = mapping_response[index]["mappings"]["properties"]
    return elasticsearch_mapping_to_schema(mapping)


def elasticsearch_mapping_to_schema(mapping: _ElasticsearchMapping) -> Schema:
    """
    Convert an Elasticsearch mapping to a PyArrow schema.

    Known limitations:
    - In Elasticsearch, any field can also hold arrays of values. Because that is rarely used and we cannot represent unions of values and/or arrays in PyArrow, this function always maps to single values.
    - For date fields, especially custom date formats, the mapped schema does not guarantee successful parsing of the dates in PyArrow.
    """
    return schema(
        [_elasticsearch_property_to_field(name, prop) for name, prop in mapping.items()]
    )


def _elasticsearch_property_to_field(name: str, prop: _ElasticsearchProperty) -> Field:
    return field(name, _elasticsearch_property_to_field_data_type(name, prop))


def _elasticsearch_property_to_field_data_type(
    name: str, prop: _ElasticsearchProperty
) -> DataType:
    type = prop["type"]
    if type == "alias":
        # https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/field-alias
        raise NotImplementedError("Alias fields are not supported yet.")
    elif type == "boolean":
        return bool_()
    elif type == "byte":
        return int8()
    elif type == "short":
        return int16()
    elif type == "integer":
        return int32()
    elif type == "long":
        return int64()
    elif type == "unsigned_long":
        return uint64()
    elif type == "half_float":
        return float16()
    elif type == "float":
        return float32()
    elif type == "double":
        return float64()
    elif type == "scaled_float":
        warn(
            "Elasticsearch scaled_float is mapped to float64, precision may be lost.",
            UserWarning,
            stacklevel=2,
        )
        return float64()
    elif type == "rank_feature":
        return float64()
    elif type == "text":
        return string()
    elif type == "keyword":
        return string()
    elif type == "binary":
        warn(
            "Elasticsearch binary fields are mapped to Base64-encoded strings.",
            UserWarning,
            stacklevel=2,
        )
        return string()
    elif type == "ip":
        return string()
    elif type == "search_as_you_type":
        return string()
    elif type == "version":
        return string()
    elif type == "date":
        if "format" in prop and all(
            format
            in (
                "date",
                "basic_date",
                "basic_ordinal_date",
                "basic_week_date",
                "ordinal_date",
                "strict_date",
                "strict_ordinal_date",
                "strict_week_date",
                "strict_weekyear",
                "strict_weekyear_week",
                "strict_weekyear_week_day",
                "strict_year",
                "strict_year_month",
                "strict_year_month_day",
                "week_date",
                "weekyear",
                "weekyear_week",
                "weekyear_week_day",
                "year",
                "year_month",
                "year_month_day",
                "yyyy-MM-dd",
            )
            for format in cast(dict, prop)["format"].split("||")
        ):
            # If all formats are date-only formats, we can use date32.
            # https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/mapping-date-format
            return date32()
        return date64()
    elif type == "dense_vector":
        if "dims" not in prop:
            raise ValueError("Dense vector property must have dims.")
        element_type: DataType
        if "element_type" in prop:
            if cast(dict, prop)["element_type"] == "float":
                element_type = float32()
            elif cast(dict, prop)["element_type"] == "byte":
                element_type = int8()
            elif cast(dict, prop)["element_type"] == "bit":
                element_type = bool_()
            else:
                raise NotImplementedError(
                    f"Dense vector element type '{cast(dict, prop)['element_type']}' is not supported."
                )
        else:
            element_type = float32()
        return fixed_shape_tensor(element_type, [cast(dict, prop)["dims"]])
    elif type == "object":
        if "properties" not in prop:
            raise ValueError("Object property must have properties.")
        return struct(elasticsearch_mapping_to_schema(prop["properties"]))
    elif type == "nested":
        if "properties" not in prop:
            raise ValueError("Object property must have properties.")
        return list_(struct(elasticsearch_mapping_to_schema(prop["properties"])))
    elif type == "rank_features":
        return map_(string(), float64())
    else:
        raise NotImplementedError(f"Property type '{type}' is not supported yet.")
