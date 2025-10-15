from typing import cast, Optional, Iterable, Union
from warnings import warn

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

from ray_elasticsearch._compat import (
    Elasticsearch,
    Document,
    InnerDoc,
    Field as EsField,
    Object,
    Nested,
)

_META_FIELDS: Iterable[Field] = [
    field("_index", string()),
    field("_type", string()),
    field("_id", string()),
    field("_version", int64()),
    field("_seq_no", int64()),
    field("_primary_term", int64()),
    field("_score", float64(), nullable=True),
]
_META_SCHEMA = schema(fields=_META_FIELDS)


def complete_schema(
    base_schema: Schema,
    source_fields: Optional[Iterable[str]] = None,
    meta_fields: Optional[Iterable[str]] = None,
) -> Schema:
    source_field_names = (
        set(source_fields) if source_fields is not None else set(base_schema.names)
    )
    source_schema = schema(
        [
            base_schema.field(name)
            for name in source_field_names
            if name in base_schema.names
        ]
    )

    meta_field_names = (
        set(meta_fields) if meta_fields is not None else set(_META_SCHEMA.names)
    )
    meta_schema = schema(
        [
            _META_SCHEMA.field(name)
            for name in meta_field_names
            if name in _META_SCHEMA.names
        ]
    )

    return unify_schemas([source_schema, meta_schema])


class _PropertyDict(TypedDict):
    type: str
    properties: NotRequired["_PropertiesDict"]


_PropertiesDict: TypeAlias = dict[str, _PropertyDict]


def schema_from_document(document: Union[type[Document], type[InnerDoc]]) -> Schema:
    """
    Derive a PyArrow schema from an elasticsearch-dsl Document class.

    This works by inspecting the mapping that would be generated for the Document class.

    Known limitations:
    - In Elasticsearch, any field can also hold arrays of values. Unless explicitly defined as `multi=True` in the Elasticsearch DSL model, this function will assume a single value, not a list.
    - For date fields, especially custom date formats, the mapped schema does not guarantee successful parsing of the dates in PyArrow.
    """
    mapping = document._doc_type.mapping
    properties = mapping.properties.properties
    return schema(
        [
            field(
                name=name,
                type=_field_to_field_data_type(properties[name]),
                nullable=not properties[name]._required,
            )
            for name in mapping
        ]
    )


def schema_from_elasticsearch(
    elasticsearch: Elasticsearch,
    index: str,
) -> Schema:
    """
    Fetch the mapping for the given index from Elasticsearch and convert it to a PyArrow schema.

    Known limitations:
    - In Elasticsearch, any field can also hold arrays of values. Unless explicitly defined as `multi=True` in the Elasticsearch DSL model, this function will assume a single value, not a list.
    - For date fields, especially custom date formats, the mapped schema does not guarantee successful parsing of the dates in PyArrow.
    """
    mapping_response = elasticsearch.indices.get_mapping(index=index)
    if (
        index not in mapping_response
        or "mappings" not in mapping_response[index]
        or "properties" not in mapping_response[index]["mappings"]
    ):
        raise ValueError(f"No mapping found for index '{index}'.")
    return _properties_dict_to_schema(
        mapping=mapping_response[index]["mappings"]["properties"]
    )


def _properties_dict_to_schema(mapping: _PropertiesDict) -> Schema:
    """
    Determine a PyArrow `Schema` from an Elasticsearch mapping, given as a dictionary of properties or Elasticsearch DSL class.
    """
    return schema(
        [
            field(
                name=name,
                type=_property_to_field_data_type(prop),
            )
            for name, prop in mapping.items()
        ]
    )


def _field_to_field_data_type(
    field: EsField,
    force_single: bool = False,
) -> DataType:
    """
    Determine the PyArrow `DataType` for a given Elasticsearch DSL `Field` instance.
    """
    if isinstance(field, Nested):
        return list_(struct(schema_from_document(field._doc_class)))
    elif field._multi and not force_single:
        return list_(_field_to_field_data_type(field, force_single=True))
    elif isinstance(field, Object):
        return struct(schema_from_document(field._doc_class))
    else:
        return _property_to_field_data_type(field.to_dict())


def _property_to_field_data_type(prop: _PropertyDict) -> DataType:
    """
    Determine the PyArrow `DataType` for a given Elasticsearch property, given as a dictionary.
    """

    prop_dict = cast(dict, prop)
    if "type" not in prop_dict and "properties" in prop_dict:
        return struct(_properties_dict_to_schema(prop_dict["properties"]))

    type = prop_dict["type"]
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
        if "format" in prop_dict and all(
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
            for format in prop_dict["format"].split("||")
        ):
            # If all formats are date-only formats, we can use date32.
            # https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/mapping-date-format
            return date32()
        return date64()
    elif type == "dense_vector":
        if "dims" not in prop_dict:
            raise ValueError("Dense vector property must have dims.")
        element_type: DataType
        if "element_type" in prop_dict:
            if prop_dict["element_type"] == "float":
                element_type = float32()
            elif prop_dict["element_type"] == "byte":
                element_type = int8()
            elif prop_dict["element_type"] == "bit":
                element_type = bool_()
            else:
                raise NotImplementedError(
                    f"Dense vector element type '{prop_dict['element_type']}' is not supported."
                )
        else:
            element_type = float32()
        return fixed_shape_tensor(element_type, [prop_dict["dims"]])
    elif type == "nested":
        if "properties" not in prop_dict:
            raise ValueError("Object property must have properties.")
        return list_(struct(_properties_dict_to_schema(prop_dict["properties"])))
    elif type == "rank_features":
        return map_(string(), float64())
    else:
        raise NotImplementedError(f"Property type '{type}' is not supported yet.")
