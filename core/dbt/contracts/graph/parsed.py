from dataclasses import dataclass, field
from typing import Optional, Union, List, Dict, Any

from hologram import JsonSchemaMixin
from hologram.helpers import StrEnum, NewPatternType

import dbt.clients.jinja
from dbt.contracts.graph.unparsed import (
    UnparsedNode, UnparsedMacro, UnparsedDocumentationFile, Quoting,
    UnparsedBaseNode, FreshnessThreshold
)
from dbt.contracts.util import Replaceable

from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from dbt.node_types import (
    NodeType, SourceType, SnapshotType, MacroType, TestType
)


class TimestampStrategy(StrEnum):
    Timestamp = 'timestamp'


class CheckStrategy(StrEnum):
    Check = 'check'


class All(StrEnum):
    All = 'all'


@dataclass
class Hook(JsonSchemaMixin, Replaceable):
    sql: str
    transaction: bool = True
    index: Optional[int] = None


def insensitive_patterns(*patterns: str):
    lowercased = []
    for pattern in patterns:
        lowercased.append(
            ''.join('[{}{}]'.format(s.upper(), s.lower()) for s in pattern)
        )
    return '^({})$'.format('|'.join(lowercased))


Severity = NewPatternType('Severity', insensitive_patterns('warn', 'error'))


@dataclass
class NodeConfig(JsonSchemaMixin, Replaceable):
    enabled: bool
    materialized: str
    persist_docs: Dict[str, Any] = field(default_factory=dict)
    post_hook: List[Hook] = field(default_factory=list)
    pre_hook: List[Hook] = field(default_factory=list)
    vars: Dict[str, Any] = field(default_factory=dict)
    quoting: Dict[str, Any] = field(default_factory=dict)
    column_types: Dict[str, Any] = field(default_factory=dict)
    tags: Union[List[str], str] = field(default_factory=list)
    _extra: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.tags, str):
            self.tags = [self.tags]

        if isinstance(self.post_hook, (str, dict)):
            self.post_hook = [self.post_hook]

        if isinstance(self.pre_hook, (str, dict)):
            self.pre_hook = [self.pre_hook]

    @property
    def extra(self):
        return self._extra

    @classmethod
    def from_dict(cls, data, validate=True):
        self = super().from_dict(data=data, validate=validate)
        keys = self.to_dict(validate=False, omit_none=False)
        for key, value in data.items():
            if key not in keys:
                self._extra[key] = value
        return self

    def to_dict(self, omit_none=True, validate=False):
        data = super().to_dict(omit_none=omit_none, validate=validate)
        data.update(self._extra)
        return data

    def replace(self, **kwargs):
        dct = self.to_dict(omit_none=False, validate=False)
        dct.update(kwargs)
        return self.from_dict(dct)

    @classmethod
    def json_schema(cls, embeddable=False):
        dct = super().json_schema(embeddable=embeddable)
        cls._schema[cls.__name__]["additionalProperties"] = True

        if embeddable:
            dct[cls.__name__]["additionalProperties"] = True
        else:
            dct["additionalProperties"] = True
        return dct

    @classmethod
    def field_mapping(cls):
        return {'post_hook': 'post-hook', 'pre_hook': 'pre-hook'}


@dataclass
class ColumnInfo(JsonSchemaMixin, Replaceable):
    name: str
    description: str = ''


# Docrefs are not quite like regular references, as they indicate what they
# apply to as well as what they are referring to (so the doc package + doc
# name, but also the column name if relevant). This is because column
# descriptions are rendered separately from their models.
@dataclass
class Docref(JsonSchemaMixin, Replaceable):
    documentation_name: str
    documentation_package: str
    column_name: Optional[str]


@dataclass
class HasFqn(JsonSchemaMixin, Replaceable):
    fqn: List[str]


@dataclass
class HasUniqueID(JsonSchemaMixin, Replaceable):
    unique_id: str


@dataclass
class DependsOn(JsonSchemaMixin, Replaceable):
    nodes: List[str] = field(default_factory=list)
    macros: List[str] = field(default_factory=list)


@dataclass
class CanRef(JsonSchemaMixin, Replaceable):
    refs: List[List[Any]]
    sources: List[List[Any]]
    depends_on: DependsOn


@dataclass
class HasRelationMetadata(JsonSchemaMixin, Replaceable):
    database: str
    schema: str


class ParsedNodeMixins:
    @property
    def is_refable(self):
        return self.resource_type in NodeType.refable()

    @property
    def is_ephemeral(self):
        return self.config.materialized == 'ephemeral'

    @property
    def is_ephemeral_model(self):
        return self.is_refable and self.is_ephemeral

    @property
    def depends_on_nodes(self):
        return self.depends_on.nodes

    def patch(self, patch):
        """Given a ParsedNodePatch, add the new information to the node."""
        # explicitly pick out the parts to update so we don't inadvertently
        # step on the model name or anything
        self.patch_path = patch.original_file_path
        self.description = patch.description
        self.columns = patch.columns
        self.docrefs = patch.docrefs
        # patches should always trigger re-validation
        self.to_dict(validate=True)

    def get_materialization(self):
        return self.config.materialized


# TODO(jeb): hooks should get their own parsed type instead of including
# index everywhere!
@dataclass
class ParsedNode(
        UnparsedNode,
        HasUniqueID,
        HasFqn,
        CanRef,
        HasRelationMetadata,
        ParsedNodeMixins):
    alias: str
    empty: bool
    tags: List[str]
    config: NodeConfig
    docrefs: List[Docref] = field(default_factory=list)
    description: str = field(default='')
    columns: Dict[str, ColumnInfo] = field(default_factory=dict)
    patch_path: Optional[str] = None
    build_path: Optional[str] = None
    index: Optional[int] = None


@dataclass
class TestConfig(NodeConfig):
    severity: Severity = 'error'


@dataclass
class ParsedTestNode(
        UnparsedNode,
        HasUniqueID,
        HasFqn,
        CanRef,
        HasRelationMetadata,
        ParsedNodeMixins):
    resource_type: TestType
    alias: str
    empty: bool
    tags: List[str]
    config: TestConfig
    docrefs: List[Docref] = field(default_factory=list)
    description: str = field(default='')
    columns: Dict[str, ColumnInfo] = field(default_factory=dict)
    patch_path: Optional[str] = None
    build_path: Optional[str] = None
    column_name: Optional[str] = None


@dataclass(init=False)
class _SnapshotConfig(NodeConfig):
    unique_key: str
    target_database: str = None
    target_schema: str = None

    def __init__(
        self,
        unique_key: str,
        target_database: str = None,
        target_schema: str = None,
        **kwargs
    ) -> None:

        self.target_database = target_database
        self.target_schema = target_schema
        self.unique_key = unique_key
        super().__init__(**kwargs)


@dataclass(init=False)
class TimestampSnapshotConfig(_SnapshotConfig):
    strategy: TimestampStrategy
    updated_at: str

    def __init__(
        self, strategy: TimestampStrategy, updated_at: str, **kwargs
    ) -> None:
        self.strategy = strategy
        self.updated_at = updated_at
        super().__init__(**kwargs)


@dataclass(init=False)
class CheckSnapshotConfig(_SnapshotConfig):
    strategy: CheckStrategy
    # TODO: is there a way to get this to accept tuples of strings? Adding
    # `Tuple[str, ...]` to the list of types results in this:
    # ['email'] is valid under each of {'type': 'array', 'items':
    # {'type': 'string'}}, {'type': 'array', 'items': {'type': 'string'}}
    # but without it, parsing gets upset about values like `('email',)`
    # maybe hologram itself should support this behavior? It's not like tuples
    # are meaningful in json
    check_cols: Union[All, List[str]]

    def __init__(
        self, strategy: CheckStrategy, check_cols: Union[All, List[str]],
        **kwargs
    ) -> None:
        self.strategy = strategy
        self.check_cols = check_cols
        super().__init__(**kwargs)


@dataclass
class IntermediateSnapshotNode(ParsedNode):
    # at an intermediate stage in parsing, where we've built something better
    # than an unparsed node for rendering in parse mode, it's pretty possible
    # that we won't have critical snapshot-related information that is only
    # defined in config blocks. To fix that, we have an intermediate type that
    # uses a regular node config, which the snapshot parser will then convert
    # into a full ParsedSnapshotNode after rendering.
    resource_type: SnapshotType
    config: NodeConfig


@dataclass
class ParsedSnapshotNode(ParsedNode):
    resource_type: SnapshotType
    config: Union[CheckSnapshotConfig, TimestampSnapshotConfig]


# The parsed node update is only the 'patch', not the test. The test became a
# regular parsed node. Note that description and columns must be present, but
# may be empty.
@dataclass
class ParsedNodePatch(JsonSchemaMixin, Replaceable):
    name: str
    description: str
    original_file_path: str
    columns: Dict[str, ColumnInfo]
    docrefs: List[Docref]


@dataclass
class _MacroDependsOn(JsonSchemaMixin, Replaceable):
    macros: List[str]


@dataclass
class ParsedMacro(UnparsedMacro):
    name: str
    resource_type: MacroType
    unique_id: str
    tags: List[str]
    depends_on: _MacroDependsOn

    @property
    def generator(self):
        """
        Returns a function that can be called to render the macro results.
        """
        return dbt.clients.jinja.macro_generator(self)


@dataclass
class ParsedDocumentation(UnparsedDocumentationFile):
    name: str
    unique_id: str
    block_contents: str


@dataclass
class ParsedSourceDefinition(
        UnparsedBaseNode,
        HasUniqueID,
        HasRelationMetadata,
        HasFqn):
    quoting: Quoting
    name: str
    source_name: str
    source_description: str
    loader: str
    identifier: str
    resource_type: SourceType
    loaded_at_field: Optional[str]
    freshness: FreshnessThreshold = field(default_factory=FreshnessThreshold)
    docrefs: List[Docref] = field(default_factory=list)
    description: str = ''
    columns: Dict[str, ColumnInfo] = field(default_factory=dict)

    @property
    def is_ephemeral_model(self):
        return False

    @property
    def depends_on_nodes(self):
        return []

    @property
    def refs(self):
        return []

    @property
    def sources(self):
        return []

    @property
    def tags(self):
        return []

    @property
    def has_freshness(self):
        return bool(self.freshness) and self.loaded_at_field is not None
