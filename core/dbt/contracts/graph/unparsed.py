from dbt.node_types import UnparsedNodeType, NodeType
from dbt.contracts.util import Replaceable

from hologram import JsonSchemaMixin
from hologram.helpers import StrLiteral

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any, Union


@dataclass
class UnparsedBaseNode(JsonSchemaMixin, Replaceable):
    package_name: str
    root_path: str
    path: str
    original_file_path: str


@dataclass
class HasSQL:
    raw_sql: str


@dataclass
class UnparsedMacro(UnparsedBaseNode, HasSQL):
    pass


@dataclass
class UnparsedNode(UnparsedBaseNode, HasSQL):
    name: str
    resource_type: UnparsedNodeType


@dataclass
class UnparsedRunHook(UnparsedNode):
    index: Optional[int] = None


@dataclass
class NamedTested(JsonSchemaMixin, Replaceable):
    name: str
    description: str = ''
    tests: Optional[List[Union[Dict[str, Any], str]]] = None

    def __post_init__(self):
        if self.tests is None:
            self.tests = []


@dataclass
class ColumnDescription(JsonSchemaMixin, Replaceable):
    columns: Optional[List[NamedTested]]

    def __post_init__(self):
        if self.columns is None:
            self.columns = []


@dataclass
class NodeDescription(NamedTested):
    pass


@dataclass
class UnparsedNodeUpdate(NodeDescription, ColumnDescription):
    def __post_init__(self):
        NodeDescription.__post_init__(self)
        ColumnDescription.__post_init__(self)


class TimePeriod(Enum):
    minute = 'minute'
    hour = 'hour'
    day = 'day'


@dataclass
class Time(JsonSchemaMixin, Replaceable):
    count: int
    period: TimePeriod


@dataclass
class FreshnessThreshold(JsonSchemaMixin, Replaceable):
    warn_after: Optional[Time] = None
    error_after: Optional[Time] = None


@dataclass
class Quoting(JsonSchemaMixin, Replaceable):
    database: Optional[bool] = None
    schema: Optional[bool] = None
    identifier: Optional[bool] = None


@dataclass
class UnparsedSourceTableDefinition(NodeDescription, ColumnDescription):
    loaded_at_field: Optional[str] = None
    identifier: Optional[str] = None
    quoting: Quoting = field(default_factory=Quoting)
    freshness: FreshnessThreshold = field(default_factory=FreshnessThreshold)

    def __post_init__(self):
        NodeDescription.__post_init__(self)
        ColumnDescription.__post_init__(self)


@dataclass
class UnparsedSourceDefinition(JsonSchemaMixin, Replaceable):
    name: str
    description: str = ''
    database: Optional[str] = None
    schema: Optional[str] = None
    loader: str = ''
    quoting: Quoting = field(default_factory=Quoting)
    freshness: FreshnessThreshold = field(default_factory=FreshnessThreshold)
    loaded_at_field: Optional[str] = None
    tables: List[UnparsedSourceTableDefinition] = field(default_factory=list)


@dataclass
class UnparsedDocumentationFile(JsonSchemaMixin, Replaceable):
    package_name: str
    root_path: str
    path: str
    original_file_path: str
    file_contents: str
    # TODO: remove this.
    resource_type: StrLiteral(NodeType.Documentation)
