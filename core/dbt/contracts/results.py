from dbt.contracts.graph.manifest import CompileResultNode
from dbt.contracts.graph.unparsed import Time
from dbt.contracts.graph.parsed import ParsedSourceDefinition
from dbt.contracts.util import Writable
from hologram.helpers import StrEnum
from hologram import JsonSchemaMixin

import agate

from dataclasses import dataclass, field
from datetime import datetime
from typing import Union, Dict, List, Optional, Any
from numbers import Real


@dataclass
class TimingInfo(JsonSchemaMixin):
    name: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    def begin(self):
        self.started_at = datetime.utcnow()

    def end(self):
        self.completed_at = datetime.utcnow()


class collect_timing_info:
    def __init__(self, name):
        self.timing_info = TimingInfo(name=name)

    def __enter__(self):
        self.timing_info.begin()
        return self.timing_info

    def __exit__(self, exc_type, exc_value, traceback):
        self.timing_info.end()


@dataclass
class PartialResult(JsonSchemaMixin, Writable):
    node: CompileResultNode
    error: Optional[str] = None
    status: Union[None, str, int, bool] = None
    execution_time: Union[str, int] = 0
    thread_id: Optional[int] = 0
    timing: List[TimingInfo] = field(default_factory=list)
    fail: Optional[bool] = None

    # if the result got to the point where it could be skipped/failed, we would
    # be returning a real result, not a partial.
    @property
    def skipped(self):
        return False


@dataclass
class WritableRunModelResult(PartialResult):
    skip: bool = False


@dataclass
class RunModelResult(WritableRunModelResult):
    agate_table: Optional[agate.Table] = None

    def to_dict(self, *args, **kwargs):
        dct = super().to_dict(*args, **kwargs)
        dct.pop('agate_table', None)
        return dct


@dataclass
class ExecutionResult(JsonSchemaMixin, Writable):
    results: List[Union[WritableRunModelResult, PartialResult]]
    generated_at: datetime
    elapsed_time: Real


class FreshnessStatus(StrEnum):
    Pass = 'pass'
    Warn = 'warn'
    Error = 'error'


@dataclass(init=False)
class SourceFreshnessResult(PartialResult):
    max_loaded_at: datetime
    snapshotted_at: datetime
    age: Real
    status: FreshnessStatus
    node: ParsedSourceDefinition

    def __init__(
        self,
        max_loaded_at: datetime,
        snapshotted_at: datetime,
        age: float,
        status: FreshnessStatus,
        node: ParsedSourceDefinition,
        **kwargs,
    ) -> None:
        self.max_loaded_at = max_loaded_at
        self.snapshotted_at = snapshotted_at
        self.age = age
        self.status = status
        self.node = node
        super().__init__(**kwargs)

    @property
    def failed(self):
        return self.status == 'error'

    @property
    def skipped(self):
        return False


@dataclass
class FreshnessMetadata(JsonSchemaMixin):
    generated_at: datetime
    elapsed_time: Real


@dataclass
class FreshnessExecutionResult(FreshnessMetadata):
    results: List[Union[PartialResult, SourceFreshnessResult]]

    def write(self, path):
        """Create a new object with the desired output schema and write it."""
        meta = FreshnessMetadata(
            generated_at=self.generated_at,
            elapsed_time=self.elapsed_time,
        )
        sources = {}
        for result in self.results:
            unique_id = result.node.unique_id
            if result.error is not None:
                result_dict = {
                    'error': result.error,
                    'state': 'runtime error'
                }
            else:
                result_dict = {
                    'max_loaded_at': result.max_loaded_at,
                    'snapshotted_at': result.snapshotted_at,
                    'max_loaded_at_time_ago_in_s': result.age,
                    'state': result.status,
                    'criteria': result.node.freshness,
                }
            sources[unique_id] = result_dict
        output = FreshnessRunOutput(meta=meta, sources=sources)
        output.write(path)


def _copykeys(src, keys, **updates):
    return {k: getattr(src, k) for k in keys}


@dataclass
class FreshnessCriteria(JsonSchemaMixin):
    warn_after: Time
    error_after: Time


class FreshnessErrorEnum(StrEnum):
    runtime_error = 'runtime error'


@dataclass
class SourceFreshnessRuntimeError(JsonSchemaMixin):
    error: str
    state: FreshnessErrorEnum


@dataclass
class SourceFreshnessOutput(JsonSchemaMixin):
    max_loaded_at: datetime
    snapshotted_at: datetime
    max_loaded_at_time_ago_in_s: Real
    state: FreshnessStatus
    criteria: FreshnessCriteria


SourceFreshnessRunResult = Union[SourceFreshnessOutput,
                                 SourceFreshnessRuntimeError]


@dataclass
class FreshnessRunOutput(JsonSchemaMixin):
    meta: FreshnessMetadata
    sources: Dict[str, SourceFreshnessRunResult]


@dataclass
class RemoteCompileResult(JsonSchemaMixin):
    raw_sql: str
    compiled_sql: str
    timing: List[TimingInfo]

    @property
    def error(self):
        return None


@dataclass
class ResultTable(JsonSchemaMixin):
    column_names: List[str]
    rows: List[Any]


@dataclass
class RemoteRunResult(RemoteCompileResult):
    table: ResultTable
