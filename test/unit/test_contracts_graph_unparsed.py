import pytest

from hologram import ValidationError

from dbt.contracts.graph.unparsed import (
    UnparsedNode, UnparsedRunHook, UnparsedMacro
)
from dbt.node_types import NodeType


def test_unparsed_node_ok():
    node_dict = {
        'name': 'foo',
        'root_path': '/root/',
        'resource_type': NodeType.Model,
        'path': '/root/x/path.sql',
        'original_file_path': '/root/path.sql',
        'package_name': 'test',
        'raw_sql': 'select * from {{ ref("thing") }}',
    }
    node = UnparsedNode(
        package_name='test',
        root_path='/root/',
        path='/root/x/path.sql',
        original_file_path='/root/path.sql',
        raw_sql='select * from {{ ref("thing") }}',
        name='foo',
        resource_type=NodeType.Model,
    )
    assert UnparsedNode.from_dict(node_dict) == node
    assert node.to_dict() == node_dict
    assert node.empty is False
    with pytest.raises(ValidationError):
        UnparsedRunHook.from_dict(node_dict)
    with pytest.raises(ValidationError):
        UnparsedMacro.from_dict(node_dict)


def test_empty_unparsed_node():
    node_dict = {
        'name': 'foo',
        'root_path': '/root/',
        'resource_type': NodeType.Model,
        'path': '/root/x/path.sql',
        'original_file_path': '/root/path.sql',
        'package_name': 'test',
        'raw_sql': '  \n',
    }
    node = UnparsedNode(
        package_name='test',
        root_path='/root/',
        path='/root/x/path.sql',
        original_file_path='/root/path.sql',
        raw_sql='  \n',
        name='foo',
        resource_type=NodeType.Model,
    )
    assert UnparsedNode.from_dict(node_dict) == node
    assert node.to_dict() == node_dict
    assert node.empty is True


def test_unparsed_node_bad_type():
    node_dict = {
        'name': 'foo',
        'root_path': '/root/',
        'resource_type': NodeType.Source,  # not valid!
        'path': '/root/x/path.sql',
        'original_file_path': '/root/path.sql',
        'package_name': 'test',
        'raw_sql': 'select * from {{ ref("thing") }}',
    }
    with pytest.raises(ValidationError):
        UnparsedNode.from_dict(node_dict)


def test_unparsed_run_hook_ok():
    node_dict = {
        'name': 'foo',
        'root_path': 'test/dbt_project.yml',
        'resource_type': NodeType.Operation,
        'path': '/root/dbt_project.yml',
        'original_file_path': '/root/dbt_project.yml',
        'package_name': 'test',
        'raw_sql': 'GRANT select on dbt_postgres',
        'index': 4
    }
    node = UnparsedRunHook(
        package_name='test',
        root_path='test/dbt_project.yml',
        path='/root/dbt_project.yml',
        original_file_path='/root/dbt_project.yml',
        raw_sql='GRANT select on dbt_postgres',
        name='foo',
        resource_type=NodeType.Operation,
        index=4,
    )
    assert UnparsedRunHook.from_dict(node_dict) == node
    assert node.to_dict() == node_dict
    with pytest.raises(ValidationError):
        UnparsedNode.from_dict(node_dict)


def test_unparsed_run_hook_bad_type():
    node_dict = {
        'name': 'foo',
        'root_path': 'test/dbt_project.yml',
        'resource_type': NodeType.Model,  # invalid
        'path': '/root/dbt_project.yml',
        'original_file_path': '/root/dbt_project.yml',
        'package_name': 'test',
        'raw_sql': 'GRANT select on dbt_postgres',
        'index': 4
    }
    with pytest.raises(ValidationError):
        UnparsedRunHook.from_dict(node_dict)
