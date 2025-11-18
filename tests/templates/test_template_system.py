import pytest
import yaml
from typer.main import get_command
from conduit_core.templates.registry import (
    TEMPLATE_REGISTRY,
    get_template,
    load_template_yaml,
)
from conduit_core.cli_plugins.template import template_app   
from click.testing import CliRunner


def test_registry_has_12_templates():
    assert len(TEMPLATE_REGISTRY) == 12


def test_all_templates_have_required_fields():
    for name, meta in TEMPLATE_REGISTRY.items():
        assert meta.get("description")
        assert meta.get("category")
        assert meta.get("source_type")
        assert meta.get("destination_type")
        assert meta.get("yaml_path")


@pytest.mark.parametrize("template_name", TEMPLATE_REGISTRY.keys())
def test_template_yaml_files_exist(template_name):
    yaml_content = load_template_yaml(template_name)
    assert yaml_content is not None
    assert yaml_content.strip() != ""


@pytest.mark.parametrize("template_name", TEMPLATE_REGISTRY.keys())
def test_templates_parse_as_valid_yaml(template_name):
    yaml_content = load_template_yaml(template_name)
    parsed = yaml.safe_load(yaml_content)
    assert isinstance(parsed, dict)
    assert "version" in parsed
    assert "sources" in parsed
    assert "destinations" in parsed
    assert "pipelines" in parsed


@pytest.mark.parametrize("template_name", TEMPLATE_REGISTRY.keys())
def test_templates_contain_inline_markers(template_name):
    yaml_content = load_template_yaml(template_name)
    assert "⚠️ UPDATE THIS" in yaml_content
    assert "HOW TO USE" in yaml_content
    assert "${" in yaml_content


def test_template_list_command(cli_runner):
    command = get_command(template_app)
    result = cli_runner.invoke(command, ["list"])
    assert result.exit_code == 0
    assert "AVAILABLE TEMPLATES" in result.output


def test_template_info_command(cli_runner):
    command = get_command(template_app)
    result = cli_runner.invoke(command, ["info", "csv_to_snowflake"])
    assert result.exit_code == 0
    assert "csv_to_snowflake" in result.output
    assert "CAPABILITIES" in result.output


def test_template_generate_command(cli_runner):
    command = get_command(template_app)
    result = cli_runner.invoke(command, ["generate", "csv_to_snowflake"])
    assert result.exit_code == 0

    parsed = yaml.safe_load(result.output)
    assert parsed["sources"][0]["type"] == "csv"
    assert parsed["destinations"][0]["type"] == "snowflake"
