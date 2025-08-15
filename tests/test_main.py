"""Test cases for the __main__ module."""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from odoo_data_flow import __main__


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""
    return CliRunner()


# --- Project Mode Tests ---
@patch("odoo_data_flow.__main__.run_project_flow")
def test_project_mode_with_explicit_flow_file(
    mock_run_flow: MagicMock, runner: CliRunner
) -> None:
    """It should run project mode when --flow-file is explicitly provided."""
    with runner.isolated_filesystem():
        with open("test_flow.yml", "w") as f:
            f.write("flow: content")
        result = runner.invoke(__main__.cli, ["--flow-file", "test_flow.yml"])
        assert result.exit_code == 0
        mock_run_flow.assert_called_once_with("test_flow.yml", None)


@patch("odoo_data_flow.__main__.run_project_flow")
def test_project_mode_with_default_flow_file(
    mock_run_flow: MagicMock, runner: CliRunner
) -> None:
    """It should use flows.yml by default if it exists and no command is given."""
    with runner.isolated_filesystem():
        with open("flows.yml", "w") as f:
            f.write("default flow")
        result = runner.invoke(__main__.cli)
        assert result.exit_code == 0
        mock_run_flow.assert_called_once_with("flows.yml", None)


def test_shows_help_when_no_command_or_flow_file(runner: CliRunner) -> None:
    """It should show the help message when no command or flow file is found."""
    with runner.isolated_filesystem():
        result = runner.invoke(__main__.cli)
        assert result.exit_code == 0
        assert "Usage: cli" in result.output


# --- Single-Action Mode Tests (Refactored) ---


def test_import_fails_without_required_options(runner: CliRunner) -> None:
    """The import command should fail if required options are missing."""
    result = runner.invoke(__main__.cli, ["import"])
    assert result.exit_code != 0
    assert "Missing option" in result.output
    assert "--connection-file" in result.output


@patch("odoo_data_flow.__main__.run_import")
def test_import_command_calls_runner(
    mock_run_import: MagicMock, runner: CliRunner
) -> None:
    """Tests that the import command calls the correct runner function."""
    with runner.isolated_filesystem():
        with open("conn.conf", "w") as f:
            f.write("[Connection]")
        result = runner.invoke(
            __main__.cli,
            [
                "import",
                "--connection-file",
                "conn.conf",
                "--file",
                "my.csv",
                "--model",
                "res.partner",
            ],
        )
        assert result.exit_code == 0
        mock_run_import.assert_called_once()
        call_kwargs = mock_run_import.call_args.kwargs
        assert call_kwargs["config"] == "conn.conf"
        assert call_kwargs["filename"] == "my.csv"
        assert call_kwargs["model"] == "res.partner"


@patch("odoo_data_flow.__main__.run_export")
def test_export_command_calls_runner(
    mock_run_export: MagicMock, runner: CliRunner
) -> None:
    """Tests that the export command calls the correct runner function."""
    with runner.isolated_filesystem():
        with open("conn.conf", "w") as f:
            f.write("[Connection]")
        result = runner.invoke(
            __main__.cli,
            [
                "export",
                "--connection-file",
                "conn.conf",
                "--output",
                "my.csv",
                "--model",
                "res.partner",
                "--fields",
                "id,name",
            ],
        )
        assert result.exit_code == 0
        mock_run_export.assert_called_once()
        call_kwargs = mock_run_export.call_args[1]
        assert call_kwargs["config"] == "conn.conf"


@patch("odoo_data_flow.__main__.run_module_installation")
def test_module_install_command(mock_run_install: MagicMock, runner: CliRunner) -> None:
    """Tests the 'module install' command with the new connection file."""
    with runner.isolated_filesystem():
        with open("conn.conf", "w") as f:
            f.write("[Connection]")
        result = runner.invoke(
            __main__.cli,
            [
                "module",
                "install",
                "--connection-file",
                "conn.conf",
                "--modules",
                "sale,mrp",
            ],
        )
        assert result.exit_code == 0
        mock_run_install.assert_called_once_with(
            config="conn.conf", modules=["sale", "mrp"]
        )
