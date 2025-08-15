"""Test the main importer orchestrator."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from odoo_data_flow.importer import (
    _count_lines,
    _infer_model_from_filename,
    run_import,
)


class TestFilenameUtils:
    """Tests for filename and path utility functions."""

    def test_count_lines(self, tmp_path):
        """Test that line counting works correctly."""
        file_path = tmp_path / "test.txt"
        file_path.write_text("line1\nline2\nline3")
        assert _count_lines(str(file_path)) == 3

    def test_infer_model_from_filename(self):
        """Test model name inference from various filename formats."""
        assert _infer_model_from_filename("res_partner.csv") == "res.partner"
        assert _infer_model_from_filename("sale_order_line.csv") == "sale.order.line"
        assert _infer_model_from_filename("x_custom_model.csv") == "x.custom.model"
        assert _infer_model_from_filename("res_partner_fail.csv") == "res.partner"
        assert _infer_model_from_filename("res_users_123.csv") == "res.users"


class TestRunImport:
    """Tests for the main run_import orchestrator function."""

    @patch("odoo_data_flow.importer.import_threaded.import_data")
    @patch("odoo_data_flow.importer._run_preflight_checks")
    def test_run_import_success_path(self, mock_preflight, mock_import_data, tmp_path):
        """Test the successful execution path of run_import."""
        # Arrange
        source_file = tmp_path / "source.csv"
        source_file.touch()
        mock_preflight.return_value = True
        mock_import_data.return_value = (True, {"total_records": 1})

        # Act
        run_import(
            config="dummy.conf",
            filename=str(source_file),
            model="res.partner",
            deferred_fields=None,
            unique_id_field=None,
            no_preflight_checks=False,
            headless=True,
            worker=1,
            batch_size=100,
            skip=0,
            fail=False,
            separator=";",
            ignore=None,
            context={},
            encoding="utf-8",
            o2m=False,
            groupby=None,
        )

        # Assert
        mock_preflight.assert_called_once()
        mock_import_data.assert_called_once()

    @patch("odoo_data_flow.importer._infer_model_from_filename")
    @patch("odoo_data_flow.importer._show_error_panel")
    def test_run_import_fails_if_model_not_found(
        self, mock_show_error, mock_infer_model
    ):
        """Test that the import aborts if no model can be determined."""
        # Arrange
        mock_infer_model.return_value = None

        # Act
        run_import(
            config="dummy.conf",
            filename="no_model.csv",
            model=None,  # No model provided
            deferred_fields=None,
            unique_id_field=None,
            no_preflight_checks=False,
            headless=True,
            worker=1,
            batch_size=100,
            skip=0,
            fail=False,
            separator=";",
            ignore=None,
            context={},
            encoding="utf-8",
            o2m=False,
            groupby=None,
        )

        # Assert
        mock_show_error.assert_called_once()
        assert "Model Not Found" in mock_show_error.call_args[0]

    @patch("odoo_data_flow.importer.import_threaded.import_data")
    def test_import_data_simple_success(
        self, mock_import_data: MagicMock, tmp_path: Path
    ) -> None:
        """Tests a simple, successful import with no failures."""
        source_file = tmp_path / "source.csv"
        mock_import_data.return_value = (True, {"created_records": 2})

        run_import(
            config=str(source_file),
            filename=str(source_file),
            model="res.partner",
            deferred_fields=None,
            unique_id_field="id",
            no_preflight_checks=True,
            headless=True,
            worker=1,
            batch_size=100,
            skip=0,
            fail=False,
            separator=";",
            ignore=[],
            context={},
            encoding="utf-8",
            o2m=False,
            groupby=None,
        )
        mock_import_data.assert_called_once()

    @patch("odoo_data_flow.importer.import_threaded.import_data")
    def test_import_data_two_pass_success(
        self, mock_import_data: MagicMock, tmp_path: Path
    ) -> None:
        """Tests a successful two-pass import with deferred fields."""
        source_file = tmp_path / "source.csv"
        mock_import_data.return_value = (True, {"created_records": 2})

        run_import(
            config=str(source_file),
            filename=str(source_file),
            model="res.partner",
            deferred_fields=["parent_id"],
            unique_id_field="id",
            no_preflight_checks=True,
            headless=True,
            worker=1,
            batch_size=100,
            skip=0,
            fail=False,
            separator=";",
            ignore=[],
            context={},
            encoding="utf-8",
            o2m=False,
            groupby=None,
        )
        mock_import_data.assert_called_once()
