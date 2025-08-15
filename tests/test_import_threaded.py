"""Tests for the refactored, low-level, multi-threaded import logic."""

from unittest.mock import MagicMock, patch

from rich.progress import Progress

from odoo_data_flow.import_threaded import (
    _orchestrate_pass_1,
    import_data,
)


class TestImportData:
    """Tests for the main `import_data` orchestrator."""

    @patch("odoo_data_flow.import_threaded._read_data_file")
    @patch("odoo_data_flow.import_threaded.conf_lib.get_connection_from_config")
    @patch("odoo_data_flow.import_threaded._run_threaded_pass")
    def test_import_data_success_path_no_defer(
        self,
        mock_run_pass: MagicMock,
        mock_get_conn: MagicMock,
        mock_read_file: MagicMock,
    ) -> None:
        """Test a successful single-pass import (no deferred fields)."""
        # Arrange
        mock_read_file.return_value = (["id", "name"], [["xml_a", "A"]])
        mock_run_pass.return_value = (
            {"id_map": {"xml_a": 101}, "failed_lines": []},  # results dict
            False,  # aborted = False
        )

        mock_get_conn.return_value.get_model.return_value = MagicMock()

        # Act
        result, _ = import_data(
            config="dummy.conf",
            model="res.partner",
            unique_id_field="id",
            file_csv="dummy.csv",
        )

        # Assert
        assert result is True
        mock_run_pass.assert_called_once()  # Only Pass 1 should run

    @patch("odoo_data_flow.import_threaded._read_data_file")
    @patch("odoo_data_flow.import_threaded.conf_lib.get_connection_from_config")
    @patch("odoo_data_flow.import_threaded._run_threaded_pass")
    def test_import_data_success_path_with_defer(
        self,
        mock_run_pass: MagicMock,
        mock_get_conn: MagicMock,
        mock_read_file: MagicMock,
    ) -> None:
        """Test a successful two-pass import (with deferred fields)."""
        # Arrange
        mock_read_file.return_value = (
            ["id", "name", "parent_id"],
            [["xml_a", "A", ""], ["xml_b", "B", "xml_a"]],
        )
        # Simulate results for Pass 1 and Pass 2
        mock_run_pass.side_effect = [
            (
                {"id_map": {"xml_a": 101, "xml_b": 102}, "failed_lines": []},
                False,
            ),  # Pass 1 (results, aborted)
            (
                {"failed_writes": []},
                False,
            ),  # Pass 2 (results, aborted)
        ]
        mock_get_conn.return_value.get_model.return_value = MagicMock()

        # Act
        result = import_data(
            config="dummy.conf",
            model="res.partner",
            unique_id_field="id",
            file_csv="dummy.csv",
            deferred_fields=["parent_id"],
        )

        # Assert
        assert result[0] is True
        assert mock_run_pass.call_count == 2  # Both passes should run

    @patch("odoo_data_flow.import_threaded._read_data_file")
    def test_import_data_fails_if_unique_id_not_in_header(
        self, mock_read_file: MagicMock
    ) -> None:
        """Test that the import fails if the unique_id_field is missing."""
        # Arrange
        mock_read_file.return_value = (["name"], [["A"]])  # No 'id' column

        # Act
        result, _ = import_data(
            config="dummy.conf",
            model="res.partner",
            unique_id_field="id",  # We expect 'id' but it's not there
            file_csv="dummy.csv",
        )

        # Assert
        assert result is False

    @patch("odoo_data_flow.import_threaded._create_batches")
    @patch("odoo_data_flow.import_threaded._run_threaded_pass")
    def test_orchestrate_pass_1_does_not_sort_for_o2m(
        self, mock_run_pass: MagicMock, mock_create_batches: MagicMock
    ) -> None:
        """Verify Pass 1 does NOT sort data when o2m is True."""
        mock_run_pass.return_value = ({}, False)
        header = ["id", "name", "parent_id"]
        data = [
            ["child1", "C1", "parent1"],
            ["parent1", "P1", ""],
        ]

        with Progress() as progress:
            _orchestrate_pass_1(
                progress,
                MagicMock(),
                "res.partner",
                header,
                data,
                "id",
                [],
                [],
                {},
                None,
                None,
                1,
                10,
                o2m=True,
                split_by_cols=None,
            )

        # Check that the data passed to _create_batches was NOT sorted
        call_args = mock_create_batches.call_args[0]
        unsorted_data = call_args[0]
        assert unsorted_data[0][0] == "child1"
        assert unsorted_data[1][0] == "parent1"
