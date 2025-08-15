"""Test the threaded import module."""

from unittest.mock import MagicMock, patch

from odoo_data_flow.import_threaded import (
    _create_batches,
    _execute_load_batch,
    _filter_ignored_columns,
    _prepare_pass_2_data,
    _read_data_file,
    import_data,
)


class TestDataReadingAndPreparation:
    """Tests for data reading and preparation helpers."""

    def test_read_data_file_success(self, tmp_path):
        """Test successful reading of a standard CSV file."""
        # Arrange
        file_path = tmp_path / "test.csv"
        file_path.write_text("id,name\n1,test1\n2,test2", encoding="utf-8")

        # Act
        header, data = _read_data_file(str(file_path), ",", "utf-8", 0)

        # Assert
        assert header == ["id", "name"]
        assert data == [["1", "test1"], ["2", "test2"]]

    def test_filter_ignored_columns(self):
        """Test that specified columns are correctly removed from data."""
        # Arrange
        header = ["id", "name", "age", "city"]
        data = [[1, "Alice", 30, "New York"], [2, "Bob", 25, "London"]]
        ignore = ["age", "city"]

        # Act
        new_header, new_data = _filter_ignored_columns(ignore, header, data)

        # Assert
        assert new_header == ["id", "name"]
        assert new_data == [[1, "Alice"], [2, "Bob"]]

    def test_prepare_pass_2_data(self):
        """Test the preparation of data for the second pass (updates)."""
        # Arrange
        header = ["id", "name", "parent_id"]
        all_data = [["child1", "C1", "parent1"], ["parent1", "P1", ""]]
        id_map = {"child1": 101, "parent1": 100}
        deferred_fields = ["parent_id"]
        unique_id_field_index = 0

        # Act
        pass_2_data = _prepare_pass_2_data(
            all_data, header, unique_id_field_index, id_map, deferred_fields
        )

        # Assert
        assert pass_2_data == [(101, {"parent_id": 100})]


class TestBatchCreation:
    """Tests for the _create_batches helper function."""

    def test_create_batches_simple(self):
        """Test simple batching without grouping."""
        # Arrange
        data = [[i] for i in range(10)]
        header = ["id"]

        # Act
        batches = list(_create_batches(data, None, header, 3, False))

        # Assert
        assert len(batches) == 4
        assert len(batches[0][1]) == 3
        assert len(batches[3][1]) == 1

    def test_create_batches_with_grouping(self):
        """Test batch creation with a grouping column."""
        # Arrange
        header = ["id", "group"]
        data = [[1, "A"], [2, "B"], [3, "A"], [4, "B"], [5, "A"]]

        # Act
        batches = list(_create_batches(data, ["group"], header, 10, False))

        # Assert
        # The data should be sorted by group, resulting in two batches
        assert len(batches) == 2
        # First batch should contain all 'A's
        assert all(row[1] == "A" for row in batches[0][1])
        # Second batch should contain all 'B's
        assert all(row[1] == "B" for row in batches[1][1])


class TestExecuteLoadBatch:
    """Tests for the _execute_load_batch worker function."""

    @patch("odoo_data_flow.import_threaded._create_batch_individually")
    def test_execute_load_batch_force_create(self, mock_create_individually):
        """Test that force_create bypasses load and calls individual creation."""
        # Arrange
        thread_state = {
            "force_create": True,
            "progress": MagicMock(),
            "model": MagicMock(),
            "unique_id_field_index": 0,
        }
        mock_create_individually.return_value = {"success": True}

        # Act
        result = _execute_load_batch(thread_state, [[]], [], 1)

        # Assert
        mock_create_individually.assert_called_once()
        assert result["success"] is True


class TestImportData:
    """Tests for the main import_data orchestrator."""

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
        mock_run_pass.assert_called_once()

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
        assert mock_run_pass.call_count == 2

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
