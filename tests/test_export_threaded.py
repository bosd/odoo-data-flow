"""Test the Export Handling mechanism."""

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from odoo_data_flow.export_threaded import (
    RPCThreadExport,
    _clean_batch,
    _initialize_export,
    _process_export_batches,
    export_data,
)


@pytest.fixture
def mock_conf_lib() -> Generator[MagicMock, None, None]:
    """Fixture to mock conf_lib.get_connection_from_config."""
    with patch(
        "odoo_data_flow.export_threaded.conf_lib.get_connection_from_config"
    ) as mock_conn:
        mock_model_obj = MagicMock()
        mock_model_obj.fields_get.return_value = {
            "id": {"type": "integer"},
            "name": {"type": "char"},
            "is_company": {"type": "boolean"},
            "phone": {"type": "char"},
        }
        mock_connection = MagicMock()
        mock_connection.get_model.return_value = mock_model_obj
        mock_conn.return_value = mock_connection
        yield mock_conn


class TestInitializeExport:
    """Tests for the _initialize_export helper function."""

    @patch("odoo_data_flow.export_threaded.log")
    def test_initialize_export_warns_for_non_existent_field(
        self, mock_log: MagicMock, mock_conf_lib: MagicMock
    ) -> None:
        """Non exsisting field test.

        Tests that a warning is logged for a field that does not exist
        on the model.
        """
        # --- Arrange ---
        header = ["name", "non_existent_field"]
        mock_model = mock_conf_lib.return_value.get_model.return_value
        mock_model.fields_get.return_value = {
            "id": {"type": "integer"},
            "name": {"type": "char"},
        }

        # --- Act ---
        _initialize_export(
            config="dummy.conf",
            model_name="res.partner",
            header=header,
            technical_names=False,
        )

        # --- Assert ---
        mock_log.warning.assert_called_once()
        call_args, _ = mock_log.warning.call_args
        assert (
            "Field 'non_existent_field' (base: 'non_existent_field') not found"
            in call_args[0]
        )

    @patch("odoo_data_flow.export_threaded.log")
    def test_initialize_export_does_not_warn_for_valid_and_special_fields(
        self, mock_log: MagicMock, mock_conf_lib: MagicMock
    ) -> None:
        """Test no warning for special fields export.

        Tests that no warning is logged for valid fields, including special
        and relational syntax.
        """
        # --- Arrange ---
        header = [".id", "id", "name", "parent_id/id"]
        mock_model = mock_conf_lib.return_value.get_model.return_value
        mock_model.fields_get.return_value = {
            "id": {"type": "integer"},
            "name": {"type": "char"},
            "parent_id": {"type": "many2one"},
        }

        # --- Act ---
        _initialize_export(
            config="dummy.conf",
            model_name="res.partner",
            header=header,
            technical_names=False,
        )

        # --- Assert ---
        mock_log.warning.assert_not_called()


class TestRPCThreadExport:
    """Tests for the RPCThreadExport class."""

    def test_execute_batch_read_method(self) -> None:
        """Tests that model.read is called when use read method is True."""
        mock_model = MagicMock()
        mock_connection = MagicMock()
        fields_info = {"id": {"type": "integer"}}
        thread = RPCThreadExport(
            1,
            mock_connection,
            mock_model,
            ["id"],
            fields_info,
            technical_names=True,
        )
        thread._execute_batch([1], 1)
        mock_model.read.assert_called_once_with([1], ["id"])

    def test_execute_batch_export_data(self: "TestRPCThreadExport") -> None:
        """Tests that model.export_data is called when use read method is False."""
        mock_model = MagicMock()
        mock_connection = MagicMock()
        mock_model.export_data.return_value = {"datas": [["Test"]]}
        fields_info = {"name": {"type": "char"}}
        thread = RPCThreadExport(
            1,
            mock_connection,
            mock_model,
            ["name"],
            fields_info,
            technical_names=False,
        )
        thread._execute_batch([1], 1)
        mock_model.export_data.assert_called_once()

    def test_execute_batch_handles_json_decode_error(self) -> None:
        """Test JSONDecodeError.

        Tests that a JSONDecodeError is handled gracefully during export.
        """
        # 1. Setup
        mock_model = MagicMock()
        mock_connection = MagicMock()
        mock_model.read.side_effect = httpx.DecodingError(
            "Expecting value", request=None
        )
        fields_info = {"id": {"type": "integer"}}
        thread = RPCThreadExport(
            1,
            mock_connection,
            mock_model,
            ["id"],
            fields_info,
            technical_names=True,
        )

        # 2. Action
        with patch("odoo_data_flow.export_threaded.log.error") as mock_log_error:
            result = thread._execute_batch([1], 1)

            # 3. Assert
            assert result == ([], [])  # Should return an empty list on failure
            mock_log_error.assert_called_once()
            assert "failed permanently" in mock_log_error.call_args[0][0]
            assert "network error" not in mock_log_error.call_args[0][0]

    def test_rpc_thread_export_memory_error(self) -> None:
        """Test for memory errors.

        Test that the RPCThreadExport class handles MemoryError and subsequent failures.
        """
        mock_model = MagicMock()
        mock_connection = MagicMock()
        mock_model.read.side_effect = [
            Exception({"data": {"name": "builtins.MemoryError"}}),
            [{"id": 1}],
            Exception("A permanent error"),
        ]
        fields_info = {"id": {"type": "integer"}}
        thread = RPCThreadExport(
            1,
            mock_connection,
            mock_model,
            ["id"],
            fields_info,
            technical_names=True,
        )
        result = thread._execute_batch([1, 2, 3], 1)
        assert result == ([{"id": 1}], [1])


class TestCleanBatch:
    """Tests for the _clean_batch utility function."""

    def test_clean_batch_creates_dataframe(self) -> None:
        """Tests that a DataFrame is created correctly from a list of dicts."""
        # Arrange
        test_data = [
            {"id": 1, "name": "Test 1"},
            {"id": 2, "name": "Test 2"},
        ]

        # Act
        result_df = _clean_batch(test_data)

        # Assert
        assert isinstance(result_df, pl.DataFrame)
        assert len(result_df) == 2
        expected_df = pl.DataFrame(test_data)
        assert_frame_equal(result_df, expected_df)

    def test_clean_batch_empty_input(self) -> None:
        """Tests that an empty list is handled correctly."""
        # Act & Assert
        assert _clean_batch([]).is_empty()

    def test_clean_batch_with_boolean(self) -> None:
        """Test that _clean_batch handles boolean values correctly."""
        data = [{"id": 1, "active": True}, {"id": 2, "active": False}]
        # field_types = {"id": "integer", "active": "boolean"}
        df = _clean_batch(data)
        assert df.to_dicts() == data


class TestExportData:
    """Tests for the main export_data orchestrator function."""

    def test_export_in_memory_success(self, mock_conf_lib: MagicMock) -> None:
        """Tests the success path for a default in-memory export."""
        # --- Arrange ---
        header = ["id", "name", "active"]
        mock_model = mock_conf_lib.return_value.get_model.return_value
        mock_model.search.return_value = [1, 2]

        # *** FIX ***: Mock 'export_data', not 'read', for this test case.
        # The return format is a dict with a 'datas' key containing a list of lists.
        mock_model.export_data.return_value = {
            "datas": [
                ["xml_id.1", "Test 1", True],
                ["xml_id.2", "Test 2", False],
            ]
        }

        # The fields_get mock is still correct.
        mock_model.fields_get.return_value = {
            "id": {"type": "char"},
            "name": {"type": "char"},
            "active": {"type": "boolean"},
        }

        # --- Act ---
        _, _, _, result_df = export_data(
            config="dummy.conf",
            model="res.partner",
            domain=[],
            header=header,
            output=None,
            technical_names=False,  # This correctly invokes the export_data path
        )

        # --- Assert ---
        assert result_df is not None
        expected_df = pl.DataFrame(
            {
                "id": ["xml_id.1", "xml_id.2"],
                "name": ["Test 1", "Test 2"],
                "active": [True, False],
            }
        )
        assert_frame_equal(result_df, expected_df)

    def test_export_to_file_default_mode_success(
        self, mock_conf_lib: MagicMock, tmp_path: Path
    ) -> None:
        """Tests a file export using the default in-memory concat mode."""
        # --- Arrange ---
        header = ["id", "name"]
        output_file = tmp_path / "output.csv"
        mock_model = mock_conf_lib.return_value.get_model.return_value
        mock_model.search.return_value = [1, 2]
        mock_model.read.return_value = [
            {"id": 1, "name": "Test 1"},
            {"id": 2, "name": "Test 2"},
        ]
        mock_model.fields_get.return_value = {
            "id": {"type": "integer"},
            "name": {"type": "char"},
        }

        # --- Act ---
        _, _, _, result_df = export_data(
            config="dummy.conf",
            model="res.partner",
            domain=[],
            header=header,
            output=str(output_file),
            technical_names=True,
            streaming=False,
        )

        # --- Assert ---
        assert output_file.exists()
        assert result_df is not None

        expected_df = pl.DataFrame(
            {
                "id": [1, 2],
                "name": ["Test 1", "Test 2"],
            }
        ).with_columns(pl.col("id").cast(pl.Int64))

        # Add separator and sort both frames
        on_disk_df = pl.read_csv(output_file, separator=";")
        assert_frame_equal(result_df.sort("id"), expected_df.sort("id"))
        assert_frame_equal(on_disk_df.sort("id"), expected_df.sort("id"))

    def test_export_to_file_streaming_success(
        self, mock_conf_lib: MagicMock, tmp_path: Path
    ) -> None:
        """Tests the success path where data is streamed to a CSV file."""
        # --- Arrange ---
        header = ["id", "name"]
        output_file = tmp_path / "output.csv"
        mock_model = mock_conf_lib.return_value.get_model.return_value
        mock_model.search.return_value = [1, 2]
        mock_model.read.side_effect = [
            [{"id": 2, "name": "Test 2"}],  # Simulate out-of-order completion
            [{"id": 1, "name": "Test 1"}],
        ]
        mock_model.fields_get.return_value = {
            "id": {"type": "integer"},
            "name": {"type": "char"},
        }

        # --- Act ---
        success, _, _, result_df = export_data(
            config="dummy.conf",
            model="res.partner",
            domain=[],
            header=header,
            output=str(output_file),
            technical_names=True,
            streaming=True,
            batch_size=1,
        )

        # --- Assert ---
        assert success is True
        assert result_df is None
        assert output_file.exists()

        # Add separator and sort both frames
        on_disk_df = pl.read_csv(output_file, separator=";")
        expected_df = pl.DataFrame({"id": [1, 2], "name": ["Test 1", "Test 2"]})
        assert_frame_equal(on_disk_df.sort("id"), expected_df.sort("id"))

    def test_export_handles_connection_failure(self) -> None:
        """Tests that None is returned if the initial connection fails."""
        with patch(
            "odoo_data_flow.export_threaded.conf_lib.get_connection_from_config",
            side_effect=Exception("Connection Error"),
        ):
            success, _, _, result = export_data(
                config="bad.conf",
                model="res.partner",
                domain=[],
                header=["id"],
                output="fail.csv",
            )
        assert success is False
        assert result is None

    def test_export_handles_no_records_found(
        self, mock_conf_lib: MagicMock, tmp_path: Path
    ) -> None:
        """Tests behavior when no records match the domain."""
        output_file = tmp_path / "empty.csv"
        mock_model = mock_conf_lib.return_value.get_model.return_value
        mock_model.search.return_value = []
        mock_model.fields_get.return_value = {
            "id": {"type": "integer"},
            "name": {"type": "char"},
        }

        success, _, _, result_df = export_data(
            config="dummy.conf",
            model="res.partner",
            domain=[],
            header=["id", "name"],
            output=str(output_file),
            separator=",",
        )

        assert success is True
        assert output_file.exists()

        with open(output_file) as f:
            assert f.read().strip() == "id,name"
        assert output_file.exists()
        assert result_df is not None
        assert result_df.is_empty()

    def test_export_handles_memory_error_fallback(
        self, mock_conf_lib: MagicMock, tmp_path: Path
    ) -> None:
        """Tests that the batch is split and retried on server MemoryError."""
        # --- Arrange ---
        output_file = tmp_path / "output.csv"
        mock_model = mock_conf_lib.return_value.get_model.return_value
        mock_model.search.return_value = [1, 2, 3, 4]

        # Simulate Odoo failing with MemoryError on the first large batch,
        # then succeeding on the two smaller retry batches.
        memory_error_response = Exception(
            {
                "code": 200,
                "message": "Odoo Server Error",
                "data": {"name": "builtins.MemoryError", "debug": "..."},
            }
        )
        mock_model.read.side_effect = [
            memory_error_response,
            [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}],  # 1st retry
            [{"id": 3, "name": "C"}, {"id": 4, "name": "D"}],  # 2nd retry
        ]
        mock_model.fields_get.return_value = {
            "id": {"type": "integer"},
            "name": {"type": "char"},
        }

        # --- Act ---
        success, _, _, result_df = export_data(
            config="dummy.conf",
            model="res.partner",
            domain=[],
            header=["id", "name"],
            output=str(output_file),
            technical_names=True,
            streaming=True,
            batch_size=4,
        )

        # --- Assert ---
        assert success is True
        assert result_df is None
        assert output_file.exists()
        assert mock_model.read.call_count == 3  # 1 failure + 2 retries

        # Verify the final file has all data from the successful retries
        on_disk_df = pl.read_csv(output_file, separator=";")
        expected_df = pl.DataFrame({"id": [1, 2, 3, 4], "name": ["A", "B", "C", "D"]})
        assert_frame_equal(on_disk_df.sort("id"), expected_df.sort("id"))

    def test_export_handles_empty_batch_result(
        self, mock_conf_lib: MagicMock, tmp_path: Path
    ) -> None:
        """Tests that an empty result from a batch is handled gracefully."""
        # --- Arrange ---
        output_file = tmp_path / "output.csv"
        mock_model = mock_conf_lib.return_value.get_model.return_value
        mock_model.search.return_value = [1, 2]
        # Simulate one batch succeeding and one returning no data
        mock_model.read.side_effect = [[{"id": 1, "name": "A"}], []]
        mock_model.fields_get.return_value = {
            "id": {"type": "integer"},
            "name": {"type": "char"},
        }

        # --- Act ---
        export_data(
            config="dummy.conf",
            model="res.partner",
            domain=[],
            header=["id", "name"],
            output=str(output_file),
            technical_names=True,
            batch_size=1,
        )

        # --- Assert ---
        # The file should contain only the data from the successful batch
        on_disk_df = pl.read_csv(output_file, separator=";")
        assert len(on_disk_df) == 1
        assert on_disk_df["id"][0] == 1

    def test_export_handles_permanent_worker_failure(
        self, mock_conf_lib: MagicMock, tmp_path: Path
    ) -> None:
        """Tests that a non-MemoryError exception in a worker is survivable."""
        # --- Arrange ---
        output_file = tmp_path / "output.csv"
        mock_model = mock_conf_lib.return_value.get_model.return_value
        mock_model.search.return_value = [1, 2]
        # Simulate one batch succeeding and one failing with a different error
        mock_model.read.side_effect = [
            [{"id": 1, "name": "A"}],
            ValueError("A permanent error"),
        ]
        mock_model.fields_get.return_value = {
            "id": {"type": "integer"},
            "name": {"type": "char"},
        }

        # --- Act ---
        export_data(
            config="dummy.conf",
            model="res.partner",
            domain=[],
            header=["id", "name"],
            output=str(output_file),
            technical_names=True,
            batch_size=1,
        )

        # --- Assert ---
        # The export should complete with data from the successful batch
        assert output_file.exists()
        on_disk_df = pl.read_csv(output_file, separator=";")
        assert len(on_disk_df) == 1

    def test_initialize_export_connection_error(self, mock_conf_lib: MagicMock) -> None:
        """Tests that the function handles connection errors gracefully."""
        mock_conf_lib.side_effect = Exception("Connection Refused")

        # The function now returns three values
        connection, model_obj, fields_info = _initialize_export(
            "dummy.conf", "res.partner", ["name"], False
        )

        assert connection is None
        assert model_obj is None
        assert fields_info is None

    @patch("odoo_data_flow.export_threaded._determine_export_strategy")
    def test_export_data_streaming_no_output(
        self, mock_determine_export_strategy: MagicMock
    ) -> None:
        """Tests that streaming mode without an output path returns None."""
        mock_determine_export_strategy.return_value = (
            MagicMock(),
            MagicMock(),
            {"name": {"type": "char"}},
            False,
            False,
        )

        success, _, _, result_df = export_data(
            config="dummy.conf",
            model="res.partner",
            domain=[],
            header=["name"],
            output=None,
            streaming=True,
        )
        assert success is False
        assert result_df is None

    @patch("concurrent.futures.as_completed")
    def test_process_export_batches_task_failure(
        self, mock_as_completed: MagicMock
    ) -> None:
        """Test that _process_export_batches handles a failing future."""
        mock_future = MagicMock()
        mock_future.result.side_effect = Exception("Task failed")
        mock_as_completed.return_value = [mock_future]
        mock_rpc_thread = MagicMock()
        mock_rpc_thread.futures = [mock_future]

        result = _process_export_batches(
            mock_rpc_thread,
            1,
            "res.partner",
            None,
            {},
            ";",
            False,
            None,
            False,
            "utf-8",
        )
        if result is not None:
            assert result.is_empty()

    @patch("concurrent.futures.as_completed")
    def test_process_export_batches_empty_result(
        self, mock_as_completed: MagicMock
    ) -> None:
        """Test that _process_export_batches handles an empty result from a future."""
        mock_future = MagicMock()
        mock_future.result.return_value = ([], [])
        mock_as_completed.return_value = [mock_future]
        mock_rpc_thread = MagicMock()
        mock_rpc_thread.futures = [mock_future]

        result = _process_export_batches(
            mock_rpc_thread,
            1,
            "res.partner",
            None,
            {},
            ";",
            False,
            None,
            False,
            "utf-8",
        )
        if result is not None:
            assert result.is_empty()

    def test_process_export_batches_no_dfs_with_output(self, tmp_path: Path) -> None:
        """Test _process_export_batches with no dataframes and an output file."""
        mock_rpc_thread = MagicMock()
        mock_rpc_thread.futures = []
        mock_rpc_thread.has_failures = False
        output_file = tmp_path / "output.csv"

        fields_info = {"id": {"type": "integer"}}

        with patch("polars.DataFrame.write_csv") as mock_write_csv:
            result = _process_export_batches(
                mock_rpc_thread,
                0,
                "res.partner",
                str(output_file),
                fields_info,
                ";",
                False,
                None,
                False,
                "utf-8",
            )
        assert result is not None
        assert result.is_empty()
        mock_write_csv.assert_called_once()

    def test_export_relational_raw_id_success(self, mock_conf_lib: MagicMock) -> None:
        """Test Relational Raw id.

        Tests that requesting a relational field with '/.id' triggers read mode
        and correctly returns an integer database ID.
        """
        # --- Arrange ---
        header = ["name", "parent_id/.id"]
        mock_model = mock_conf_lib.return_value.get_model.return_value
        mock_model.search.return_value = [10, 20]

        # *** FIX ***: The read() mock must include the 'id' of the records being read.
        mock_model.read.return_value = [
            {
                "id": 10,
                "name": "Child Category",
                "parent_id": (5, "Parent Category"),
            },
            {"id": 20, "name": "Root Category", "parent_id": False},
        ]

        mock_model.fields_get.return_value = {
            "id": {"type": "integer"},
            "name": {"type": "char"},
            "parent_id": {"type": "many2one"},
        }

        # --- Act ---
        _, _, _, result_df = export_data(
            config="dummy.conf",
            model="res.partner.category",
            domain=[],
            header=header,
            output=None,
        )

        # --- Assert ---
        assert result_df is not None
        expected_df = pl.DataFrame(
            {
                "name": ["Child Category", "Root Category"],
                "parent_id/.id": [5, None],
            }
        ).with_columns(pl.col("parent_id/.id").cast(pl.Int64))

        assert_frame_equal(result_df, expected_df)

    def test_export_hybrid_mode_success(self, mock_conf_lib: MagicMock) -> None:
        """Test the hybrid mode.

        Tests that the hybrid export mode correctly fetches raw IDs and
        enriches the data with related XML IDs.
        """
        # --- Arrange ---
        header = [".id", "parent_id/id"]
        mock_model = mock_conf_lib.return_value.get_model.return_value
        mock_model.search.return_value = [10]

        # 1. Mock the metadata call (_initialize_export)
        mock_model.fields_get.return_value = {
            "id": {"type": "integer"},
            "parent_id": {
                "type": "many2one",
                "relation": "res.partner.category",
            },
        }

        # 2. Mock the primary read() call
        mock_model.read.return_value = [{"id": 10, "parent_id": (5, "Parent Category")}]

        # 3. Mock the secondary XML ID lookup on 'ir.model.data'
        mock_ir_model_data = MagicMock()
        mock_ir_model_data.search_read.return_value = [
            {"res_id": 5, "module": "base", "name": "cat_parent"}
        ]
        mock_conf_lib.return_value.get_model.side_effect = [
            mock_model,
            mock_ir_model_data,
        ]

        # --- Act ---
        _, _, _, result_df = export_data(
            config="dummy.conf",
            model="res.partner.category",
            domain=[],
            header=header,
            output=None,
        )

        # --- Assert ---
        assert result_df is not None
        expected_df = pl.DataFrame(
            {".id": [10], "parent_id/id": ["base.cat_parent"]},
            schema={".id": pl.Int64, "parent_id/id": pl.String},
        )
        assert_frame_equal(result_df, expected_df)

    def test_export_id_and_dot_id_in_read_mode(self, mock_conf_lib: MagicMock) -> None:
        """Test the read mode.

        Tests that in read() mode, both 'id' and '.id' correctly resolve
        to the integer database ID.
        """
        # --- Arrange ---
        header = [".id", "id", "name"]
        mock_model = mock_conf_lib.return_value.get_model.return_value
        mock_model.search.return_value = [101, 102]
        mock_model.read.return_value = [
            {"id": 101, "name": "Record 101"},
            {"id": 102, "name": "Record 102"},
        ]
        mock_model.fields_get.return_value = {
            "id": {"type": "integer"},
            "name": {"type": "char"},
        }

        # --- Act ---
        _, _, _, result_df = export_data(
            config="dummy.conf",
            model="res.partner",
            domain=[],
            header=header,
            output=None,
            technical_names=True,
        )

        # --- Assert ---
        assert result_df is not None

        # *** FIX ***: Use the 'schema' argument to define dtypes on creation.
        expected_df = pl.DataFrame(
            {
                ".id": [101, 102],
                "id": [101, 102],
                "name": ["Record 101", "Record 102"],
            },
            schema={".id": pl.Int64, "id": pl.Int64, "name": pl.String},
        )

        assert_frame_equal(result_df, expected_df)

    def test_export_id_in_export_data_mode(self, mock_conf_lib: MagicMock) -> None:
        """Test export id in export data.

        Tests that in export_data mode, the 'id' field correctly resolves
        to a string (for XML IDs).
        """
        # --- Arrange ---
        header = ["id", "name"]
        mock_model = mock_conf_lib.return_value.get_model.return_value
        mock_model.search.return_value = [1, 2]
        mock_model.export_data.return_value = {
            "datas": [
                ["__export__.rec_1", "Record 1"],
                ["__export__.rec_2", "Record 2"],
            ]
        }
        mock_model.fields_get.return_value = {
            "id": {"type": "integer"},  # Odoo still says it's an integer
            "name": {"type": "char"},
        }

        # --- Act ---
        _, _, _, result_df = export_data(
            config="dummy.conf",
            model="res.partner",
            domain=[],
            header=header,
            output=None,
            technical_names=False,  # Explicitly use export_data mode
        )

        # --- Assert ---
        assert result_df is not None
        expected_df = pl.DataFrame(
            {
                "id": ["__export__.rec_1", "__export__.rec_2"],
                "name": ["Record 1", "Record 2"],
            }
        )
        # Check that our logic correctly inferred the 'id' column should be a string
        assert result_df.schema["id"] == pl.String
        assert_frame_equal(result_df, expected_df)

    @patch("odoo_data_flow.export_threaded.concurrent.futures.as_completed")
    @patch("odoo_data_flow.export_threaded.RPCThreadExport")
    def test_export_auto_enables_read_mode_for_selection_field(
        self,
        mock_rpc_thread_class: MagicMock,
        mock_as_completed: MagicMock,
        mock_conf_lib: MagicMock,
    ) -> None:
        """Test read mode for selection fields.

        Tests that including a 'selection' field automatically triggers the
        'read' method to export the raw technical value.
        """
        # --- Arrange ---
        header = ["name", "state"]
        mock_connection = mock_conf_lib.return_value
        mock_model = mock_connection.get_model.return_value
        mock_model.search.return_value = [1]

        mock_model.fields_get.return_value = {
            "id": {"type": "integer"},
            "name": {"type": "char"},
            "state": {"type": "selection"},
        }

        # FIX: Mock the result that the processing loop will receive
        mock_future = MagicMock()
        mock_future.result.return_value = (
            [{"name": "Test Record", "state": "done", "id": 1}],
            [1],
        )
        mock_as_completed.return_value = [mock_future]

        # --- Act ---
        _, _, _, result_df = export_data(
            config="dummy.conf",
            model="sale.order",
            domain=[],
            header=header,
            output=None,
        )

        # --- Assert ---
        _init_args, init_kwargs = mock_rpc_thread_class.call_args
        assert init_kwargs.get("technical_names") is True, "Read mode was not triggered"

        assert result_df is not None
        expected_df = pl.DataFrame({"name": ["Test Record"], "state": ["done"]})
        assert_frame_equal(result_df, expected_df, check_dtypes=False)

    @patch("odoo_data_flow.export_threaded.concurrent.futures.as_completed")
    @patch("odoo_data_flow.export_threaded.RPCThreadExport")
    def test_export_auto_enables_read_mode_for_binary_field(
        self,
        mock_rpc_thread_class: MagicMock,
        mock_as_completed: MagicMock,
        mock_conf_lib: MagicMock,
    ) -> None:
        """Test read mode for selection fields.

        Tests that including a 'binary' field automatically triggers the
        'read' method to export the base64 data.
        """
        # --- Arrange ---
        header = ["name", "datas"]
        mock_connection = mock_conf_lib.return_value
        mock_model = mock_connection.get_model.return_value
        mock_model.search.return_value = [1]

        mock_model.fields_get.return_value = {
            "id": {"type": "integer"},
            "name": {"type": "char"},
            "datas": {"type": "binary"},
        }

        # FIX: Mock the result that the processing loop will receive
        mock_future = MagicMock()
        mock_future.result.return_value = (
            [{"name": "test.zip", "datas": "UEsDBAoAAAAA...", "id": 1}],
            [1],
        )
        mock_as_completed.return_value = [mock_future]

        # --- Act ---
        _, _, _, result_df = export_data(
            config="dummy.conf",
            model="ir.attachment",
            domain=[],
            header=header,
            output=None,
        )

        # --- Assert ---
        _init_args, init_kwargs = mock_rpc_thread_class.call_args
        assert init_kwargs.get("technical_names") is True, "Read mode was not triggered"

        assert result_df is not None
        expected_df = pl.DataFrame({"name": ["test.zip"], "datas": ["UEsDBAoAAAAA..."]})
        assert_frame_equal(result_df, expected_df)

    @patch("odoo_data_flow.export_threaded.concurrent.futures.as_completed")
    @patch("odoo_data_flow.export_threaded._clean_batch")
    @patch("odoo_data_flow.export_threaded.Progress")
    def test_process_export_batches_handles_inconsistent_schemas(
        self,
        mock_progress: MagicMock,
        mock_clean_batch: MagicMock,
        mock_as_completed: MagicMock,
    ) -> None:
        """Tests that batches with inconsistent schemas are correctly concatenated.

        This validates the fix for the `SchemaError` that occurred when one batch
        inferred a column as Boolean and another inferred it as String. The test
        ensures that the final concatenated DataFrame has a consistent, correct schema.
        """
        # --- Arrange ---
        mock_rpc_thread = MagicMock(spec=RPCThreadExport)
        mock_rpc_thread.has_failures = False
        future1, future2 = MagicMock(), MagicMock()
        future1.result.return_value = ([{"id": 1, "is_special": True}], [1])
        future2.result.return_value = ([{"id": 2, "is_special": "False"}], [2])
        mock_rpc_thread.futures = [future1, future2]
        mock_as_completed.return_value = [future1, future2]
        mock_rpc_thread.executor = MagicMock()
        mock_rpc_thread.executor.shutdown.return_value = None

        mock_clean_batch.side_effect = [
            pl.DataFrame({"id": [1], "is_special": [True]}),
            pl.DataFrame({"id": [2], "is_special": ["False"]}),
        ]

        fields_info = {
            "id": {"type": "integer"},
            "is_special": {"type": "boolean"},
        }
        total_ids = 2

        # --- Act ---
        final_df = _process_export_batches(
            rpc_thread=mock_rpc_thread,
            total_ids=total_ids,
            model_name="test.model",
            output=None,
            fields_info=fields_info,
            separator=",",
            streaming=False,
            session_dir=None,
            is_resuming=False,
            encoding="utf-8",
        )
        assert final_df is not None

        # --- Assert ---
        expected_schema = {"id": pl.Int64(), "is_special": pl.Boolean()}
        expected_df = pl.DataFrame(
            {"id": [1, 2], "is_special": [True, False]},
            schema=expected_schema,
        )
        final_df = final_df.sort("id")
        assert_frame_equal(final_df, expected_df)

    def test_export_with_non_existent_fields(
        self, mock_conf_lib: MagicMock, tmp_path: Path
    ) -> None:
        """Tests that exporting with non-existent fields completes and adds null columns."""
        # --- Arrange ---
        header = ["id", "name", "field_does_not_exist", "another_bad_field"]
        output_file = tmp_path / "output.csv"
        mock_model = mock_conf_lib.return_value.get_model.return_value
        mock_model.search.return_value = [1, 2]
        mock_model.read.return_value = [
            {"id": 1, "name": "Test 1"},
            {"id": 2, "name": "Test 2"},
        ]
        mock_model.fields_get.return_value = {
            "id": {"type": "integer"},
            "name": {"type": "char"},
        }

        # --- Act ---
        success, _, _, result_df = export_data(
            config="dummy.conf",
            model="res.partner",
            domain=[],
            header=header,
            output=str(output_file),
            technical_names=True,
        )

        # --- Assert ---
        assert success is True
        assert result_df is not None

        expected_df = pl.DataFrame(
            {
                "id": [1, 2],
                "name": ["Test 1", "Test 2"],
                "field_does_not_exist": [None, None],
                "another_bad_field": [None, None],
            }
        ).with_columns(
            pl.col("id").cast(pl.Int64),
            pl.col("field_does_not_exist").cast(pl.String),
            pl.col("another_bad_field").cast(pl.String),
        )

        # Reorder columns to match expected output
        result_df = result_df[expected_df.columns]

        assert_frame_equal(result_df.sort("id"), expected_df.sort("id"))
