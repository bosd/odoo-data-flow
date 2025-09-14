"""Tests for the direct relational import strategy."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from rich.progress import Progress

from odoo_data_flow.lib import relational_import


@patch("odoo_data_flow.lib.relational_import.cache.load_id_map")
def test_run_direct_relational_import(
    mock_load_id_map: MagicMock,
    tmp_path: Path,
) -> None:
    """Verify the direct relational import workflow."""
    # Arrange
    source_df = pl.DataFrame(
        {
            "id": ["p1", "p2"],
            "name": ["Partner 1", "Partner 2"],
            "category_id": ["cat1,cat2", "cat2,cat3"],
        }
    )
    mock_load_id_map.return_value = pl.DataFrame(
        {"external_id": ["cat1", "cat2", "cat3"], "db_id": [11, 12, 13]}
    )

    strategy_details = {
        "relation_table": "res.partner.category.rel",
        "relation_field": "partner_id",
        "relation": "category_id",
    }
    id_map = {"p1": 1, "p2": 2}
    progress = Progress()
    task_id = progress.add_task("test")

    # Act
    result = relational_import.run_direct_relational_import(
        "dummy.conf",
        "res.partner",
        "category_id",
        strategy_details,
        source_df,
        id_map,
        1,
        10,
        progress,
        task_id,
        "source.csv",
    )

    # Assert
    assert isinstance(result, dict)
    assert "file_csv" in result
    assert "model" in result
    assert "unique_id_field" in result
    assert result["model"] == "res.partner.category.rel"
    assert result["unique_id_field"] == "partner_id"

    # Verify the content of the temporary CSV and cleanup
    temp_csv_path = result["file_csv"]
    try:
        df = pl.read_csv(temp_csv_path, truncate_ragged_lines=True)
        expected_df = pl.DataFrame(
            {
                "partner_id": [1, 1, 2, 2],
                "category_id/id": [11, 12, 12, 13],
            }
        )
        assert_frame_equal(df, expected_df, check_row_order=False)
    finally:
        Path(temp_csv_path).unlink(missing_ok=True)


@patch("odoo_data_flow.lib.relational_import.conf_lib.get_connection_from_config")
@patch("odoo_data_flow.lib.relational_import._resolve_related_ids")
def test_run_write_tuple_import(
    mock_resolve_ids: MagicMock,
    mock_get_conn: MagicMock,
) -> None:
    """Verify the write tuple import workflow."""
    # Arrange
    source_df = pl.DataFrame(
        {
            "id": ["p1", "p2"],
            "name": ["Partner 1", "Partner 2"],
            "category_id": ["cat1,cat2", "cat2,cat3"],
        }
    )
    mock_resolve_ids.return_value = pl.DataFrame(
        {"external_id": ["cat1", "cat2", "cat3"], "db_id": [11, 12, 13]}
    )
    mock_rel_model = MagicMock()
    mock_get_conn.return_value.get_model.return_value = mock_rel_model

    strategy_details = {
        "relation_table": "res.partner.category.rel",
        "relation_field": "partner_id",
        "relation": "category_id",
    }
    id_map = {"p1": 1, "p2": 2}
    progress = Progress()
    task_id = progress.add_task("test")

    # Act
    result = relational_import.run_write_tuple_import(
        "dummy.conf",
        "res.partner",
        "category_id",
        strategy_details,
        source_df,
        id_map,
        1,
        10,
        progress,
        task_id,
        "source.csv",
    )

    # Assert
    assert result is True
    assert mock_rel_model.create.call_count == 1


class TestResolveRelatedIds:
    """Tests for the _resolve_related_ids helper function."""

    @patch("odoo_data_flow.lib.relational_import.cache.load_id_map")
    def test_resolve_related_ids_cache_hit(self, mock_load_id_map: MagicMock) -> None:
        """Test that _resolve_related_ids returns the cached DataFrame on a hit."""
        cached_df = pl.DataFrame({"external_id": ["cat1"], "db_id": [11]})
        mock_load_id_map.return_value = cached_df
        result = relational_import._resolve_related_ids(
            "dummy.conf", "res.partner.category", pl.Series(["cat1"])
        )
        assert result is not None
        assert_frame_equal(result, cached_df)
        mock_load_id_map.assert_called_once_with("dummy.conf", "res.partner.category")

    @patch("odoo_data_flow.lib.relational_import.cache.load_id_map", return_value=None)
    @patch("odoo_data_flow.lib.relational_import.conf_lib.get_connection_from_config")
    def test_resolve_related_ids_failure(
        self, mock_get_conn: MagicMock, mock_load_id_map: MagicMock
    ) -> None:
        """Test that _resolve_related_ids returns None on failure."""
        mock_get_conn.return_value.get_model.return_value.search_read.return_value = []
        result = relational_import._resolve_related_ids(
            "dummy.conf", "res.partner", pl.Series(["p1.p1"])
        )
        assert result is None

    @patch("odoo_data_flow.lib.relational_import.cache.load_id_map", return_value=None)
    @patch("odoo_data_flow.lib.relational_import.conf_lib.get_connection_from_config")
    def test_resolve_related_ids_xml_rpc_error(
        self, mock_get_conn: MagicMock, mock_load_id_map: MagicMock
    ) -> None:
        """Test that an exception during XML-RPC is handled."""
        mock_get_conn.return_value.get_model.return_value.search_read.side_effect = Exception(
            "XML-RPC Error"
        )
        result = relational_import._resolve_related_ids(
            "dummy.conf", "res.partner", pl.Series(["p1.p1"])
        )
        assert result is None

    @patch("odoo_data_flow.lib.relational_import.cache.load_id_map", return_value=None)
    @patch("odoo_data_flow.lib.relational_import.conf_lib.get_connection_from_config")
    def test_resolve_related_ids_invalid_ids(
        self, mock_get_conn: MagicMock, mock_load_id_map: MagicMock
    ) -> None:
        """Test that invalid external IDs are skipped."""
        mock_get_conn.return_value.get_model.return_value.search_read.return_value = []
        result = relational_import._resolve_related_ids(
            "dummy.conf", "res.partner", pl.Series(["invalid_id"])
        )
        assert result is None

    @patch("odoo_data_flow.lib.relational_import.conf_lib.get_connection_from_dict")
    def test_resolve_related_ids_with_dict(self, mock_get_conn_dict: MagicMock) -> None:
        """Test _resolve_related_ids with a dictionary config."""
        mock_get_conn_dict.return_value.get_model.return_value.search_read.return_value = (
            []
        )
        result = relational_import._resolve_related_ids(
            {"host": "localhost"}, "res.partner", pl.Series(["p1.p1"])
        )
        assert result is None

    @patch("odoo_data_flow.lib.relational_import.cache.load_id_map", return_value=None)
    @patch(
        "odoo_data_flow.lib.relational_import.conf_lib.get_connection_from_config"
    )
    def test_resolve_related_ids_connection_error(
        self, mock_get_conn: MagicMock, mock_load_id_map: MagicMock
    ) -> None:
        """Test that _resolve_related_ids returns None on connection error."""
        mock_get_conn.return_value.is_connected.return_value = False
        result = relational_import._resolve_related_ids(
            "dummy.conf", "res.partner", pl.Series(["p1.p1"])
        )
        assert result is None


@patch("odoo_data_flow.lib.relational_import.conf_lib.get_connection_from_config")
def test_run_write_o2m_tuple_import(mock_get_conn: MagicMock) -> None:
    """Verify the o2m tuple import workflow."""
    # Arrange
    source_df = pl.DataFrame(
        {
            "id": ["p1"],
            "name": ["Partner 1"],
            "line_ids": ['[{"product": "prodA", "qty": 1}]'],
        }
    )
    mock_parent_model = MagicMock()
    mock_get_conn.return_value.get_model.return_value = mock_parent_model

    strategy_details: dict[str, str] = {}
    id_map = {"p1": 1}
    progress = Progress()
    task_id = progress.add_task("test")

    # Act
    result = relational_import.run_write_o2m_tuple_import(
        "dummy.conf",
        "res.partner",
        "line_ids",
        strategy_details,
        source_df,
        id_map,
        1,
        10,
        progress,
        task_id,
        "source.csv",
    )

    # Assert
    assert result is True
    mock_parent_model.write.assert_called_once_with(
        [1], {"line_ids": [(0, 0, {"product": "prodA", "qty": 1})]}
    )


def test_run_direct_relational_import_missing_details() -> None:
    """Test direct relational import with missing strategy details."""
    progress = Progress()
    task_id = progress.add_task("test")
    result = relational_import.run_direct_relational_import(
        "dummy.conf",
        "res.partner",
        "category_id",
        {},  # Empty strategy_details
        pl.DataFrame(),
        {},
        1,
        10,
        progress,
        task_id,
        "source.csv",
    )
    assert result is None


def test_run_write_tuple_import_missing_details() -> None:
    """Test write tuple import with missing strategy details."""
    progress = Progress()
    task_id = progress.add_task("test")
    result = relational_import.run_write_tuple_import(
        "dummy.conf",
        "res.partner",
        "category_id",
        {},  # Empty strategy_details
        pl.DataFrame(),
        {},
        1,
        10,
        progress,
        task_id,
        "source.csv",
    )
    assert result is False


@patch("odoo_data_flow.lib.relational_import.conf_lib.get_connection_from_config")
def test_run_write_o2m_tuple_import_parent_not_found(
    mock_get_conn: MagicMock,
) -> None:
    """Test o2m tuple import when parent ID is not in the map."""
    progress = Progress()
    task_id = progress.add_task("test")
    source_df = pl.DataFrame(
        {
            "id": ["p1"],
            "name": ["Partner 1"],
            "line_ids": ['[{"product": "prodA", "qty": 1}]'],
        }
    )
    mock_parent_model = MagicMock()
    mock_get_conn.return_value.get_model.return_value = mock_parent_model
    result = relational_import.run_write_o2m_tuple_import(
        "dummy.conf",
        "res.partner",
        "line_ids",
        {},
        source_df,
        {},  # Empty id_map
        1,
        10,
        progress,
        task_id,
        "source.csv",
    )
    assert result is False
    mock_parent_model.write.assert_not_called()


@patch("odoo_data_flow.lib.relational_import.conf_lib.get_connection_from_config")
def test_run_write_o2m_tuple_import_invalid_json(
    mock_get_conn: MagicMock,
) -> None:
    """Test o2m tuple import with invalid JSON."""
    progress = Progress()
    task_id = progress.add_task("test")
    source_df = pl.DataFrame(
        {
            "id": ["p1"],
            "name": ["Partner 1"],
            "line_ids": ["not valid json"],
        }
    )
    mock_parent_model = MagicMock()
    mock_get_conn.return_value.get_model.return_value = mock_parent_model
    result = relational_import.run_write_o2m_tuple_import(
        "dummy.conf",
        "res.partner",
        "line_ids",
        {},
        source_df,
        {"p1": 1},
        1,
        10,
        progress,
        task_id,
        "source.csv",
    )
    assert result is False
    mock_parent_model.write.assert_not_called()


@patch("odoo_data_flow.lib.relational_import.conf_lib.get_connection_from_config")
def test_run_write_o2m_tuple_import_write_fails(
    mock_get_conn: MagicMock,
) -> None:
    """Test o2m tuple import when the final write fails."""
    progress = Progress()
    task_id = progress.add_task("test")
    source_df = pl.DataFrame(
        {
            "id": ["p1"],
            "name": ["Partner 1"],
            "line_ids": ['[{"product": "prodA", "qty": 1}]'],
        }
    )
    mock_parent_model = MagicMock()
    mock_parent_model.write.side_effect = Exception("Write failed")
    mock_get_conn.return_value.get_model.return_value = mock_parent_model
    result = relational_import.run_write_o2m_tuple_import(
        "dummy.conf",
        "res.partner",
        "line_ids",
        {},
        source_df,
        {"p1": 1},
        1,
        10,
        progress,
        task_id,
        "source.csv",
    )
    assert result is False


@patch(
    "odoo_data_flow.lib.relational_import._resolve_related_ids", return_value=None
)
def test_run_write_tuple_import_resolve_fails(mock_resolve_ids: MagicMock) -> None:
    """Test write tuple import when ID resolution fails."""
    progress = Progress()
    task_id = progress.add_task("test")
    source_df = pl.DataFrame(
        {
            "id": ["p1"],
            "name": ["Partner 1"],
            "category_id": ["cat1"],
        }
    )
    strategy_details = {
        "relation_table": "res.partner.category.rel",
        "relation_field": "partner_id",
        "relation": "category_id",
    }
    result = relational_import.run_write_tuple_import(
        "dummy.conf",
        "res.partner",
        "category_id",
        strategy_details,
        source_df,
        {"p1": 1},
        1,
        10,
        progress,
        task_id,
        "source.csv",
    )
    assert result is False


@patch("odoo_data_flow.lib.relational_import.conf_lib.get_connection_from_config")
@patch("odoo_data_flow.lib.relational_import._resolve_related_ids")
def test_run_write_tuple_import_create_fails(
    mock_resolve_ids: MagicMock, mock_get_conn: MagicMock
) -> None:
    """Verify write tuple import handles create failures."""
    source_df = pl.DataFrame(
        {
            "id": ["p1"],
            "name": ["Partner 1"],
            "category_id": ["cat1"],
        }
    )
    mock_resolve_ids.return_value = pl.DataFrame(
        {"external_id": ["cat1"], "db_id": [11]}
    )
    mock_rel_model = MagicMock()
    mock_rel_model.create.side_effect = Exception("Create failed")
    mock_get_conn.return_value.get_model.return_value = mock_rel_model

    strategy_details = {
        "relation_table": "res.partner.category.rel",
        "relation_field": "partner_id",
        "relation": "category_id",
    }
    id_map = {"p1": 1}
    progress = Progress()
    task_id = progress.add_task("test")

    result = relational_import.run_write_tuple_import(
        "dummy.conf",
        "res.partner",
        "category_id",
        strategy_details,
        source_df,
        id_map,
        1,
        10,
        progress,
        task_id,
        "source.csv",
    )
    assert result is False


@patch(
    "odoo_data_flow.lib.relational_import._resolve_related_ids", return_value=None
)
def test_run_direct_relational_import_resolve_fails(
    mock_resolve_ids: MagicMock,
) -> None:
    """Test direct relational import when ID resolution fails."""
    progress = Progress()
    task_id = progress.add_task("test")
    source_df = pl.DataFrame(
        {
            "id": ["p1"],
            "name": ["Partner 1"],
            "category_id": ["cat1"],
        }
    )
    strategy_details = {
        "relation_table": "res.partner.category.rel",
        "relation_field": "partner_id",
        "relation": "category_id",
    }
    result = relational_import.run_direct_relational_import(
        "dummy.conf",
        "res.partner",
        "category_id",
        strategy_details,
        source_df,
        {"p1": 1},
        1,
        10,
        progress,
        task_id,
        "source.csv",
    )
    assert result is None
