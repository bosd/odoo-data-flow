"""Odoo module manifest for the Data Quality Dashboard."""

{
    "name": "ODF Data Quality Dashboard",
    "summary": """
        Provides a dashboard to identify and manage data quality issues
        after data import.""",
    "author": "OdooDataFlow",
    "website": "https://github.com/OdooDataFlow/odoo-data-flow",
    "category": "Tools",
    "version": "18.0.1.0.0",
    "depends": ["base"],
    "data": [
        "security/ir.model.access.csv",
        "views/odf_data_quality_issue_views.xml",
        "data/ir_cron_data.xml",
    ],
    "installable": True,
    "application": True,
}
