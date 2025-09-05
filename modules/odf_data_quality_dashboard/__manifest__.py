{
    "name": "ODF Data Quality Dashboard",
    "version": "18.0.1.0.0",
    "summary": "Data Quality Dashboard for Odoo",
    "author": "OdooDataFlow",
    "website": "https://github.com/OdooDataFlow/odoo-data-flow",
    "license": "AGPL-3",
    "category": "Tools",
    "depends": ["base", "base_vat""],
    "data": [
        "security/ir.model.access.csv",
        "security/security.xml",
        "data/res_users.xml",
        "data/scheduled_actions.xml",
        "views/data_quality_issue_views.xml",
        "views/menus.xml",
    ],
    "installable": True,
}
