"""Module to manage data quality issues."""

from datetime import datetime, timedelta

from odoo import api, fields, models


class OdfDataQualityIssue(models.Model):
    """Represents a data quality issue found in the system.

    This model stores records of data inconsistencies or errors,
    allowing users to track and resolve them in a structured manner.
    """

    _name = "odf.data.quality.issue"
    _description = "Data Quality Issue"
    _order = "create_date desc"

    name = fields.Char(
        string="Title",
        required=True,
        help="A concise summary of the data quality issue.",
    )
    issue_type = fields.Char(
        string="Issue Type",
        required=True,
        help="The category of the issue, e.g., 'Invalid VAT'.",
    )
    related_record = fields.Reference(
        string="Related Record",
        selection=[("res.partner", "Partner"), ("product.product", "Product")],
        help="A reference to the record that has the data quality issue.",
    )
    status = fields.Selection(
        [
            ("todo", "To Do"),
            ("in_progress", "In Progress"),
            ("done", "Done"),
        ],
        string="Status",
        default="todo",
        required=True,
        help="The current stage of the issue resolution process.",
    )
    notes = fields.Text(
        string="Notes",
        help="Detailed comments or notes about the issue.",
    )

    # -------------------------------------------------------------------------
    # Business Methods
    # -------------------------------------------------------------------------
    @api.model
    def _run_nightly_validation(self):
        """Run all nightly data validation checks."""
        self._check_partners_with_missing_vat()

    @api.model
    def _check_partners_with_missing_vat(self):
        """Check for partners created in the last 24h with missing VAT."""
        yesterday = datetime.now() - timedelta(days=1)
        # Search for companies created in the last 24 hours without a VAT
        partners = self.env["res.partner"].search(
            [
                ("is_company", "=", True),
                ("create_date", ">=", yesterday),
                ("vat", "=", False),
            ]
        )
        vals_list = []
        for partner in partners:
            vals_list.append(
                {
                    "name": f"Missing VAT for Partner: {partner.name}",
                    "issue_type": "Missing VAT",
                    "related_record": f"res.partner,{partner.id}",
                    "status": "todo",
                    "notes": (
                        f"The partner '{partner.name}' is a company but does "
                        "not have a VAT number."
                    ),
                }
            )
        if vals_list:
            self.create(vals_list)
