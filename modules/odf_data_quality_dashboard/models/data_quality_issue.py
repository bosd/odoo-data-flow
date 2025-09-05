# -*- coding: utf-8 -*-
from odoo import api, fields, models


class DataQualityIssue(models.Model):
    _name = 'odf.data.quality.issue'
    _description = 'Data Quality Issue'

    name = fields.Char(
        string='Name',
        required=True,
    )
    issue_type = fields.Char(
        string='Issue Type',
    )
    related_record = fields.Reference(
        selection=[
            ('res.partner', 'Partner'),
            ('product.product', 'Product'),
        ],
        string='Related Record',
    )
    status = fields.Selection(
        selection=[
            ('new', 'New'),
            ('in_progress', 'In Progress'),
            ('resolved', 'Resolved'),
        ],
        string='Status',
        default='new',
    )
    notes = fields.Text(
        string='Notes',
    )

    @api.model
    def _run_data_quality_checks(self):
        """Dispatcher for all data quality checks."""
        self._check_partner_vat()

    @api.model
    def _check_partner_vat(self):
        """
        Check for partners with invalid VAT numbers using Odoo's built-in
        VAT validation and a batched VIES check.
        """
        from datetime import timedelta
        import time

        yesterday = fields.Datetime.now() - timedelta(days=1)

        # 1. Fetch partners that need checking.
        partners_to_check = self.env["res.partner"].search([
            ("write_date", ">=", fields.Datetime.to_string(yesterday)),
            ("vat", "!=", False),
            ("vat", "!=", ""),
        ])

        if not partners_to_check:
            return

        # 2. Filter out partners that already have an open "Invalid VAT" issue.
        existing_issues = self.env["odf.data.quality.issue"].search([
            ("related_record", "in", [f"res.partner,{pid}" for pid in partners_to_check.ids]),
            ("issue_type", "=", "Invalid VAT"),
            ("status", "!=", "resolved"),
        ])
        partners_with_existing_issue_ids = {
            issue.related_record.id for issue in existing_issues if issue.related_record
        }
        partners_to_validate = partners_to_check.filtered(
            lambda p: p.id not in partners_with_existing_issue_ids
        )

        if not partners_to_validate:
            return

        # 3. Perform validation checks.
        invalid_partners = self.env['res.partner']

        # 3a. First, run the VIES check in batches if enabled. This is the most
        # reliable check, but it is remote and requires careful handling.
        if self.env.company.vat_check_vies:
            batch_size = 10  # Process 10 partners per batch
            for i in range(0, len(partners_to_validate), batch_size):
                batch = partners_to_validate[i:i + batch_size]
                try:
                    # The button_vies_check method from base_vat is designed to
                    # be called on a recordset and handles iteration internally.
                    batch.button_vies_check()
                except Exception:
                    # If the VIES service fails, we can't validate this batch.
                    # We could log this, but for now we'll just continue.
                    pass
                time.sleep(1)  # Wait 1 second between batches to be safe.

        # 3b. Now, check the validation status. The `check_vat` method in
        # `base_vat` provides the definitive status, incorporating the
        # result of the VIES check if it was performed.
        partners_to_validate.refresh() # Refresh to get latest status
        for partner in partners_to_validate:
            if not partner.check_vat():
                invalid_partners |= partner

        # 4. Create issues for all invalid partners found.
        issues_to_create = []
        for partner in invalid_partners:
            issues_to_create.append({
                'name': f"Invalid VAT number for '{partner.display_name}'",
                'issue_type': 'Invalid VAT',
                'related_record': f'res.partner,{partner.id}',
            })

        if issues_to_create:
            self.env['odf.data.quality.issue'].create(issues_to_create)
