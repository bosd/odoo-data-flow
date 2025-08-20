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
        Check for partners with invalid VAT numbers using a performant,
        batch-oriented approach.
        """
        from datetime import timedelta
        yesterday = fields.Datetime.now() - timedelta(days=1)

        # 1. Fetch all partners modified recently that have a VAT number.
        partners_to_check = self.env['res.partner'].search([
            ('write_date', '>=', fields.Datetime.to_string(yesterday)),
            ('vat', '!=', False),
            ('vat', '!=', ''),
        ])

        if not partners_to_check:
            return

        # 2. Fetch all existing, unresolved "Invalid VAT" issues for the partners.
        existing_issues = self.env['odf.data.quality.issue'].search([
            ('related_record', 'in', [f'res.partner,{pid}' for pid in partners_to_check.ids]),
            ('issue_type', '=', 'Invalid VAT'),
            ('status', '!=', 'resolved'),
        ])
        partners_with_existing_issue = set(
            issue.related_record.id for issue in existing_issues if issue.related_record
        )

        # 3. Process in memory to find partners with invalid VATs that need a new issue.
        issues_to_create = []
        for partner in partners_to_check:
            # Skip if partner already has an open issue.
            if partner.id in partners_with_existing_issue:
                continue

            # Simple validation: VAT must have at least 3 characters.
            if len(partner.vat) < 3:
                issues_to_create.append({
                    'name': f"Invalid VAT number for '{partner.display_name}'",
                    'issue_type': 'Invalid VAT',
                    'related_record': f'res.partner,{partner.id}',
                })

        # 4. Create all new issues in a single batch create call.
        if issues_to_create:
            self.env['odf.data.quality.issue'].create(issues_to_create)
