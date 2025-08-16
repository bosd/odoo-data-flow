# -*- coding: utf-8 -*-
from odoo import api, fields, models
from odoo.addons.base.models.res_partner import Partner


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
        self._check_partner_vat()

    @api.model
    def _check_partner_vat(self):
        """Check for partners with invalid VAT."""
        from datetime import timedelta
        yesterday = fields.Datetime.now() - timedelta(days=1)
        partners_to_check = self.env['res.partner'].search([
            ('write_date', '>=', fields.Datetime.to_string(yesterday)),
            ('vat', '!=', False),
            ('vat', '!=', ''),
        ])
        for partner in partners_to_check:
            # This is a placeholder for a real VAT validation.
            # For this example, we consider a VAT invalid if it has less than 3 chars.
            if len(partner.vat) < 3:
                # Avoid creating duplicate issues for the same partner.
                existing_issue = self.search_count([
                    ('related_record', '=', f'res.partner,{partner.id}'),
                    ('issue_type', '=', 'Invalid VAT'),
                    ('status', '!=', 'resolved'),
                ])
                if not existing_issue:
                    self.create({
                        'name': f"Invalid VAT number for '{partner.display_name}'",
                        'issue_type': 'Invalid VAT',
                        'related_record': f'res.partner,{partner.id}',
                    })
