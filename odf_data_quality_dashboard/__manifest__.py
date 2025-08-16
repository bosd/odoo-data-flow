# -*- coding: utf-8 -*-
{
    'name': 'ODF Data Quality Dashboard',
    'version': '18.0.1.0.0',
    'summary': 'Data Quality Dashboard for Odoo',
    'author': 'Odoo Community Association (OCA), Odoo Data Flow',
    'website': 'https://github.com/OCA/odoo-data-flow',
    'license': 'AGPL-3',
    'category': 'Tools',
    'depends': ['base'],
    'data': [
        'security/ir.model.access.csv',
        'views/data_quality_issue_views.xml',
        'views/menus.xml',
        'data/scheduled_actions.xml',
    ],
    'installable': True,
}
