{
    'name': 'Odoo Data Flow Core',
    'version': '18.0.1.0.0',
    'author': 'Odoo Data Flow',
    'depends': ['base'],
    'data': [
        'security/ir.model.access.csv',
        'views/odf_connection_views.xml',
        'views/odf_flow_project_views.xml',
        'views/menus.xml',
        'data/scheduled_actions.xml',
    ],
    'installable': True,
    'application': True,
}
