# -*- coding: utf-8 -*-

from odoo import models, fields, api
import logging
import traceback

_logger = logging.getLogger(__name__)

try:
    from odoo_data_flow.workflow_runner import run_workflow
except ImportError:
    run_workflow = None
    _logger.warning("The 'odoo-data-flow' library is not installed. Please install it to use ODF Core features.")


class OdfFlowProject(models.Model):
    _name = 'odf.flow.project'
    _description = 'Odoo Data Flow Project'

    name = fields.Char(required=True)
    active = fields.Boolean(default=True)
    source_connection_id = fields.Many2one('odf.connection', required=True)
    destination_connection_id = fields.Many2one('odf.connection', required=True)
    flow_file_path = fields.Char(string="Flow File Path", required=True)
    status = fields.Selection([
        ('new', 'New'),
        ('running', 'Running'),
        ('done', 'Done'),
        ('failed', 'Failed'),
    ], default='new', copy=False, tracking=True)

    @api.model
    def _run_projects(self):
        """
        This method is called by a scheduled action to run all active data flow projects.
        """
        projects = self.search([('active', '=', True), ('status', '!=', 'running')])
        for project in projects:
            project.write({'status': 'running'})
            self.env.cr.commit()

            try:
                if not run_workflow:
                    raise ImportError("The 'odoo-data-flow' library is not available.")

                if not project.flow_file_path:
                    raise ValueError(f"Flow file path is not set for project '{project.name}'")

                # The odoo-data-flow library expects a path to a YAML config file.
                run_workflow(project.flow_file_path)

                project.write({'status': 'done'})
                self.env.cr.commit()

            except Exception:
                _logger.error(
                    "Failed to run data flow project '%s'.\n%s",
                    project.name,
                    traceback.format_exc()
                )
                project.write({'status': 'failed'})
                self.env.cr.commit()
