
# encoding = utf-8
# Always put this line at the beginning of this file
import ta_splunkadx_alert_declare

import os
import sys

from alert_actions_base import ModularAlertBase
import modalert_send_to_adx_helper

class AlertActionWorkersend_to_adx(ModularAlertBase):

    def __init__(self, ta_name, alert_name):
        super(AlertActionWorkersend_to_adx, self).__init__(ta_name, alert_name)

    def validate_params(self):

        if not self.get_param("cluster_url"):
            self.log_error('cluster_url is a mandatory parameter, but its value is None.')
            return False

        if not self.get_param("app_id"):
            self.log_error('app_id is a mandatory parameter, but its value is None.')
            return False

        if not self.get_param("app_secret"):
            self.log_error('app_secret is a mandatory parameter, but its value is None.')
            return False

        if not self.get_param("tenant_id"):
            self.log_error('tenant_id is a mandatory parameter, but its value is None.')
            return False

        if not self.get_param("database_name"):
            self.log_error('database_name is a mandatory parameter, but its value is None.')
            return False

        if not self.get_param("table_name"):
            self.log_error('table_name is a mandatory parameter, but its value is None.')
            return False
        return True

    def process_event(self, *args, **kwargs):
        status = 0
        try:
            if not self.validate_params():
                return 3
            status = modalert_send_to_adx_helper.process_event(self, *args, **kwargs)
        except (AttributeError, TypeError) as ae:
            self.log_error("Error: {}. Please double check spelling and also verify that a compatible version of Splunk_SA_CIM is installed.".format(str(ae)))
            return 4
        except Exception as e:
            msg = "Unexpected error: {}."
            if e:
                self.log_error(msg.format(str(e)))
            else:
                import traceback
                self.log_error(msg.format(traceback.format_exc()))
            return 5
        return status

if __name__ == "__main__":
    exitcode = AlertActionWorkersend_to_adx("TA-splunkadx-alert", "send_to_adx").run(sys.argv)
    sys.exit(exitcode)
