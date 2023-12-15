import json
from django.http import Http404
from insuree.reports.code_converstion_for_report import convert_activity_data
from policyholder.models import PolicyHolder
from report.apps import ReportConfig
from report.services import get_report_definition, generate_report


def generate_report_for_employee_declaration(code, uuid, contract_approved_date, amount_due):
    try:
        # policyholder = PolicyHolder.objects.get(uuid=uuid)
        # json_ext_data = policyholder.json_ext['jsonExt'] if policyholder.json_ext else {}
        # address_data = policyholder.address['address'] if policyholder.address else {}
        # activity_code = json_ext_data.get('activityCode', '')  # Use .get() to avoid KeyError
        # converted_activity_code = convert_activity_data(activity_code)
        # converted_creation_date = str(contract_approved_date.strftime('%d-%m-%Y'))
        # ad = str(amount_due)
        data = {
            "data": {
                "rib": "",  # TBD
                "contract_number": "",
                "creation_date":  "",
                "camu_code":  "",
                "activity_code": "",
                "niu":  "",
                "address":  "",
                "phone":"",
                "totalinsurees":  "",
                "contribution_sum": "",  # TBD
                "subscriber_contribution": "",  # TBD
                "name": "",  # TBD
                "insured_contribution": "",  # TBD
                "expected_amout": "",
            }
        }
        report_config = ReportConfig.get_report('certificate_declaration_insured')
        if not report_config:
            raise Http404("Report configuration does not exist")
        report_definition = get_report_definition(
            'certificate_declaration_insured', report_config["default_report"]
        )
        template_dict = json.loads(report_definition)
        pdf = generate_report('certificate_declaration_insured', template_dict, data)
        return pdf
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

