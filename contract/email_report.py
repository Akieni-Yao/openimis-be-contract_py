import json
from django.http import Http404

from contract.models import Contract
from contract.views import get_contract_custom_field_data
from contribution_plan.models import ContributionPlanBundleDetails
from insuree.reports.code_converstion_for_report import convert_activity_data
from payment.models import Payment
from policyholder.models import PolicyHolder, PolicyHolderContributionPlan, PolicyHolderInsuree
from report.apps import ReportConfig
from report.services import get_report_definition, generate_report


def generate_report_for_employee_declaration(contract_id, code, uuid, contract_approved_date, amount_due):
    try:
        total_amount = 0
        payment = Payment.objects.filter(contract__id=contract_id).first()
        if payment:
            total_amount = payment.total_amount
        policyholder = PolicyHolder.objects.get(uuid=uuid)
        ph_cpb = PolicyHolderContributionPlan.objects.filter(policy_holder=policyholder, is_deleted=False).first()
        json_ext_data = policyholder.json_ext['jsonExt'] if policyholder.json_ext else {}
        address_data = policyholder.address['address'] if policyholder.address else {}
        activity_code = json_ext_data.get('activityCode', '')
        number_of_insuree = json_ext_data.get('nbEmployees', '')
        converted_activity_code = convert_activity_data(activity_code)
        # converted_creation_date = str(contract_approved_date.strftime('%d-%m-%Y'))
        # ad = str(amount_due)
        cpb = ph_cpb.contribution_plan_bundle
        cpbd = ContributionPlanBundleDetails.objects.filter(contribution_plan_bundle=cpb, is_deleted=False).first()
        conti_plan = cpbd.contribution_plan if cpbd else None
        ercp = 0
        eecp = 0
        phn_json = PolicyHolderInsuree.objects.filter(contribution_plan_bundle__id=cpb.id,
                                                      policy_holder__code=policyholder.code,
                                                      policy_holder__date_valid_to__isnull=True,
                                                      policy_holder__is_deleted=False, date_valid_to__isnull=True,
                                                      is_deleted=False).first()
        ei = 0
        if phn_json and phn_json.json_ext:
            json_data = phn_json.json_ext
            ei = float(json_data.get('calculation_rule', {}).get('income', 0))
        if conti_plan and conti_plan.json_ext:
            json_data = conti_plan.json_ext
            calculation_rule = json_data.get('calculation_rule')
            if calculation_rule:
                ercp = float(calculation_rule.get('employerContribution', 0))
                eecp = float(calculation_rule.get('employeeContribution', 0))
        employer_contribution = round(ei * ercp / 100, 2) if ercp and ei is not None else 0
        salary_share = round(ei * eecp / 100, 2) if eecp and ei is not None else 0
        total = salary_share + employer_contribution
        data = {
            "data": {
                "rib": "",
                "contract_number": str(code) if code else '',
                "creation_date":  '',
                "camu_code": policyholder.code if policyholder.code else '',
                "activity_code": str(converted_activity_code) if converted_activity_code else '',
                "niu": policyholder.json_ext['jsonExt']['niu'] if hasattr(policyholder,
                                                                               'json_ext') and 'jsonExt' in policyholder.json_ext and 'niu' in
                                                                       policyholder.json_ext['jsonExt'] else "" ,
                "address": address_data if address_data else '',
                "phone": str(policyholder.phone) if policyholder.phone else '',
                "totalinsurees": str(number_of_insuree) if number_of_insuree else '',
                "contribution_sum": str(total) if total else '',
                "subscriber_contribution": str(employer_contribution) if employer_contribution else '',
                "name": "",
                "insured_contribution": str(salary_share) if salary_share else '',
                "expected_amout": str(total_amount) if total_amount else '',
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
