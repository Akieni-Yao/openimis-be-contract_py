import json
from django.db.models import Q

from django.http import Http404, JsonResponse
from contract.models import Contract, ContractDetails
from contract.views import get_contract_custom_field_data
from report.apps import ReportConfig
from report.services import get_report_definition, generate_report


def filter_amount_contract(arg="amount_from", arg2="amount_to", **kwargs):
    amount_from = kwargs.get(arg)
    amount_to = kwargs.get(arg2)

    status_notified = [1, 2]
    status_rectified = [4, 11, 3]
    status_due = [5, 6, 7, 8, 9, 10]

    # scenario - only amount_to set
    if not amount_from and amount_to:
        return (
            Q(amount_notified__lte=amount_to, state__in=status_notified)
            | Q(amount_rectified__lte=amount_to, state__in=status_rectified)
            | Q(amount_due__lte=amount_to, state__in=status_due)
        )

    # scenario - only amount_from set
    if amount_from and not amount_to:
        return (
            Q(amount_notified__gte=amount_from, state__in=status_notified)
            | Q(amount_rectified__gte=amount_from, state__in=status_rectified)
            | Q(amount_due__gte=amount_from, state__in=status_due)
        )

    # scenario - both filters set
    if amount_from and amount_to:
        return (
            Q(
                amount_notified__gte=amount_from,
                amount_notified__lte=amount_to,
                state__in=status_notified,
            )
            | Q(
                amount_rectified__gte=amount_from,
                amount_rectified__lte=amount_to,
                state__in=status_rectified,
            )
            | Q(
                amount_due__gte=amount_from,
                amount_due__lte=amount_to,
                state__in=status_due,
            )
        )


def generate_report_for_contract_receipt(contract_id):
    from core import datetime

    now = datetime.datetime.now()
    try:
        contract = Contract.objects.filter(id=contract_id, is_deleted=False).first()
        if contract:
            contract_details = ContractDetails.objects.filter(
                contract_id=contract_id, is_deleted=False
            )
            if contract_details:
                policy_holder = contract.policy_holder
                current_date = str(now.strftime("%d-%m-%Y"))
                date_valid_to = str(contract.date_valid_to.strftime("%d-%m-%Y"))
                total_insuree = contract_details.count()
                total_salary_brut = 0
                part_salariale = 0
                part_patronale = 0
                total_due_pay = (
                    contract.amount_due if contract.amount_due is not None else 0
                )
                user_location = ""
                user_name = ""

                for detail in contract_details:
                    jsonExt = detail.json_ext
                    customField = get_contract_custom_field_data(detail)
                    print(
                        f"=========================================== customField {customField['customField']}"
                    )
                    total_salary_brut += jsonExt["calculation_rule"]["income"]
                    part_salariale += customField['customField']["salaryShare"]
                    part_patronale += customField['customField']["employerContribution"]

                data = {
                    "data": {
                        "id": contract.code,
                        "period": "",
                        "current_date": current_date,
                        "subscriber_name": (
                            policy_holder.trade_name
                            if policy_holder.trade_name is not None
                            else ""
                        ),
                        "subscriber_camu_number": (
                            policy_holder.code if policy_holder.code is not None else ""
                        ),
                        "subscriber_addresse": (
                            policy_holder.address
                            if policy_holder.address is not None
                            else ""
                        ),
                        "date_valid_to": date_valid_to,
                        "total_insuree": total_insuree,
                        "total_salary_brut": total_salary_brut,
                        "part_salariale": part_salariale,
                        "part_patronale": part_patronale,
                        "total_due_pay": total_due_pay,
                        "user_location": user_location,
                        "user_name": user_name,
                    }
                }
                print(f"=========================================== data {data}")
                report_name = "contract_referrals"
                report_config = ReportConfig.get_report(report_name)
                print("=========================================== report_config ")
                if not report_config:
                    raise Http404("Report configuration does not exist")
                report_definition = (report_name, report_config["default_report"])
                print("=========================================== report_definition")
                template_dict = json.loads(report_definition)
                print("=========================================== template_dict")
                pdf = generate_report(report_name, template_dict, data)
                print(f"=========================================== pdf {pdf}")
                print("Report generated successfully.")
                return pdf
    except Exception as e:
        print(f"An exception occurred: {str(e)}")
        raise  # Re-raise the exception or handle it according to your requirements
    print("PDF not generated.")
    return None  # Handle the case where no PDF is generated
