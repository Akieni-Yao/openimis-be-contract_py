import json
from django.db.models import Q

from django.http import Http404, JsonResponse
from contract.models import Contract, ContractDetails
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
            ).first()
            if contract_details:
                policy_holder = contract.policy_holder
                current_date = str(now.strftime("%d-%m-%Y"))
                date_valid_to = str(contract.date_valid_to.strftime("%d-%m-%Y"))
                total_insuree = contract_details.count()
                total_salary_brut = 0
                part_salariale = 0
                part_patronale = 0
                total_due_pay = contract.get("amountDue", 0)
                user_location = ""
                user_name = ""

                for detail in contract_details:
                    jsonExt = detail.json_ext
                    customField = json.loads(detail.customField)
                    total_salary_brut += jsonExt["calculation_rule"]["income"]
                    part_salariale += customField["salaryShare"]
                    part_patronale += customField["employerContribution"]

                data = {
                    "data": {
                        "id": contract.code,
                        "period": "",
                        "current_date": current_date,
                        "subscriber_name": policy_holder.get("trade_name", ""),
                        "subscriber_camu_number": policy_holder.get("code", ""),
                        "subscriber_addresse": policy_holder.get("address", ""),
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
                report_name = "contract_referrals"
                report_config = ReportConfig.get_report(report_name)
                if not report_config:
                    raise Http404("Report configuration does not exist")
                report_definition = (report_name, report_config["default_report"])
                template_dict = json.loads(report_definition)
                pdf = generate_report(report_name, template_dict, data)
                print("Report generated successfully.")
                return pdf
    except Exception as e:
        print(f"An exception occurred: {str(e)}")
        raise  # Re-raise the exception or handle it according to your requirements
    print("PDF not generated.")
    return None  # Handle the case where no PDF is generated
