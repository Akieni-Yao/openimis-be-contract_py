import io
import json
import logging

import pandas as pd
from celery.result import AsyncResult
from contribution_plan.models import ContributionPlanBundleDetails
from django.db import transaction
from django.http import HttpResponse, JsonResponse
from insuree.models import Insuree
from openIMIS.celery import app
from policyholder.models import (
    PolicyHolder,
    PolicyHolderContributionPlan,
    PolicyHolderInsuree,
)
from rest_framework.decorators import api_view
from rest_framework.response import Response

from contract.models import Contract, ContractDetails
from contract.utils import create_new_insuree_and_add_contract_details, custom_round

logger = logging.getLogger(__name__)

HEADER_INCOME = "income"


def generate_multi_contract_excel_data(contract_detail):
    try:
        contract_detail = get_contract_custom_field_data(contract_detail)
        contract_data = {
            "Assuré": (
                contract_detail["insuree"]["lastName"]
                + " "
                + contract_detail["insuree"]["otherNames"]
                if contract_detail.get("insuree")
                else ""
            ),
            "Numéro CAMU": (
                str(contract_detail["insuree"]["camu_number"])
                if contract_detail.get("insuree")
                and contract_detail["insuree"].get("camu_number")
                else ""
            ),
            "Numéro CAMU temporaire": (
                contract_detail["insuree"]["chfId"]
                if contract_detail.get("insuree")
                and contract_detail["insuree"].get("chfId")
                else ""
            ),
            "Ensemble du plan de contribution": (
                contract_detail["contributionPlanBundle"]["code"]
                + " - "
                + contract_detail["contributionPlanBundle"]["name"]
                if contract_detail.get("contributionPlanBundle")
                and contract_detail["contributionPlanBundle"].get("code")
                and contract_detail["contributionPlanBundle"].get("name")
                else ""
            ),
            "Gross Salary": (
                str(contract_detail["jsonExt"]["calculation_rule"]["income"])
                if contract_detail.get("jsonExt")
                and contract_detail["jsonExt"].get("calculation_rule")
                and contract_detail["jsonExt"]["calculation_rule"].get("income")
                else ""
            ),
            "Cotisation de l'employeur": (
                str(contract_detail["customField"]["employerContribution"])
                if contract_detail.get("customField")
                and contract_detail["customField"].get("employerContribution")
                else ""
            ),
            "Cotisation des employés": (
                str(contract_detail["customField"]["salaryShare"])
                if contract_detail.get("customField")
                and contract_detail["customField"].get("salaryShare")
                else ""
            ),
            "Total": (
                str(contract_detail["customField"]["total"])
                if contract_detail.get("customField")
                and contract_detail["customField"].get("total")
                else ""
            ),
        }
        logger.info(contract_data)

        return contract_data
    except Exception as e:
        return None


def multi_contract(request, contract_id):
    is_confirmed = request.GET.get("is_confirmed", False)

    contract_details = ContractDetails.objects.filter(
        contract_id=contract_id, is_deleted=False
    )
    if is_confirmed:
        contract_details = contract_details.filter(is_confirmed=True)

    all_contract_data = []
    for detail in contract_details:
        # contract_data = get_contract_custom_field_data(detail)
        contract_data = generate_multi_contract_excel_data(detail)
        if contract_data:
            all_contract_data.append(contract_data)
    if not all_contract_data:
        return None
    # Create a DataFrame from all the contract data
    df = pd.DataFrame(all_contract_data)
    response = HttpResponse(
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response["Content-Disposition"] = 'attachment; filename="multiple_contracts.xlsx"'
    # Write DataFrame to response as an Excel file
    df.to_excel(response, index=False, header=True)
    return response


# def resolve_custom_field(detail):
#         try:
#             cpb = detail.contribution_plan_bundle
#             cpbd = ContributionPlanBundleDetails.objects.filter(
#                 contribution_plan_bundle=cpb,
#                 is_deleted=False
#             ).first()
#             conti_plan = cpbd.contribution_plan if cpbd else None
#             ercp = 0
#             eecp = 0
#             if conti_plan and conti_plan.json_ext:
#                 json_data = conti_plan.json_ext
#                 calculation_rule = json_data.get('calculation_rule')
#                 if calculation_rule:
#                     ercp = float(calculation_rule.get(
#                         'employerContribution', 0.0))
#                     eecp = float(calculation_rule.get(
#                         'employeeContribution', 0.0))

#             # Uncommented lines can be used if needed for future logic
#             # insuree = self.insuree
#             # policy_holder = self.contract.policy_holder
#             # phn_json = PolicyHolderInsuree.objects.filter(
#             #     insuree_id=insuree.id,
#             #     policy_holder__code=policy_holder.code,
#             #     policy_holder__date_valid_to__isnull=True,
#             #     policy_holder__is_deleted=False,
#             #     date_valid_to__isnull=True,
#             #     is_deleted=False
#             # ).first()
#             # if phn_json and phn_json.json_ext:
#             #     json_data = phn_json.json_ext
#             #     ei = float(json_data.get('calculation_rule', {}).get('income', 0))
#             self_json = detail.json_ext if detail.json_ext else None
#             ei = 0.0
#             if self_json:
#                 ei = float(
#                     self_json.get('calculation_rule', {}).get('income', 0.0))

#             # Use integer arithmetic to avoid floating-point issues
#             employer_contribution = (ei * ercp / 100) if ercp and ei is not None else 0.0
#             salary_share = (ei * eecp / 100) if eecp and ei is not None else 0.0
#             total = salary_share + employer_contribution

#             response = {
#                 'total': total,
#                 'employerContribution': employer_contribution,
#                 'salaryShare': salary_share,
#             }
#             return response
#         except Exception as e:
#             return None


def get_contract_custom_field_data(detail):
    cpb = detail.contribution_plan_bundle
    cpbd = ContributionPlanBundleDetails.objects.filter(
        contribution_plan_bundle=cpb, is_deleted=False
    ).first()
    conti_plan = cpbd.contribution_plan if cpbd else None
    ercp = 0.0
    eecp = 0.0

    if conti_plan and conti_plan.json_ext:
        json_data = conti_plan.json_ext
        calculation_rule = json_data.get("calculation_rule")
        if calculation_rule:
            ercp = float(calculation_rule.get("employerContribution", 0.0))
            eecp = float(calculation_rule.get("employeeContribution", 0.0))

    insuree = detail.insuree
    policy_holder = detail.contract.policy_holder
    phn_json = PolicyHolderInsuree.objects.filter(
        insuree_id=insuree.id,
        policy_holder__code=policy_holder.code,
        policy_holder__date_valid_to__isnull=True,
        policy_holder__is_deleted=False,
        date_valid_to__isnull=True,
        is_deleted=False,
    ).first()
    ei = 0

    if phn_json and phn_json.json_ext:
        json_data = phn_json.json_ext
        ei = float(json_data.get("calculation_rule", {}).get("income", 0.0))

    employer_contribution = (
        ei * ercp / 100) if ercp and ei is not None else 0.0
    salary_share = (ei * eecp / 100) if eecp and ei is not None else 0.0
    total = salary_share + employer_contribution

    custom_field_data = {
        "total": custom_round(total) if total is not None else 0,
        "employerContribution": (
            custom_round(employer_contribution)
            if employer_contribution is not None
            else 0
        ),
        "salaryShare": custom_round(salary_share) if salary_share is not None else 0,
    }

    contract_data = {
        "id": detail.id,
        "jsonExt": detail.json_ext or "",  # Set empty string if json_ext is None
        "contract": {
            "id": (
                detail.contract.id if detail.contract else ""
            ),  # Set empty string if contract is None
        },
        "insuree": {
            "id": insuree.id if insuree else "",  # Set empty string if insuree is None
            "uuid": insuree.uuid if insuree else "",
            "chfId": insuree.chf_id if insuree else "",
            "lastName": insuree.last_name if insuree else "",
            "otherNames": insuree.other_names if insuree else "",
        },
        "contributionPlanBundle": {
            "id": cpb.id if cpb else "",  # Set empty string if cpb is None
            "code": cpb.code if cpb else "",
            "name": cpb.name if cpb else "",
            "periodicity": cpb.periodicity if cpb else "",
            "dateValidFrom": cpb.date_valid_from if cpb else "",
            "dateValidTo": cpb.date_valid_to if cpb else "",
            "isDeleted": cpb.is_deleted if cpb else "",
            "replacementUuid": cpb.replacement_uuid if cpb else "",
        },
        "customField": custom_field_data,
    }
    return contract_data


def send_contract(contract_id):
    contract_details = ContractDetails.objects.filter(contract_id=contract_id)
    all_contract_data = []
    for detail in contract_details:
        contract_data = generate_multi_contract_excel_data(detail)
        if contract_data:
            all_contract_data.append(contract_data)
    if not all_contract_data:
        return None
    df = pd.DataFrame(all_contract_data)
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False, header=True)
    excel_buffer.seek(0)
    return excel_buffer.getvalue()


@api_view(["POST"])
def update_contract_salaries(request, contract_id):
    try:
        file = request.FILES["file"]
        user = request.user

        # Read file content
        file_content = file.read()

        # Call the async task
        from contract.tasks import update_contract_salaries_async
        task = update_contract_salaries_async.delay(
            str(user.id),
            str(contract_id),
            file_content
        )

        return JsonResponse({
            "success": True,
            "message": "Contract salary update has been queued for processing",
            "task_id": task.id
        })

    except Exception as e:
        logger.error(
            "An unexpected error occurred while queuing the import process: %s",
            str(e)
        )
        return Response({"success": False, "message": str(e)}, status=500)


def re_evaluate_contract_details(contract_id, user, core_username):
    from contract.models import ContractDetails
    from contract.services import Contract as ContractService

    contract_service = ContractService(user=user)
    contract = Contract.objects.filter(id=contract_id).first()

    # if contract.use_bundle_contribution_plan_amount is False:
    #     logger.info("contract.use_bundle_contribution_plan_amount is False")
    #     return

    logger.info(f"evaluate_contract_details : contract = {contract}")

    contract_details = ContractDetails.objects.filter(
        contract_id=contract.id, is_confirmed=True, is_deleted=False
    )

    logger.info(f"contract_details: {contract_details}")

    contract_details_list = {}
    contract_details_list["contract"] = contract
    contract_details_list["data"] = ContractService.gather_policy_holder_insuree_2(
        contract_service,
        list(
            ContractDetails.objects.filter(
                contract_id=contract.id,
                is_confirmed=True,
                is_deleted=False,
            ).values()
        ),
        contract.amendment,
        contract.date_valid_from,
    )

    logger.info(f"contract_details_list: {contract_details_list}")

    contract_contribution_plan_details = contract_service.evaluate_contract_valuation(
        contract_details_result=contract_details_list, save=False
    )

    amount_due = contract_contribution_plan_details["total_amount"]
    logger.info(f"===> on_contract_approve_signal : amount_due = {amount_due}")
    if isinstance(amount_due, str):
        amount_due = float(amount_due)
    rounded_amount = round(amount_due)
    contract.amount_notified = rounded_amount
    contract.save(username=core_username)

    if contract.use_bundle_contribution_plan_amount and rounded_amount > 0:
        logger.info(
            "====> contract.use_bundle_contribution_plan_amount is True")
        update_forfait_rule(contract.id, rounded_amount, core_username)


def update_forfait_rule(contract_id, rounded_amount, core_username):
    from contract.models import ContractDetails

    print(f"====> contract_id: {contract_id}")

    contract_details_to_update = ContractDetails.objects.filter(
        contract_id=contract_id, is_confirmed=True, is_deleted=False
    )

    if not contract_details_to_update:
        logger.info("=====> No contract details to update")
        return

    forfait_total = 0

    total_contract_detail = len(contract_details_to_update)

    forfait_total = rounded_amount / total_contract_detail

    print(f"====> forfait_total: {forfait_total}")

    for contract_detail in contract_details_to_update:
        try:
            print(f"====> contract_detail: {contract_detail}")
            contract_detail.json_ext = {
                "calculation_rule": {"rate": 0, "income": 0},
                "forfait_rule": {
                    "total": round(forfait_total),
                    "employerContribution": 0,
                    "salaryShare": 0,
                },
            }
            contract_detail.save(username=core_username)
        except Exception as e:
            logger.error(f"====> Error updating contract detail: {e}")

    logger.info(
        f"====> contract_details_to_update: {contract_details_to_update}")


def update_salary(parsed_json, new_income):
    try:
        logger.debug("Received JSON data: %s", parsed_json)
        logger.debug("New income value: %d", new_income)

        if (
            "calculation_rule" in parsed_json
            and "income" in parsed_json["calculation_rule"]
        ):
            logger.info(
                "Updating income value from %d to %d",
                parsed_json["calculation_rule"]["income"],
                new_income,
            )
            parsed_json["calculation_rule"]["income"] = new_income
        else:
            if "calculation_rule" not in parsed_json:
                parsed_json["calculation_rule"] = {}
            logger.info("Adding new income value: %d", new_income)
            parsed_json["calculation_rule"]["income"] = new_income

        logger.debug("Updated JSON data: %s", parsed_json)

        return parsed_json

    except json.JSONDecodeError as e:
        logger.error("JSON decoding failed: %s", e)
        return None
    except Exception as e:
        logger.error("An unexpected error occurred: %s", e)
        return None


@api_view(["GET"])
def task_status(request, task_id):
    result = AsyncResult(task_id, app=app)
    data = {
        "task_id": task_id,
        "status": result.status,
        "ready": result.ready(),
        "successful": result.successful(),
    }
    return JsonResponse(data)
