import io
import json
import logging

import pandas as pd
from contract.models import Contract, ContractDetails
from contract.utils import create_new_insuree_and_add_contract_details, custom_round
from contribution_plan.models import ContributionPlanBundleDetails
from django.db import transaction
from django.http import HttpResponse, JsonResponse
from insuree.models import Insuree
from policyholder.models import (
    PolicyHolder,
    PolicyHolderContributionPlan,
    PolicyHolderInsuree,
)
from rest_framework.decorators import api_view
from rest_framework.response import Response

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

    employer_contribution = (ei * ercp / 100) if ercp and ei is not None else 0.0
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
    from policyholder.gql.gql_mutations.create_mutations import (
        get_and_set_waiting_period_for_insuree,
    )

    file = request.FILES["file"]
    user_id = request.user.id_for_audit
    core_username = request.user.username
    total_lines = 0
    total_salaries_updated = 0
    total_validation_errors = 0
    contract = Contract.objects.filter(id=contract_id).first()
    policy_holder = PolicyHolder.objects.filter(
        code=contract.policy_holder.code
    ).first()
    ph_cpb = None
    cpb = None
    enrolment_type = None

    logger.info("======================================= update contract salaries")

    if not policy_holder:
        return Response(
            {"success": False, "message": "Policy holder not found"}, status=400
        )

    ph_cpb = PolicyHolderContributionPlan.objects.filter(
        policy_holder=policy_holder, is_deleted=False
    ).first()

    if ph_cpb:
        cpb = ph_cpb.contribution_plan_bundle

    if cpb:
        enrolment_type = cpb.name

    try:
        logger.debug("Reading the uploaded Excel file")
        df = pd.read_excel(file)
        df.columns = [col.strip() for col in df.columns]
        logger.debug("Excel file read successfully with columns: %s", df.columns)

        errors = []
        logger.debug("Starting import process for %s lines", len(df))

        # Output data preparation
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")
        processed_data = []

        # Start a transaction
        with transaction.atomic():
            logger.debug("Transaction started for updating contract salaries")

            # Fetch existing contract details for the contract_id
            exist_contract_details = ContractDetails.objects.filter(
                contract_id=contract_id, is_deleted=False
            )
            logger.debug(
                "Fetched %s existing contract details", len(exist_contract_details)
            )

            # Index existing contract details by chf_id for quick lookup
            contract_details_by_chf_id = {
                detail.insuree.chf_id: detail for detail in exist_contract_details
            }
            logger.debug("Indexed existing contract details by chf_id")

            confirmed_insurees = []

            # remove duplicates
            df = df.drop_duplicates(subset=["Numéro CAMU temporaire"])

            # Iterate over each row in the Excel file
            for index, line in df.iterrows():
                total_lines += 1
                logger.debug("Processing line %s: %s", total_lines, line)

                # Extract the chf_id and new salary
                chf_id = line.get("Numéro CAMU temporaire")

                logger.info(
                    "======================================= chf_id: %s", chf_id
                )

                if not chf_id or pd.isna(chf_id):
                    insuree_name = line.get("Assuré")
                    if not insuree_name or pd.isna(insuree_name):
                        continue
                    logger.info(
                        "======================================= insuree_name: %s",
                        insuree_name,
                    )

                    chf_id = create_new_insuree_and_add_contract_details(
                        insuree_name,
                        policy_holder,
                        cpb,
                        contract,
                        user_id,
                        request,
                        enrolment_type,
                    )
                    if not chf_id:
                        continue

                    exist_contract_details = ContractDetails.objects.filter(
                        contract_id=contract_id, is_deleted=False
                    )
                    logger.debug(
                        "Fetched %s existing contract details",
                        len(exist_contract_details),
                    )

                    # Index existing contract details by chf_id for quick lookup
                    contract_details_by_chf_id = {
                        detail.insuree.chf_id: detail
                        for detail in exist_contract_details
                    }

                gross_salary = line.get("Gross Salary")
                if not gross_salary or pd.isna(gross_salary):
                    continue
                new_gross_salary = int(gross_salary)

                if new_gross_salary <= 0:
                    continue

                logger.debug(
                    "Extracted chf_id: %s and new_gross_salary: %s",
                    chf_id,
                    new_gross_salary,
                )

                if chf_id in contract_details_by_chf_id:
                    contract_detail = contract_details_by_chf_id[chf_id]
                    logger.info(
                        f"---------------------------contract_detail: {contract_detail.json_ext}"
                    )

                    logger.info("Setting waiting period for insuree")

                    insuree = Insuree.objects.filter(chf_id=chf_id).first()

                    if insuree:
                        get_and_set_waiting_period_for_insuree(
                            insuree.id, policy_holder.id
                        )

                    current_salary = (
                        int(
                            contract_detail.json_ext.get("calculation_rule", {}).get(
                                "income", 0
                            )
                        )
                        if contract_detail.json_ext
                        else 0
                    )

                    logger.info(
                        f"---------------------------current_salary: {current_salary}"
                    )
                    logger.info(
                        f"---------------------------new_gross_salary: {new_gross_salary}"
                    )

                    logger.debug(
                        "Current salary for chf_id %s is %s", chf_id, current_salary
                    )

                    if current_salary == 0:
                        # json_ext: {'calculation_rule': {'rate': 0, 'income': '69000'}}
                        contract_detail.json_ext = {
                            "calculation_rule": {"rate": 0, "income": new_gross_salary}
                        }

                    # Check if the salary has changed
                    confirmed_insurees.append(insuree)
                    if current_salary != new_gross_salary:
                        logger.debug("Updating salary for chf_id %s", chf_id)
                        logger.info(
                            f"---------------------------contract_detail.json_ext: {contract_detail.json_ext}"
                        )
                        json_data = update_salary(
                            contract_detail.json_ext, new_gross_salary
                        )
                        logger.info(
                            f"---------------------------json_data: {json_data}"
                        )
                        if json_data is None:
                            raise ValueError(
                                f"Failed to update salary for chf_id {chf_id}"
                            )

                        contract_detail.json_ext = json_data
                        contract_detail.save(username=core_username)

                        total_salaries_updated += 1
                        status = "Success"
                        logger.info("Successfully updated salary for chf_id %s", chf_id)
                    else:
                        status = "No Change"
                        logger.info("No change in salary for chf_id %s", chf_id)
                else:
                    total_validation_errors += 1
                    status = "Error: Not Found"
                    logger.warning("No contract detail found for chf_id %s", chf_id)

                # Append the current line data with status to processed_data for output
                line_data = line.to_dict()
                line_data["Status"] = status
                processed_data.append(line_data)

            # Update the is_confirmed field for the confirmed_insurees
            logger.info("confirmed_insurees: %s", confirmed_insurees)

            contract_details = ContractDetails.objects.filter(
                contract_id=contract_id,
                is_deleted=False,
                insuree__in=confirmed_insurees,
            )

            for contract_detail in contract_details:
                if contract_detail.is_confirmed is False:
                    contract_detail.is_confirmed = True
                    contract_detail.save(username=core_username)

            logger.info("confirmed_insurees: %s", confirmed_insurees)

            contract_details_exludes = ContractDetails.objects.filter(
                contract_id=contract_id,
                is_deleted=False,
            ).exclude(insuree__in=confirmed_insurees)

            for contract_details_exlude in contract_details_exludes:
                if contract_details_exlude.is_confirmed is True:
                    contract_details_exlude.is_confirmed = False
                    contract_details_exlude.save(username=core_username)

            # Update the is_confirmed field for the confirmed_insurees
            logger.info(
                "turn is_confirmed to false for the insurees not in confirmed_insurees"
            )

            # Create DataFrame for processed data with status
            processed_df = pd.DataFrame(processed_data)

            # Write processed data with status to output Excel file
            logger.debug("Writing processed data to output Excel file")
            processed_df.to_excel(writer, index=False, header=True)
            writer.save()
            output.seek(0)
            logger.debug("Output Excel file created successfully")

        # If there are no errors, return success
        if total_validation_errors == 0:
            logger.info(
                "Import process completed successfully with %s lines processed, %s salaries updated",
                total_lines,
                total_salaries_updated,
            )
            return JsonResponse({"success": True, "message": None})
        else:
            # Construct error message
            error_message = f"{total_validation_errors} entries had errors."
            logger.warning("Import process completed with errors: %s", error_message)
            return Response({"success": False, "message": error_message}, status=400)

    except Exception as e:
        logger.error(
            "An unexpected error occurred during the import process: %s", str(e)
        )
        return Response({"success": False, "error": str(e)}, status=500)


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
