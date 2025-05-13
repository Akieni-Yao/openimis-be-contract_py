import logging

from celery import shared_task
from core.constants import CONTRACT_CREATION_NT
from core.models import User
from core.notification_service import create_camu_notification
from rest_framework.response import Response

from contract.models import Contract, ContractContributionPlanDetails, ContractMutation
from contract.services import Contract as ContractService
from contract.services import ContractToInvoiceService

logger = logging.getLogger(__name__)


@shared_task
def approve_contracts(user_id, contracts):
    user = User.objects.get(id=user_id)
    contract_service = ContractService(user=user)
    for contract in contracts:
        output = contract_service.approve(contract={"id": contract})


@shared_task
def counter_contracts(user_id, contracts):
    user = User.objects.get(id=user_id)
    contract_service = ContractService(user=user)
    for contract in contracts:
        output = contract_service.counter(contract={"id": contract})


@shared_task
def create_invoice_from_contracts(user_id, contracts):
    user = User.objects.get(id=user_id)
    contract_service = ContractToInvoiceService(user=user)
    for contract in contracts:
        contract_instance = Contract.objects.filter(id=contract)
        if contract_instance:
            contract_instance = contract_instance.first()
            ccpd_list = ContractContributionPlanDetails.objects.filter(
                contract_details__contract=contract_instance)
            output = contract_service.create_invoice(
                instance=contract_instance,
                convert_to="InvoiceLine",
                user=user,
                ccpd_list=ccpd_list
            )


@shared_task
def create_contract_async(user_id, contract_data, client_mutation_id=None):
    """
    Asynchronous task to create a contract and handle related operations
    """
    try:
        user = User.objects.get(id=user_id)
        contract_service = ContractService(user=user)

        # Create the contract
        output = contract_service.create(contract=contract_data)

        if output["success"]:
            contract = Contract.objects.get(id=output["data"]["id"])
            Contract.objects.filter(id=contract.id).update(
                process_status=Contract.ProcessStatus.CREATED
            )

            # Send notification
            try:
                create_camu_notification(CONTRACT_CREATION_NT, contract)
                logger.info("Sent Notification.")
            except Exception as e:
                logger.error(f"Failed to send notification: {e}")

            # Create contract mutation
            if client_mutation_id:
                ContractMutation.object_mutated(
                    user, client_mutation_id=client_mutation_id, contract=contract
                )

            return Response({
                "success": True,
                "message": "Contract created successfully"
            })
        else:
            return Response({
                "success": False,
                "message": output.get("message", "Unknown error"),
            })

    except Exception as e:
        logger.error(f"Error in create_contract_async: {str(e)}")
        return Response({
            "success": False,
            "message": "Error creating contract",
        })


@shared_task
def update_contract_salaries_async(user_id, contract_id, file_data):
    """
    Asynchronous task to update contract salaries from an Excel file
    """
    try:
        import io
        import logging

        import pandas as pd
        from core.models import User
        from django.core.files.base import ContentFile
        from django.db import transaction
        from insuree.models import Insuree
        from policyholder.gql.gql_mutations.create_mutations import (
            get_and_set_waiting_period_for_insuree,
        )
        from policyholder.models import PolicyHolder, PolicyHolderContributionPlan

        from contract.models import Contract, ContractDetails
        from contract.views import (
            create_new_insuree_and_add_contract_details,
            re_evaluate_contract_details,
            update_salary,
        )

        logger = logging.getLogger(__name__)
        # Get user
        user = User.objects.get(id=user_id)
        core_username = user.username
        user_id_for_audit = user.id_for_audit

        # Initialize counters
        total_lines = 0
        total_salaries_updated = 0
        total_validation_errors = 0
        print(f"{'*'*30}")

        # Get contract and policy holder
        contract = Contract.objects.filter(id=contract_id).first()
        policy_holder = PolicyHolder.objects.filter(
            code=contract.policy_holder.code
        ).first()

        if not policy_holder:
            return {
                "success": False,
                "message": "Policy holder not found"
            }

        # Get contribution plan bundle
        ph_cpb = PolicyHolderContributionPlan.objects.filter(
            policy_holder=policy_holder, is_deleted=False
        ).first()
        cpb = ph_cpb.contribution_plan_bundle if ph_cpb else None
        enrolment_type = cpb.name if cpb else None

        # Convert file data to pandas DataFrame
        file_content = ContentFile(file_data)
        df = pd.read_excel(file_content)
        df.columns = [col.strip() for col in df.columns]

        # Output data preparation
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")
        processed_data = []

        # Start transaction
        with transaction.atomic():
            # Get existing contract details
            exist_contract_details = ContractDetails.objects.filter(
                contract_id=contract_id, is_deleted=False
            )

            # Index contract details by chf_id
            contract_details_by_chf_id = {
                detail.insuree.chf_id: detail for detail in
                exist_contract_details
            }

            confirmed_insurees = []
            already_processed_chf_ids = []

            # Process each row
            for index, line in df.iterrows():
                total_lines += 1
                chf_id = line.get("Numéro CAMU temporaire")

                if not chf_id or pd.isna(chf_id):
                    insuree_name = line.get("Assuré")
                    if not insuree_name or pd.isna(insuree_name):
                        continue

                    chf_id = create_new_insuree_and_add_contract_details(
                        insuree_name,
                        policy_holder,
                        cpb,
                        contract,
                        user_id_for_audit,
                        None,  # No request object in async task
                        enrolment_type,
                    )
                    if not chf_id:
                        continue
                    print(f"{'2*'*30}")

                    # Refresh contract details after new insuree creation
                    exist_contract_details = ContractDetails.objects.filter(
                        contract_id=contract_id, is_deleted=False
                    )
                    contract_details_by_chf_id = {
                        detail.insuree.chf_id: detail
                        for detail in exist_contract_details
                    }
                if chf_id in already_processed_chf_ids:
                    continue

                already_processed_chf_ids.append(chf_id)
                gross_salary = line.get("Gross Salary")
                new_gross_salary = 0

                if contract.use_bundle_contribution_plan_amount is False:
                    if not gross_salary or pd.isna(gross_salary):
                        continue
                    new_gross_salary = int(gross_salary)
                    if new_gross_salary <= 0:
                        continue

                if chf_id in contract_details_by_chf_id:
                    contract_detail = contract_details_by_chf_id[chf_id]
                    insuree = Insuree.objects.filter(chf_id=chf_id).first()

                    if insuree:
                        get_and_set_waiting_period_for_insuree(
                            insuree.id, policy_holder.id
                        )
                    current_salary = (
                        int(
                            contract_detail
                            .json_ext.get("calculation_rule", {})
                            .get("income", 0)
                        )
                        if contract_detail.json_ext
                        else 0
                    )

                    if current_salary == 0:
                        contract_detail.json_ext = {
                            "calculation_rule": {"rate": 0, "income": new_gross_salary}
                        }

                    confirmed_insurees.append(insuree)
                    if current_salary != new_gross_salary:
                        json_data = update_salary(
                            contract_detail.json_ext, new_gross_salary
                        )
                        if json_data is None:
                            raise ValueError(
                                f"Failed to update salary for chf_id {chf_id}"
                            )

                        contract_detail.json_ext = json_data
                        contract_detail.save(username=core_username)
                        total_salaries_updated += 1
                        status = "Success"
                    else:
                        status = "No Change"
                else:
                    total_validation_errors += 1
                    status = "Error: Not Found"

                line_data = line.to_dict()
                line_data["Status"] = status
                processed_data.append(line_data)

            # Update confirmation status for contract details
            contract_details = ContractDetails.objects.filter(
                contract_id=contract_id,
                is_deleted=False,
                insuree__in=confirmed_insurees,
            )

            for contract_detail in contract_details:
                if contract_detail.is_confirmed is False:
                    contract_detail.is_confirmed = True
                    contract_detail.save(username=core_username)

            contract_details_excludes = ContractDetails.objects.filter(
                contract_id=contract_id,
                is_deleted=False,
            ).exclude(insuree__in=confirmed_insurees)

            for contract_detail_exclude in contract_details_excludes:
                if contract_detail_exclude.is_confirmed is True:
                    contract_detail_exclude.is_confirmed = False
                    contract_detail_exclude.save(username=core_username)

            # Create output Excel file
            processed_df = pd.DataFrame(processed_data)
            processed_df.to_excel(writer, index=False, header=True)
            writer.save()
            output.seek(0)

        # Evaluate contract details if no errors
        if total_validation_errors == 0:
            re_evaluate_contract_details(contract_id, user, core_username)
            return {
                "success": True,
                "message": None
            }
        else:
            error_message = f"{total_validation_errors} entries had errors."
            return {
                "success": False,
                "message": error_message
            }

    except Exception as e:
        logger.error(
            "An unexpected error occurred during the import process: %s", str(
                e)
        )
        return {
            "success": False,
            "message": str(e)
        }
