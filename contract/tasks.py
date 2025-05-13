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
