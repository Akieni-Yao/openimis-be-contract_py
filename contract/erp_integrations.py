import json
import os
import requests
import logging

from django.http import JsonResponse

from contract.models import Contract, ContractDetails
from payment.models import Payment
from contract.apps import MODULE_NAME
from core.models import ErpApiFailedLogs, ErpOperations
from datetime import datetime

logger = logging.getLogger(__name__)

# erp_url = os.environ.get('ERP_HOST')
erp_url = os.environ.get('ERP_HOST', "https://camu-staging-15480786.dev.odoo.com")

headers = {
    'Content-Type': 'application/json',
    'Tmr-Api-Key': 'test'
}
# headers1 = {
#     'Payment-Type': 'send',
#     'Tmr-Api-Key': 'test',
# }
headers1 = {
    'Payment-Type': 'receive',
    'Tmr-Api-Key': 'test',
}

def erp_submit_contract_mapping_data(customer_id, declaration_date, invoice):
    mapping_dict = {
        "customer_id": customer_id,
        "invoice_date": declaration_date,
        "invoice_lines": invoice
    }
    return mapping_dict

def erp_contract_payment_mapping_data(journel_id, payment_method_lines_id, expected_amount):
    mapping_dict = {
        "journal_id": journel_id,
        "payment_method_line_id": payment_method_lines_id,
        "amount": expected_amount
    }
    return mapping_dict



def filter_null_values(data):
    return {k: v for k, v in data.items() if v is not None}

def erp_submit_contract(id, user):
    logger.debug("====== erp_create_update_contract - start =======")

    contract = Contract.objects.select_related('policy_holder').filter(id=id).first()
    if not contract:
        logger.error("No contract found.")
        return False

    contribution = ContractDetails.objects.select_related('contract', 'contribution_plan_bundle').filter(
        contract__id=contract.id).first()
    if not contribution:
        logger.error("No contribution details found for the contract.")
        return False

    try:
        customer_id = contract.policy_holder.erp_partner_id
        # account_receivable_id = int(contribution.contribution_plan_bundle.account_receivable_id)
        declaration_date = contract.date_valid_from.strftime("%d/%m/%Y")
        product_level = contract.date_valid_from.strftime("%b %Y").upper()
        amount = contract.amount_notified

        erp_operation_contract = ErpOperations.objects.filter(code='CONTRACT').first()

        invoice = [{
            "product_id": erp_operation_contract.erp_id,
            "label": product_level,
            "account_id": erp_operation_contract.accounting_id,
            "quantity": 1,
            "unit_price": amount
        }]

        contract_data = erp_submit_contract_mapping_data(customer_id, declaration_date, invoice)
        contract_data = filter_null_values(contract_data)

        logger.debug(f"======Contract prepared data: {contract_data}")

        if contract.erp_invoice_access_id:
            action = "Update Contract"
            url = f'{erp_url}/update/invoice/{contract.erp_invoice_access_id}'
            logger.debug(f"====== Updating invoice at URL: {url} ======")
        else:
            action = "Create Contract"
            url = f'{erp_url}/create/invoice'
            logger.debug(f"====== Creating invoice at URL: {url} ======")

        response = requests.post(url, headers=headers, json=contract_data, verify=False)
        response_json = response.json()

        if response.status_code == 200:
            logger.debug(f"ERP response: {response_json}")

            if not contract.erp_invoice_access_id:
                Contract.objects.filter(id=id).update(
                    erp_contract_id=response_json.get("id"),
                    erp_invoice_access_id=response_json.get("invoice_access_id")
                )

            post_invoice_url = f'{erp_url}/post/invoice/{response_json.get("invoice_access_id")}'
            logger.debug(f"Posting invoice at URL: {post_invoice_url}")
            post_response = requests.post(post_invoice_url, headers=headers, verify=False)
            post_response_json = post_response.json()

            if response.status_code not in [200, 201]:
                failed_data = {
                    "module": 'contract-post-invoice',
                    "contract": contract,
                    "action": 'post-invoice',
                    "response_status_code": post_response.status_code,
                    "response_json": post_response_json,
                    "request_url": post_invoice_url,
                    "message": post_response.text,
                    "request_data": response_json.get("invoice_access_id"),
                    "resync_status": 0,
                    "created_by": user
                }
                try:
                    ErpApiFailedLogs.objects.create(**failed_data)
                    logger.info("ERP API Failed log saved successfully")
                except Exception as e:
                    logger.error(f"Failed to save ERP API Failed log: {e}")
                logger.error("Failed to post invoice")
                return False
            else:
                logger.info("ERP Contract ==== post invoice succesfully")

            logger.debug(f"Post invoice response: {post_response.json()}")
        else:
            failed_data = {
                "module": MODULE_NAME,
                "contract": contract,
                "action": action,
                "response_status_code": response.status_code,
                "response_json": response_json,
                "request_url": url,
                "message": response.text,
                "request_data": contract_data,
                "resync_status": 0,
                "created_by": user
            }
            try:
                ErpApiFailedLogs.objects.create(**failed_data)
                logger.info("ERP API Failed log saved successfully")
            except Exception as e:
                logger.error(f"Failed to save ERP API Failed log: {e}")
            logger.error(
                f"Failed to submit contract data. Status code: {response.status_code}, Response: {response.text}")
            return False

        try:
            json_data = json.dumps(contract_data)
            logger.debug(f"Contract data JSON: {json_data}")
        except TypeError as e:
            logger.error(f"Error serializing JSON: {e}")

        logger.debug(f"Response status code: {response.status_code}")
        logger.debug(f"Response text: {response.text}")

        logger.debug("Contract data successfully prepared: %s", contract_data)

    except Exception as e:
        logger.error("An error occurred: %s", e)
        return False

    logger.debug("====== erp_create_update_contract - end =======")
    return True


def erp_payment_contract(data, user):
    logger.debug("====== erp_create_update_contract - start =======")

    payment_details = Payment.objects.filter(id=data.id).select_related('contract').first()
    if not payment_details:
        logger.error("No payment details found.")
        return False

    payment_data = {'expected_amount': float(payment_details.expected_amount)}

    journal_id = payment_details.received_amount_transaction[0].get("journauxId", {})
    payment_method_lines_id = payment_details.received_amount_transaction[0].get("payment_method_id", {})

    if not journal_id or not payment_method_lines_id:
        logger.error("Journal ID or Payment Method Lines ID not found.")
        return False

    contract_payment_data = erp_contract_payment_mapping_data(journal_id, payment_method_lines_id,
                                                              payment_data['expected_amount'])
    contract_payment_data = filter_null_values(contract_payment_data)

    logger.debug(f"==========erp_create_update_contract - json prepared Data : {contract_payment_data}========")

    # Assuming that `payment_details.contract` is not a list and has one related contract.
    url = f'{erp_url}/invoice/register-payment/{payment_details.contract.erp_invoice_access_id}'
    logger.debug(f"====== Registering payment at URL: {url} ======")

    response = requests.post(url, headers=headers, json=contract_payment_data, verify=False)
    response_json = response.json()

    if response.status_code not in [200, 201]:
        failed_data = {
            "module": 'contract-payment-register',
            "payment": payment_details,
            "action": "Create contract payment",
            "response_status_code": response.status_code,
            "response_json": response_json,
            "request_url": url,
            "message": response.text,
            "request_data": contract_payment_data,
            "resync_status": 0,
            "created_by": user
        }
        try:
            ErpApiFailedLogs.objects.create(**failed_data)
            logger.info("ERP API Failed log saved successfully")
        except Exception as e:
            logger.error(f"Failed to save ERP API Failed log: {e}")
        logger.error(f"Failed to register payment: {response.status_code} - {response.text}")
        return False

    logger.debug(f"Register payment response: {response_json}")
    logger.debug("====== erp_create_update_contract - end =======")
    return True


def erp_payment_method_line(request, journal_id):
    if journal_id:
        url = f'{erp_url}/get/payment-method/{journal_id}'
        logger.debug(f"====== get_payment_method : url : {url} ======")
        response = requests.get(url, headers=headers1, verify=False)

        if response.status_code == 200:
            return JsonResponse(response.json(), safe=False)

    return JsonResponse({"error": f"Failed to fetch payment method: {response.status_code}", "details": response.text})