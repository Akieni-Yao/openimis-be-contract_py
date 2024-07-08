import json
import requests
import logging
from contract.models import Contract, ContractDetails
from payment.models import Payment
from decimal import Decimal


logger = logging.getLogger(__name__)

# erp_url = os.environ.get('ERP_HOST')
erp_url = "https://camu-staging-13483170.dev.odoo.com"

headers = {
    'Content-Type': 'application/json',
    'Tmr-Api-Key': 'test'
}


def erp_submit_contract_mapping_data(customer_id, declaration_date, invoice):
    mapping_dict = {
        "customer_id": customer_id,
        "invoice_date": declaration_date,
        "invoice_lines" : invoice
    }
    return mapping_dict

def erp_contract_payment_mapping_data(type_of_payment, received_amount):
    mapping_dict = {
        "journal_id": None,
        "payment_method_line_id": type_of_payment,
        "amount": received_amount
    }
    return mapping_dict



def filter_null_values(data):
    return {k: v for k, v in data.items() if v is not None}

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)  # or str(obj) if you prefer
        return super(DecimalEncoder, self).default(obj)


def erp_submit_contract(id):
    logger.debug(" ======    erp_create_update_contract - start    =======")

    contracts = Contract.objects.select_related('policy_holder').filter(id=id).first()

    if not contracts:
        logger.error("No contract found.")
        return

    contribution = ContractDetails.objects.select_related('contract', 'contribution_plan_bundle').filter(
        contract__id=contracts.id).first()

    if not contribution:
        logger.error("No contribution details found for the contract.")
        return

    try:
        customer_id = contracts.policy_holder.id
        account_receivable_id = contribution.contribution_plan_bundle.account_receivable_id
        declaration_date = contracts.date_valid_from.strftime("%d/%m/%Y")
        product_level = contracts.date_valid_from.strftime("%b %Y").upper()
        amount = contracts.amount_notified

        invoice = []
        products = {
            "product_id": 3951,
            "label": product_level,
            "account_id": account_receivable_id,
            "quantity": 1,
            "unit_price": amount
        }
        invoice.append(products)

        contract_data = erp_submit_contract_mapping_data(customer_id, declaration_date, invoice)
        contract_data = filter_null_values(contract_data)
        logger.debug(" ======    erp_submit_contract - create    =======")
        url = '{}/create/invoice'.format(erp_url)
        logger.debug(f" ======    erp_submit_contract : url : {url}    =======")

        logger.debug(f" ======    erp_submit_contract : contract_data : {contract_data}    =======")

        try:
            json_data = json.dumps(contract_data)
            logger.debug(f" ======    erp_submit_contract : json_data : {json_data}    =======")
        except TypeError as e:
            logger.error(f"Error serializing JSON: {e}")

        response = requests.post(url, headers=headers, json=contract_data, verify=False)
        logger.debug(
            f" ======    erp_submit_contract : response.status_code : {response.status_code}    =======")
        logger.debug(f" ======    erp_submit_contract : response.text : {response.text}    =======")

        logger.debug("Contract data successfully prepared: %s", contract_data)

        try:
            response_json = response.json()
            logger.debug(f" ======    erp_submit_contract : response.json : {response_json}    =======")

            # Update the Contract with the IDs from the response
            Contract.objects.filter(id=id).update(
                erp_contract_id=response.get("id"), erp_invoice_access_id=response.get("invoice_access_id"))

        except json.JSONDecodeError:
            logger.error("Failed to decode JSON response")


    except Exception as e:
        logger.error("An error occurred: %s", e)

    logger.debug(" ======    erp_submit_contract - end    =======")
    return True


def erp_contract_payment(id):
    logger.debug("====== erp_contract_payment - start ======")

    try:
        payment_details = Payment.objects.filter(contract__id=id).select_related('contract')

        if not payment_details.exists():
            logger.warning(f"No payment details found for contract ID {id}")
            return

        for payment in payment_details:
            received_amount = payment.received_amount
            type_of_payment = payment.type_of_payment

            logger.debug(
                f"Processing payment: ID {payment.id}, Received Amount: {received_amount}, Type: {type_of_payment}")

            # Call the mapping function
            contract_payment_data = erp_contract_payment_mapping_data(type_of_payment, received_amount)
            contract_payment_data = filter_null_values(contract_payment_data)

            # Set the invoice access ID for the payment
            payment.invoice_access_id = "36dffa17-bda4-43b5-bfd5-df11cae5d3d3"

            if payment.invoice_access_id:
                logger.debug("====== erp_contract_payment - update ======")
                url = '{}/invoice/register-payment/{}'.format(erp_url, payment.invoice_access_id)
                logger.debug(f"====== erp_contract_payment : url : {url} ======")

            logger.debug(f"====== erp_contract_payment : contract_data : {contract_payment_data} ======")

            try:
                json_data = json.dumps(contract_payment_data, cls=DecimalEncoder)
                logger.debug(f"====== erp_submit_contract : json_data : {json_data} ======")
            except TypeError as e:
                logger.error(f"Error serializing JSON: {e}")
                continue  # Skip this payment and move to the next

            try:
                response = requests.post(url, headers=headers, json=contract_payment_data, verify=False)
                logger.debug(
                    f"====== erp_contract_payment : response.status_code : {response.status_code} ======")
                logger.debug(f"====== erp_contract_payment : response.text : {response.text} ======")
            except requests.RequestException as e:
                logger.error(f"Error making HTTP request: {e}")

            logger.debug("Contract data successfully prepared: %s", contract_payment_data)

        logger.debug("====== erp_contract_payment - end ======")

    except Exception as e:
        logger.error(f"An error occurred in erp_contract_payment: {e}")


