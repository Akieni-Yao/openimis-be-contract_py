import pandas as pd
from django.http import HttpResponse
from django.shortcuts import get_object_or_404

from contract.models import Contract, ContractDetails


def generate_contract_excel_data(contract):
    try:
        # Process contract data here and return a dictionary or list of dictionaries with the processed data
        contract_data = {
            "Code": [contract.code],  # List with a single element for each field
            "État": [contract.state],
            "Montant": [contract.amount],
            "Date d'échéance du paiement": [str(contract.date_payment_due)],
            "Valable à partir de": [str(contract.date_valid_from)],
            "Valable jusqu'à": [str(contract.date_valid_to)],
            "Amendement": [contract.amendment],
        }
        return contract_data
    except Exception as e:
        # Handle exceptions here
        print(f"Error generating contract data: {e}")
        return None


def single_contract(request, id):
    try:
        contract = get_object_or_404(Contract, id=id)
        contract_data = generate_contract_excel_data(contract)

        if contract_data is None:
            # Handle the case where contract data generation failed
            return HttpResponse("Failed to generate contract data", status=500)

        # Convert contract data to a DataFrame
        df = pd.DataFrame(contract_data)

        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename="data.xlsx"'

        # Write DataFrame to response as an Excel file
        df.to_excel(response, index=False, header=True)

        return response
    except Exception as e:
        # Handle exceptions during export
        print(f"Error exporting contract data: {e}")
        return HttpResponse("Failed to export contract data", status=500)


def generate_multi_contract_excel_data(contract_detail):
    try:
        contract_data = {
            "Numéro CAMU": contract_detail.insuree.camu_number,
            "N° d'ins. du Resp": contract_detail.contract.state,
            "Ensemble du plan de contribution": contract_detail.contribution_plan_bundle.name,
            # "Date Payment Due": str(contract_detail.contract.date_payment_due),
            # "Date Valid From": str(contract_detail.contract.date_valid_from),
            # "Date Valid To": str(contract_detail.contract.date_valid_to),
            # "Amendment": contract_detail.contract.amendment,
            # # Add more fields if needed
            # "Insuree": contract_detail.insuree.name,  # Example: Accessing related Insuree data
            # "Contribution Plan Bundle": contract_detail.contribution_plan_bundle.name
            # Example: Accessing related ContributionPlanBundle data
        }
        return contract_data
    except Exception as e:
        print(f"Error generating contract data: {e}")
        return None


def multi_contract(request, contract_id):
    contract_details = ContractDetails.objects.filter(contract_id=contract_id)
    # Initialize an empty list to hold all contract data
    all_contract_data = []
    for detail in contract_details:
        contract_data = generate_multi_contract_excel_data(detail)
        if contract_data:
            all_contract_data.append(contract_data)

    if not all_contract_data:
        return HttpResponse("No contract data found", status=404)

        # Create a DataFrame from all the contract data
    df = pd.DataFrame(all_contract_data)

    response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = 'attachment; filename="multiple_contracts.xlsx"'

    # Write DataFrame to response as an Excel file
    df.to_excel(response, index=False, header=True)

    return response
