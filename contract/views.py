import pandas as pd
from django.http import HttpResponse
from django.shortcuts import get_object_or_404

from contract.models import Contract


def generate_contract_excel_data(contract):
    try:
        # Process contract data here and return a dictionary or list of dictionaries with the processed data
        contract_data = {
            "Code": [contract.code],  # List with a single element for each field
            "State": [contract.state],
            "Amount": [contract.amount],
            "Date Payment Due": [str(contract.date_payment_due)],
            "Date Valid From": [str(contract.date_valid_from)],
            "Date Valid To": [str(contract.date_valid_to)],
            "Amendment": [contract.amendment],
            # Add more fields if needed
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
