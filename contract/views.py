import pandas as pd
from django.http import HttpResponse

from contract.models import ContractDetails
from contribution_plan.models import ContributionPlanBundleDetails
from policyholder.models import PolicyHolderInsuree


def generate_multi_contract_excel_data(contract_detail):
    try:
        ercp = None
        eecp = None
        cpb = contract_detail.contribution_plan_bundle
        ei = float(contract_detail.json_data.get('calculation_rule', {}).get('income', 0))
        cpbd = ContributionPlanBundleDetails.objects.filter(
            contribution_plan_bundle=cpb,
            is_deleted=False
        ).first()
        conti_plan = cpbd.contribution_plan if cpbd else None
        if conti_plan:
            json_data = conti_plan.json_ext if conti_plan.json_ext else None
            calculation_rule = json_data.get('calculation_rule') if json_data else None

            if calculation_rule:
                ercp = float(calculation_rule.get('employerContribution', 0))
                eecp = float(calculation_rule.get('employeeContribution', 0))
        insuree = contract_detail.insuree
        insuree_name = f"{insuree.other_names} {insuree.last_name}"
        employer_contribution = round(ei * ercp / 100, 2) if ercp and ei is not None else 0
        salary_share = round(ei * eecp / 100, 2) if eecp and ei is not None else 0
        total = salary_share + employer_contribution
        contract_data = {
            "Assuré":insuree_name,
            "Numéro CAMU": contract_detail.insuree.camu_number,
            "N° d'ins. du Resp": contract_detail.insuree.chf_id,
            "Ensemble du plan de contribution": contract_detail.contribution_plan_bundle.name,
            "Gross Salary": str(contract_detail.contract.date_payment_due),
            "Cotisation de l'employeur": str(employer_contribution),
            "Cotisation  des employés": str(salary_share),
            "Total": str(total),
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
