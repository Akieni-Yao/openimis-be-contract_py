import pandas as pd
from django.http import HttpResponse

from contract.models import ContractDetails
from contribution_plan.models import ContributionPlanBundleDetails
from policyholder.models import PolicyHolderInsuree


def generate_multi_contract_excel_data(contract_detail):
    try:
        ercp = None
        eecp = None
        ei = None
        cpb = contract_detail.contribution_plan_bundle
        gross_salary = float(contract_detail.json_data.get('calculation_rule', {}).get('income', 0))
        cpd_code_name = f"{contract_detail.contribution_plan_bundle.code}-{contract_detail.contribution_plan_bundle.name}"
        cpbd = ContributionPlanBundleDetails.objects.filter(
            contribution_plan_bundle=cpb,
            is_deleted=False
        ).first()
        conti_plan = cpbd.contribution_plan if cpbd else None
        if conti_plan and conti_plan.json_ext:
            json_data = conti_plan.json_ext
            calculation_rule = json_data.get('calculation_rule')
            if calculation_rule:
                ercp = float(calculation_rule.get('employerContribution', 0))
                eecp = float(calculation_rule.get('employeeContribution', 0))

        insuree = contract_detail.insuree
        insuree_name = f"{insuree.other_names} {insuree.last_name}"
        policy_holder = contract_detail.contract.policy_holder
        phn_json = PolicyHolderInsuree.objects.filter(
            insuree_id=insuree.id,
            policy_holder__code=policy_holder.code,
            policy_holder__date_valid_to__isnull=True,
            policy_holder__is_deleted=False,
            date_valid_to__isnull=True,
            is_deleted=False
        ).first()

        if phn_json and phn_json.json_ext:
            json_data = phn_json.json_ext
            ei = float(json_data.get('calculation_rule', {}).get('income', 0))
        employer_contribution = round(ei * ercp / 100, 2) if ercp and ei is not None else 0
        salary_share = round(ei * eecp / 100, 2) if eecp and ei is not None else 0
        total = salary_share + employer_contribution
        contract_data = {
            "Assuré": insuree_name,
            "Numéro CAMU": contract_detail.insuree.camu_number,
            "N° d'ins. du Resp": contract_detail.insuree.chf_id,
            "Ensemble du plan de contribution": cpd_code_name,
            "Gross Salary": str(gross_salary),
            "Cotisation de l'employeur": str(employer_contribution),
            "Cotisation  des employés": str(salary_share),
            "Total": str(total),
        }
        return contract_data
    except AttributeError as ae:
        return f"Attribute error: {ae}"
    except KeyError as ke:
        return f"Key error: {ke}"
    except Exception as e:
        return f"Error generating contract data: {e}"


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
