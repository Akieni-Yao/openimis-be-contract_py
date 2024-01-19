import io

import pandas as pd
from django.http import HttpResponse

from contract.models import ContractDetails
from contribution_plan.models import ContributionPlanBundleDetails
from policyholder.models import PolicyHolderInsuree


def generate_multi_contract_excel_data(contract_detail):
    try:
        contract_detail = get_contract_custom_field_data(contract_detail)
        contract_data = {"Assuré": contract_detail["insuree"]["lastName"] + " " + contract_detail["insuree"][
            "otherNames"] if contract_detail.get("insuree") else '',
                         "Numéro CAMU": str(contract_detail["insuree"]["camu_number"]) if contract_detail.get(
                             "insuree") and
                                                                                          contract_detail[
                                                                                              "insuree"].get(
                                                                                              "camu_number") else '',
                         "Numéro CAMU temporaire": contract_detail["insuree"]["chfId"] if contract_detail.get("insuree") and
                                                                                     contract_detail["insuree"].get(
                                                                                         "chfId") else '',
                         "Ensemble du plan de contribution": contract_detail["contributionPlanBundle"]["code"] + " - " +
                                                             contract_detail["contributionPlanBundle"][
                                                                 "name"] if contract_detail.get(
                             "contributionPlanBundle") and
                                                                            contract_detail[
                                                                                "contributionPlanBundle"].get(
                                                                                "code") and
                                                                            contract_detail[
                                                                                "contributionPlanBundle"].get(
                                                                                "name") else '',
                         "Gross Salary": str(
                             contract_detail["jsonExt"]["calculation_rule"]["income"]) if contract_detail.get(
                             "jsonExt") and contract_detail["jsonExt"].get("calculation_rule") and
                                                                                          contract_detail["jsonExt"][
                                                                                              "calculation_rule"].get(
                                                                                              "income") else '',
                         "Cotisation de l'employeur": str(
                             contract_detail["customField"]["employerContribution"]) if contract_detail.get(
                             "customField") and
                                                                                        contract_detail[
                                                                                            "customField"].get(
                                                                                            "employerContribution") else '',
                         "Cotisation des employés": str(
                             contract_detail["customField"]["salaryShare"]) if contract_detail.get(
                             "customField") and contract_detail["customField"].get("salaryShare") else '',
                         "Total": str(contract_detail["customField"]["total"]) if contract_detail.get("customField") and
                                                                                  contract_detail["customField"].get(
                                                                                      "total") else '', }
        print(contract_data)

        return contract_data
    except Exception as e:
        return None


def multi_contract(request, contract_id):
    contract_details = ContractDetails.objects.filter(contract_id=contract_id)
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
    response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = 'attachment; filename="multiple_contracts.xlsx"'
    # Write DataFrame to response as an Excel file
    df.to_excel(response, index=False, header=True)
    return response


def get_contract_custom_field_data(detail):
    cpb = detail.contribution_plan_bundle
    cpbd = ContributionPlanBundleDetails.objects.filter(contribution_plan_bundle=cpb, is_deleted=False).first()
    conti_plan = cpbd.contribution_plan if cpbd else None
    ercp = 0
    eecp = 0
    if conti_plan and conti_plan.json_ext:
        json_data = conti_plan.json_ext
        calculation_rule = json_data.get('calculation_rule')
        if calculation_rule:
            ercp = float(calculation_rule.get('employerContribution', 0))
            eecp = float(calculation_rule.get('employeeContribution', 0))

    insuree = detail.insuree
    policy_holder = detail.contract.policy_holder
    phn_json = PolicyHolderInsuree.objects.filter(insuree_id=insuree.id, policy_holder__code=policy_holder.code,
                                                  policy_holder__date_valid_to__isnull=True,
                                                  policy_holder__is_deleted=False, date_valid_to__isnull=True,
                                                  is_deleted=False).first()
    ei = 0
    if phn_json and phn_json.json_ext:
        json_data = phn_json.json_ext
        ei = float(json_data.get('calculation_rule', {}).get('income', 0))
    employer_contribution = round(ei * ercp / 100, 2) if ercp and ei is not None else 0
    salary_share = round(ei * eecp / 100, 2) if eecp and ei is not None else 0
    total = salary_share + employer_contribution
    custom_field_data = {'total': total if total is not None else 0,
                         'employerContribution': employer_contribution if employer_contribution is not None else 0,
                         'salaryShare': salary_share if salary_share is not None else 0, }
    contract_data = {'id': detail.id, 'jsonExt': detail.json_ext or '',  # Set empty string if json_ext is None
                     'contract': {'id': detail.contract.id if detail.contract else '',
                                  # Set empty string if contract is None
                                  },
                     'insuree': {'id': insuree.id if insuree else '',  # Set empty string if insuree is None
                                 'uuid': insuree.uuid if insuree else '', 'chfId': insuree.chf_id if insuree else '',
                                 'lastName': insuree.last_name if insuree else '',
                                 'otherNames': insuree.other_names if insuree else '', },
                     'contributionPlanBundle': {'id': cpb.id if cpb else '',  # Set empty string if cpb is None
                                                'code': cpb.code if cpb else '', 'name': cpb.name if cpb else '',
                                                'periodicity': cpb.periodicity if cpb else '',
                                                'dateValidFrom': cpb.date_valid_from if cpb else '',
                                                'dateValidTo': cpb.date_valid_to if cpb else '',
                                                'isDeleted': cpb.is_deleted if cpb else '',
                                                'replacementUuid': cpb.replacement_uuid if cpb else '', },
                     'customField': custom_field_data, }
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
