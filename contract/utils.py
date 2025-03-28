import json
import logging
from datetime import datetime, timedelta
from datetime import datetime as dt

from contribution_plan.models import ContributionPlanBundleDetails
from core.models import User
from django.db.models import Q
from django.http import Http404, JsonResponse
from insuree.abis_api import create_abis_insuree
from insuree.dms_utils import (
    create_openKm_folder_for_bulkupload,
    send_mail_to_temp_insuree_with_pdf,
)
from insuree.models import Family, Insuree
from payment.models import Payment, PaymentDetail

# from policyholder.models import PolicyHolder
from policyholder.models import (
    PolicyHolder,
    PolicyHolderContributionPlan,
    PolicyHolderInsuree,
)

# from contract.views import resolve_custom_field
from report.apps import ReportConfig
from report.services import generate_report, get_report_definition
from workflow.workflow_stage import insuree_add_to_workflow

from contract.models import Contract, ContractContributionPlanDetails, ContractDetails

logger = logging.getLogger(__name__)


def resolve_custom_field(detail):
    try:
        cpb = detail.contribution_plan_bundle
        cpbd = ContributionPlanBundleDetails.objects.filter(
            contribution_plan_bundle=cpb, is_deleted=False
        ).first()
        conti_plan = cpbd.contribution_plan if cpbd else None
        ercp = 0
        eecp = 0
        if conti_plan and conti_plan.json_ext:
            json_data = conti_plan.json_ext
            calculation_rule = json_data.get("calculation_rule")
            if calculation_rule:
                ercp = float(calculation_rule.get("employerContribution", 0.0))
                eecp = float(calculation_rule.get("employeeContribution", 0.0))

        # Uncommented lines can be used if needed for future logic
        # insuree = self.insuree
        # policy_holder = self.contract.policy_holder
        # phn_json = PolicyHolderInsuree.objects.filter(
        #     insuree_id=insuree.id,
        #     policy_holder__code=policy_holder.code,
        #     policy_holder__date_valid_to__isnull=True,
        #     policy_holder__is_deleted=False,
        #     date_valid_to__isnull=True,
        #     is_deleted=False
        # ).first()
        # if phn_json and phn_json.json_ext:
        #     json_data = phn_json.json_ext
        #     ei = float(json_data.get('calculation_rule', {}).get('income', 0))
        self_json = detail.json_ext if detail.json_ext else None
        ei = 0.0
        if self_json:
            ei = float(self_json.get(
                "calculation_rule", {}).get("income", 0.0))

        # Use integer arithmetic to avoid floating-point issues
        employer_contribution = (
            ei * ercp / 100) if ercp and ei is not None else 0.0
        salary_share = (ei * eecp / 100) if eecp and ei is not None else 0.0
        total = salary_share + employer_contribution

        response = {
            "total": total,
            "employerContribution": employer_contribution,
            "salaryShare": salary_share,
        }

        return response
    except Exception as e:
        response = {
            "total": 0,
            "employerContribution": None,
            "salaryShare": 0,
        }
        return response


def filter_amount_contract(arg="amount_from", arg2="amount_to", **kwargs):
    amount_from = kwargs.get(arg)
    amount_to = kwargs.get(arg2)

    status_notified = [1, 2]
    status_rectified = [4, 11, 3]
    status_due = [5, 6, 7, 8, 9, 10]

    # scenario - only amount_to set
    if not amount_from and amount_to:
        return (
            Q(amount_notified__lte=amount_to, state__in=status_notified)
            | Q(amount_rectified__lte=amount_to, state__in=status_rectified)
            | Q(amount_due__lte=amount_to, state__in=status_due)
        )

    # scenario - only amount_from set
    if amount_from and not amount_to:
        return (
            Q(amount_notified__gte=amount_from, state__in=status_notified)
            | Q(amount_rectified__gte=amount_from, state__in=status_rectified)
            | Q(amount_due__gte=amount_from, state__in=status_due)
        )

    # scenario - both filters set
    if amount_from and amount_to:
        return (
            Q(
                amount_notified__gte=amount_from,
                amount_notified__lte=amount_to,
                state__in=status_notified,
            )
            | Q(
                amount_rectified__gte=amount_from,
                amount_rectified__lte=amount_to,
                state__in=status_rectified,
            )
            | Q(
                amount_due__gte=amount_from,
                amount_due__lte=amount_to,
                state__in=status_due,
            )
        )


def get_period_date(contract_date_valid_from):
    if contract_date_valid_from:
        year = str(contract_date_valid_from.strftime("%Y"))
        month = str(contract_date_valid_from.strftime("%m"))
        months_french = [
            "Janvier",
            "Février",
            "Mars",
            "Avril",
            "Mai",
            "Juin",
            "Juillet",
            "Août",
            "Septembre",
            "Octobre",
            "Novembre",
            "Décembre",
        ]
        return f"{months_french[int(month) - 1]} {year}"
    return ""


def generate_report_for_contract_receipt(contract_id, info):
    from core import datetime

    now = datetime.datetime.now()
    try:
        contract = Contract.objects.filter(
            id=contract_id, is_deleted=False).first()
        payment = Payment.objects.filter(contract=contract).first()
        user = User.objects.filter(id=info.context.user.id).first()
        policy_holder = PolicyHolder.objects.filter(
            id=contract.policy_holder_id, is_deleted=False
        ).first()

        print(f"==================================== contract {contract}")
        print(f"==================================== payment {payment}")
        print(f"==================================== user {user}")
        print(
            f"==================================== policy_holder {policy_holder}")

        locations = policy_holder.locations
        location = {
            "adresse": policy_holder.address["address"],
            "quartier": locations.name,
            "arrondissement": locations.parent.name,
            "ville": locations.parent.parent.name,
            "department": locations.parent.parent.parent.name,
        }
        print(f"==================================== location {location}")
        # for location in policy_holder.locations:
        # print(f"==================================== location name {locations.name}")
        # print(f"==================================== location type {locations.type}")
        # print(f"==================================== location code {locations.code}")
        # print(f"==================================== location parent {locations.parent}")
        # print(f"==================================== location parent {locations.parent.parent}")

        if contract:
            contract_details = ContractDetails.objects.filter(
                contract_id=contract_id, is_deleted=False
            )
            if contract_details:
                # policy_holder = contract.policy_holder
                # current_date = str(now.strftime("%d-%m-%Y à %H:%M:%S"))
                # date_valid_to = (
                #     str(payment.request_date.strftime("%d-%m-%Y"))
                #     if payment.request_date
                #     else ""
                # )
                total_insuree = contract_details.count()
                total_salary_brut = 0
                part_salariale = 0
                part_patronale = 0
                total_due_pay = (
                    contract.amount_due if contract.amount_due is not None else 0
                )
                print(
                    f"================================= info {info.context.user.id}")

                print(f"==================================== user {user}")

                user_location = "Brazzaville"
                user_name = f"{user.i_user.last_name} {user.i_user.other_names}"

                # if user.i_user.districts:
                #     user_location = user.i_user.districts[0].location.name

                for detail in contract_details:
                    jsonExt = detail.json_ext
                    customField = resolve_custom_field(detail)
                    if jsonExt is None:
                        jsonExt = {"calculation_rule": {"income": 0}}
                    print(
                        f"=========================================== customField {customField} jsonExt {jsonExt}"
                    )
                    total_salary_brut += (
                        int(jsonExt["calculation_rule"]["income"])
                        if jsonExt["calculation_rule"]["income"]
                        else 0
                    )
                    part_salariale += (
                        int(customField["salaryShare"])
                        if customField["salaryShare"]
                        else 0
                    )
                    part_patronale += (
                        int(customField["employerContribution"])
                        if customField["employerContribution"]
                        else 0
                    )

                data = {
                    "data": {
                        "payment_id": payment.payment_code,
                        "period": get_period_date(contract.date_valid_from),
                        "current_date": str(now.strftime("%d-%m-%Y à %H:%M:%S")),
                        "subscriber_name": (
                            policy_holder.trade_name
                            if policy_holder.trade_name is not None
                            else ""
                        ),
                        "subscriber_camu_number": (
                            policy_holder.code if policy_holder.code is not None else ""
                        ),
                        "subscriber_adresse": f"{location['adresse']}, {location['quartier']}, {location['arrondissement']}, {location['ville']}, {location['department']}",
                        "id": contract.code,
                        "created_at": str(contract.date_approved.strftime("%d-%m-%Y")),
                        "date_valid_to": (
                            str(contract.date_payment_due.strftime("%d-%m-%Y"))
                            if contract.date_payment_due
                            else ""
                        ),
                        "total_insuree": total_insuree,
                        "total_salary_brut": total_salary_brut,
                        "part_salariale": part_salariale,
                        "part_patronale": part_patronale,
                        "total_due_pay": total_due_pay,
                        "user_location": user_location,
                        "user_name": user_name,
                    }
                }
                print(
                    f"=========================================== data {data}")
                report_name = "contract_referrals"
                report_config = ReportConfig.get_report(report_name)
                print("=========================================== report_config ")
                if not report_config:
                    raise Http404("Report configuration does not exist")
                report_definition = get_report_definition(
                    report_name, report_config["default_report"]
                )
                print("=========================================== report_definition")
                template_dict = json.loads(report_definition)
                print("=========================================== template_dict")
                pdf = generate_report(report_name, template_dict, data)
                print("Report generated successfully.")
                return pdf
    except Exception as e:
        print(f"An exception occurred: {str(e)}")
        raise  # Re-raise the exception or handle it according to your requirements
    print("PDF not generated.")
    return None  # Handle the case where no PDF is generated


def map_enrolment_type_to_category(enrolment_type):
    # Define the mapping from input values to categories
    enrolment_type_mapping = {
        "Agents de l'Etat": "public_Employees",
        "Salariés du privé": "private_sector_employees",
        "Travailleurs indépendants et professions libérales": "Selfemployed_and_liberal_professions",
        "Pensionnés CRF et CNSS": "CRF_and_CNSS_pensioners",
        "Personnes vulnérables": "vulnerable_Persons",
        "Etudiants": "students",
        "Pensionnés de la CRF et CNSS": "CRF_and_CNSS_pensioners",
    }

    # Check if the enrolment type exists in the mapping dictionary
    if enrolment_type in enrolment_type_mapping:
        return enrolment_type_mapping[enrolment_type]
    else:
        # If the value doesn't match any predefined category, you can handle it accordingly.
        # For example, set a default category or raise an exception.
        return None


def create_new_insuree_and_add_contract_details(
    insuree_name, policy_holder, cpb, contract, user_id, request, enrolment_type
):
    from policyholder.views import generate_available_chf_id

    # split insuree_name by space
    insuree_name_parts = insuree_name.split(" ")
    last_name = insuree_name_parts[0]
    other_names = " ".join(insuree_name_parts[1:])

    village = policy_holder.locations

    dob = datetime.strptime("2007-03-03", "%Y-%m-%d")

    print("======================================= other_names: %s", other_names)
    print("======================================= last_name: %s", last_name)
    print("======================================= village: %s", village.code)

    insuree_by_name = Insuree.objects.filter(
        other_names=other_names,
        last_name=last_name,
        dob=dob,
        validity_to__isnull=True,
        legacy_id__isnull=True,
    ).first()

    if insuree_by_name:
        print(
            "======================================= insuree_by_name already exists: %s",
            insuree_by_name,
        )
        return None

    family = None
    insuree_created = None

    if village:
        family = Family.objects.create(
            head_insuree_id=1,  # dummy
            location=village,
            audit_user_id=user_id,
            status="PRE_REGISTERED",
            address="",
            json_ext={"enrolmentType": map_enrolment_type_to_category(
                enrolment_type)},
        )

    if family:
        insuree_id = generate_available_chf_id(
            "M",
            village,
            dob,
            enrolment_type,
        )
        insuree_created = Insuree.objects.create(
            other_names=other_names,
            last_name=last_name,
            dob=dob,
            family=family,
            audit_user_id=user_id,
            card_issued=False,
            chf_id=insuree_id,
            head=True,
            current_village=village,
            created_by=user_id,
            modified_by=user_id,
            marital="",
            # gender="M",
            # current_address="",
            # phone="",
            # email=line[HEADER_EMAIL],
            json_ext={
                "insureeEnrolmentType": map_enrolment_type_to_category(enrolment_type),
                # "insureelocations": response_data,
                # "BirthPlace": line[HEADER_BIRTH_LOCATION_CODE],
                # "insureeaddress": line[HEADER_ADDRESS],
            },
        )
        chf_id = insuree_id

        try:
            user = request.user
            create_openKm_folder_for_bulkupload(user, insuree_created)
        except Exception as e:
            logger.error(f"insuree bulk upload error for dms: {e}")

        try:
            insuree_add_to_workflow(
                None, insuree_created.id, "INSUREE_ENROLLMENT", "Pre_Register"
            )
            create_abis_insuree(None, insuree_created)
        except Exception as e:
            logger.error(
                f"insuree bulk upload error for abis or workflow : {e}")

        phi = PolicyHolderInsuree(
            insuree=insuree_created,
            policy_holder=policy_holder,
            contribution_plan_bundle=cpb,
            json_ext={},
            employer_number=None,
        )
        phi.save(username=request.user.username)

        contract_detail = ContractDetails(
            contract=contract,
            insuree=insuree_created,
            contribution_plan_bundle=cpb,
            json_ext={},
        )
        contract_detail.save(username=request.user.username)

    print("======================================= created insuree: %s", chf_id)
    return chf_id


def get_period_from_date(date_value):
    try:
        if isinstance(date_value, int):
            return date_value
        elif isinstance(date_value, str):
            return dt.strptime(date_value, "%Y-%m-%d").date().day
    except Exception as e:
        logger.error(f"Error getting period from date: {e}")
        return None


def get_payment_product_config(payment):
    logger.debug("====  get_payment_product_config  : Start  ====")
    payment_details = PaymentDetail.objects.filter(
        payment=payment, legacy_id__isnull=True
    ).first()
    if payment_details:
        ccpd = ContractContributionPlanDetails.objects.filter(
            contribution__id=payment_details.premium.id
        ).first()
        if ccpd:
            product_config = ccpd.contribution_plan.benefit_plan.config_data
            # product_config : {'paymentEndDate': '2024-11-05', 'sanctionAmount': 5000000, 'firstPenaltyRate': 3, 'paymentStartDate': '2024-10-20', 'secondPenaltyRate': 3, 'declarationEndDate': '2024-11-05', 'declarationStartDate': '2024-10-20'}
            logger.debug(
                f"====  get_payment_product_config  : product_config : {product_config}  ===="
            )
            logger.debug("====  get_payment_product_config  : End  ====")
            return product_config
    logger.debug("====  get_payment_product_config  : End  ====")
    return None


def get_next_month_limit_date(cpb_date, contract_date):
    from dateutil.relativedelta import relativedelta

    try:
        new_date = None
        current_day = None

        if cpb_date:
            current_day = get_period_from_date(cpb_date)

        if contract_date:
            new_date = contract_date.replace(
                day=current_day) + relativedelta(months=1)

        if hasattr(datetime.date, "from_ad_date"):
            new_date = datetime.date.from_ad_date(new_date)
        return new_date
    except Exception as e:
        logger.error(f"Error getting next month limit date: {e}")
        return None


def get_due_payment_date(contract):
    payment = Payment.objects.filter(contract=contract).first()
    payment_due_date = None
    if payment:
        product_config = get_payment_product_config(payment)
        payment_end_date = product_config.get("paymentEndDate")
        contract_date_valid_to = contract.date_valid_from
        # now = datetime.now().date()
        payment_due_date = get_next_month_limit_date(
            payment_end_date, contract_date_valid_to)
    logger.info("************************************************************")
    logger.info(
        f"config_payment_end_date  : {product_config['paymentEndDate']}")
    logger.info(f"payment_due_date  : {payment_due_date}")
    logger.info(f"contract_date_valid_to  : {contract_date_valid_to}")
    logger.info("************************************************************")
    return payment_due_date
