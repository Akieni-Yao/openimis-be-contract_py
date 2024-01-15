from core.service_signals import ServiceSignalBindType
from policy.models import Policy
from .config import get_message_approved_contract
from .email_report import generate_report_for_employee_declaration
from .models import Contract, ContractContributionPlanDetails
from core.signals import Signal, register_service_signal, bind_service_signal
from django.db.models import Q
from django.db.models.signals import post_save
from django.conf import settings
from django.core.mail import send_mail, BadHeaderError, EmailMessage
from django.dispatch import receiver
from insuree.apps import InsureeConfig
from insuree.signals import signal_before_insuree_policy_query
from payment.apps import PaymentConfig
from payment.models import Payment, PaymentDetail
from payment.signals import signal_before_payment_query
from policy.signals import signal_check_formal_sector_for_policy
from policyholder.apps import PolicyholderConfig
from policyholder.models import PolicyHolderUser, PolicyHolderInsuree
from insuree.models import InsureePolicy, Insuree, Family

import logging

from .views import multi_contract, send_contract

logger = logging.getLogger("openimis." + __name__)

_contract_signal_params = ["contract", "user"]
_contract_approve_signal_params = ["contract", "user", "contract_details_list", "service_object", "payment_service",
                                   "ccpd_service"]
signal_contract = Signal(providing_args=_contract_signal_params)
signal_contract_approve = Signal(providing_args=_contract_signal_params)


def on_contract_signal(sender, **kwargs):
    contract = kwargs["contract"]
    user = kwargs["user"]
    __save_or_update_contract(contract=contract, user=user)
    return f"contract updated - state {contract.state}"


def on_contract_approve_signal(sender, **kwargs):
    # approve scenario
    logger.info("on_contract_approve_signal : --------------------- Start ---------------------")
    user = kwargs["user"]
    contract_to_approve = kwargs["contract"]
    contract_details_list = kwargs["contract_details_list"]
    contract_service = kwargs["service_object"]
    payment_service = kwargs["payment_service"]
    ccpd_service = kwargs["ccpd_service"]
    logger.info(f"on_contract_approve_signal : contract_to_approve = {contract_to_approve}")
    # contract valuation
    contract_contribution_plan_details = contract_service.evaluate_contract_valuation(
        contract_details_result=contract_details_list,
        save=True
    )
    # contract_to_approve.amount_due = contract_contribution_plan_details["total_amount"]
    amount_due = contract_contribution_plan_details["total_amount"]
    logger.info(f"on_contract_approve_signal : amount_due = {amount_due}")
    if isinstance(amount_due, str):
        amount_due = float(amount_due)
    rounded_amount = round(amount_due, 2)
    contract_to_approve.amount_due = rounded_amount
    logger.info(f"on_contract_approve_signal : rounded_amount = {rounded_amount}")
    result = ccpd_service.create_contribution(contract_contribution_plan_details)
    
    
    from core import datetime
    now = datetime.datetime.now()
    # check and add penalty of the contract
    ccpdm = ContractContributionPlanDetails.objects.filter(contract_details__contract__id=contract_to_approve.id, is_deleted=False).first()
    product_config = ccpdm.contribution_plan.benefit_plan.config_data
    logger.info(f"on_contract_approve_signal : product_id = {ccpdm.contribution_plan.benefit_plan.id}")
    
    logger.info(f"on_contract_approve_signal : product_config = {ccpdm.contribution_plan.benefit_plan.config_data}")
    
    start_date_to_create_contract = product_config.get("declarationStartDate")
    last_date_to_create_contract = product_config.get("declarationEndDate")
    
    start_date_to_create_contract = datetime.datetime.strptime(start_date_to_create_contract, "%Y-%m-%d").date()
    last_date_to_create_contract = datetime.datetime.strptime(last_date_to_create_contract, "%Y-%m-%d").date()
    
    start_date_day_to_create_contract = start_date_to_create_contract.day
    last_date_day_to_create_contract = last_date_to_create_contract.day
    
    contract_create_date = contract_to_approve.date_created.date()
    contract_create_date_day = contract_create_date.day
    contract_create_date_month = contract_create_date.month
    contract_create_date_year = contract_create_date.year
    
    logger.info(f"on_contract_approve_signal : start_date_to_create_contract = {start_date_to_create_contract}")
    logger.info(f"on_contract_approve_signal : last_date_to_create_contract = {last_date_to_create_contract}")
    logger.info(f"on_contract_approve_signal : contract_create_date = {contract_create_date}")
    
    if start_date_day_to_create_contract < last_date_day_to_create_contract and start_date_day_to_create_contract < contract_create_date_day and contract_create_date_day < last_date_day_to_create_contract:
        logger.info("on_contract_approve_signal : date day condition 1 ---------------------")
        start_date_to_create_contract = start_date_to_create_contract.replace(
            day=start_date_day_to_create_contract, 
            month=contract_create_date_month, year=contract_create_date_year)
        last_date_to_create_contract = last_date_to_create_contract.replace(
            day=last_date_day_to_create_contract, month=contract_create_date_month, year=contract_create_date_year)
    elif start_date_day_to_create_contract < last_date_day_to_create_contract and start_date_day_to_create_contract > contract_create_date_day and contract_create_date_day < last_date_day_to_create_contract:
        logger.info("on_contract_approve_signal : date day condition 2 ---------------------")
        start_date_to_create_contract = start_date_to_create_contract.replace(
            day=start_date_day_to_create_contract, 
            month=contract_create_date_month, year=contract_create_date_year)
        last_date_to_create_contract = last_date_to_create_contract.replace(
            day=last_date_day_to_create_contract, month=contract_create_date_month, year=contract_create_date_year)
    elif start_date_day_to_create_contract > last_date_day_to_create_contract and start_date_day_to_create_contract < contract_create_date_day and contract_create_date_day > last_date_day_to_create_contract:
        logger.info("on_contract_approve_signal : date day condition 3 ---------------------")
        start_date_to_create_contract = start_date_to_create_contract.replace(
            day=start_date_day_to_create_contract, month=contract_create_date_month, year=contract_create_date_year)
        if contract_create_date_month == 12:
            contract_create_date_month = 0
            contract_create_date_year += 1
        last_date_to_create_contract = last_date_to_create_contract.replace(
            day=last_date_day_to_create_contract, month=contract_create_date_month + 1, year=contract_create_date_year)
    elif start_date_day_to_create_contract > last_date_day_to_create_contract and start_date_day_to_create_contract > contract_create_date_day and contract_create_date_day < last_date_day_to_create_contract:
        logger.info("on_contract_approve_signal : date day condition 4 ---------------------")
        last_date_to_create_contract = last_date_to_create_contract.replace(
            day=last_date_day_to_create_contract, 
            month=contract_create_date_month, year=contract_create_date_year)
        if contract_create_date_month == 1:
            contract_create_date_month = 13
            contract_create_date_year -= 1
        start_date_to_create_contract = start_date_to_create_contract.replace(
            day=start_date_day_to_create_contract, 
            month=contract_create_date_month - 1, year=contract_create_date_year)

    logger.info(f"on_contract_approve_signal : start_date_to_create_contract = {start_date_to_create_contract}")
    logger.info(f"on_contract_approve_signal : last_date_to_create_contract = {last_date_to_create_contract}")
    logger.info(f"on_contract_approve_signal : contract_create_date = {contract_create_date}")
    
    if start_date_to_create_contract < contract_create_date and contract_create_date > last_date_to_create_contract:
        logger.info("on_contract_approve_signal : contract penalty applied ---------------------")
        contract_to_approve.penalty_raised = True
        contract_to_approve.penalty_raised_date = now
    
    result_payment = __create_payment(contract_to_approve, payment_service, contract_contribution_plan_details, product_config)
    # STATE_EXECUTABLE
    contract_to_approve.date_approved = now
    contract_to_approve.state = Contract.STATE_EXECUTABLE
    approved_contract = __save_or_update_contract(contract=contract_to_approve, user=user)
    email_contact_name = contract_to_approve.policy_holder.contact_name["contactName"] \
        if contract_to_approve.policy_holder.contact_name and "contactName" in contract_to_approve.policy_holder.contact_name \
        else contract_to_approve.policy_holder.contact_name
    email = __send_email_notify_payment(
        contract_id = contract_to_approve.id,
        code=contract_to_approve.code,
        name=contract_to_approve.policy_holder.trade_name,
        contact_name=email_contact_name,
        amount_due=contract_to_approve.amount_due,
        payment_reference=contract_to_approve.payment_reference,
        email=contract_to_approve.policy_holder.email,
        policy_holder_id=contract_to_approve.policy_holder_id,
        contract_approved_date = now,
    )
    logger.info(f"on_contract_approve_signal : approved_contract = {approved_contract}")
    logger.info("on_contract_approve_signal : --------------------- End ---------------------")
    return approved_contract


# additional filters for payment in 'contract' tab
def append_contract_filter(sender, **kwargs):
    user = kwargs.get("user", None)
    additional_filter = kwargs.get('additional_filter', None)
    if "contract" in additional_filter:
        # then check perms
        if user.has_perms(PaymentConfig.gql_query_payments_perms) or user.has_perms(
                PolicyholderConfig.gql_query_payment_portal_perms):
            contract_id = additional_filter["contract"]
            contract_to_process = Contract.objects.filter(id=contract_id).first()
            # check if user is linked to ph in policy holder user table
            type_user = f"{user}"
            # related to user object output (i) or (t)
            # check if we have interactive user from current context
            if '(i)' in type_user:
                from core import datetime
                now = datetime.datetime.now()
                ph_user = PolicyHolderUser.objects.filter(
                    Q(policy_holder__id=contract_to_process.policy_holder.id, user__id=user.id)
                ).filter(
                    Q(date_valid_from=None) | Q(date_valid_from__lte=now),
                    Q(date_valid_to=None) | Q(date_valid_to__gte=now)
                ).first()
                if ph_user or user.has_perms(PaymentConfig.gql_query_payments_perms):
                    return Q(
                        payment_details__premium__contract_contribution_plan_details__contract_details__contract__id=contract_id
                    )


# additional filters for InsureePolicy in contract
def append_contract_policy_insuree_filter(sender, **kwargs):
    user = kwargs.get("user", None)
    additional_filter = kwargs.get('additional_filter', None)
    if "contract" in additional_filter:
        # then check perms
        if user.has_perms(InsureeConfig.gql_query_insuree_policy_perms) or user.has_perms(
                PolicyholderConfig.gql_query_insuree_policy_portal_perms):
            contract_id = additional_filter["contract"]
            contract_to_process = Contract.objects.filter(id=contract_id).first()
            # check if user is linked to ph in policy holder user table
            type_user = f"{user}"
            # related to user object output (i) or (t)
            # check if we have interactive user from current context
            if '(i)' in type_user:
                from core import datetime
                now = datetime.datetime.now()
                ph_user = PolicyHolderUser.objects.filter(
                    Q(policy_holder__id=contract_to_process.policy_holder.id, user__id=user.id)
                ).filter(
                    Q(date_valid_from=None) | Q(date_valid_from__lte=now),
                    Q(date_valid_to=None) | Q(date_valid_to__gte=now)
                ).first()
                if ph_user or user.has_perms(InsureeConfig.gql_query_insuree_policy_perms):
                    policies = list(
                        ContractContributionPlanDetails.objects.filter(
                            contract_details__contract__id=contract_id).values_list("policy", flat=True)
                    )
                    return Q(
                        start_date__gte=contract_to_process.date_valid_from,
                        start_date__lte=contract_to_process.date_valid_to,
                        policy__in=policies
                    )


# check if policy is related to formal sector contract
def formal_sector_policies(sender, **kwargs):
    policy_id = kwargs.get('policy_id', None)
    ccpd = ContractContributionPlanDetails.objects.filter(policy__id=policy_id, is_deleted=False).first()
    if ccpd:
        cd = ccpd.contract_details
        contract = cd.contract
        return contract.policy_holder
    else:
        return None


signal_contract.connect(on_contract_signal, dispatch_uid="on_contract_signal")
signal_contract_approve.connect(on_contract_approve_signal, dispatch_uid="on_contract_approve_signal")
signal_before_payment_query.connect(append_contract_filter)
signal_before_insuree_policy_query.connect(append_contract_policy_insuree_filter)
signal_check_formal_sector_for_policy.connect(formal_sector_policies)


@receiver(post_save, sender=Payment, dispatch_uid="payment_signal_paid")
def activate_contracted_policies(sender, instance, **kwargs):
    logger.info("====  activate_contracted_policies  ====  start  ====")
    received_amount = instance.received_amount if instance.received_amount else 0
    # check if payment is related to the contract
    payment_detail = PaymentDetail.objects.filter(
        payment__id=int(instance.id)
    ).prefetch_related(
        'premium__contract_contribution_plan_details__contract_details__contract'
    ).prefetch_related(
        'premium__contract_contribution_plan_details'
    ).filter(premium__contract_contribution_plan_details__isnull=False)
    if len(list(payment_detail)) > 0:
        if instance.expected_amount <= received_amount:
            contribution_list_id = [pd.premium.id for pd in payment_detail]
            contract_list = Contract.objects.filter(
                contractdetails__contractcontributionplandetails__contribution__id__in=contribution_list_id
            ).distinct()
            ccpd_number = ContractContributionPlanDetails.objects.prefetch_related('contract_details__contract').filter(
                contract_details__contract__in=list(contract_list)
            ).count()
            contract_plan_periodicity = ContractContributionPlanDetails.objects.prefetch_related('contract_details__contract').filter(
                contract_details__contract__in=list(contract_list)
            ).first()
            periodicity = contract_plan_periodicity.contribution_plan.periodicity if contract_plan_periodicity.contribution_plan else None
            logger.info(f"====  activate_contracted_policies  ====  periodicity  ====  {periodicity}")
            logger.info(f"====  activate_contracted_policies  ====  contribution_list_id  ====  {contribution_list_id}")

            if ccpd_number == len(list(payment_detail)):
                for contract in contract_list:
                    if contract.state == Contract.STATE_EXECUTABLE:
                        # get the ccpd related to the currently processing contract
                        ccpd_list = list(
                            ContractContributionPlanDetails.objects.prefetch_related(
                                'contract_details__contract').filter(
                                contract_details__contract=contract
                            )
                        )
                        # TODO support Split payment and check that
                        #  the payment match the value of all contributions

                        # Activate all employees. If the contact has to be activated but employee activation requires
                        # additional rules, intercept the signal on activate_contract_contribution_plan_detail
                        for ccpd in ccpd_list:
                            try:
                                logger.info(f"====  activate_contracted_policies  ====  ccpd  ====  {ccpd}")
                                pass
                                # assign_policy = False
                                # insuree_pd = PaymentDetail.objects.filter(insurance_number=ccpd.contract_details.insuree.chf_id,
                                #         premium__contract_contribution_plan_details__contract_details__contract__policy_holder=ccpd.contract_details.contract.policy_holder,
                                #         premium__contract_contribution_plan_details__isnull=False).all()

                                # if periodicity == 1 and len(insuree_pd) >= 3:
                                #     assign_policy = True
                                # elif periodicity == 3 and len(insuree_pd) >= 1:
                                #     assign_policy = True

                                # if assign_policy:
                                #     if ccpd.contract_details.insuree.status == "APPROVED" and ccpd.contract_details.insuree.document_status and ccpd.contract_details.insuree.biometrics_is_master:
                                #         PolicyHolderInsuree.objects.filter(policy_holder__uuid=ccpd.contract_details.contract.policy_holder.uuid, insuree_id=ccpd.contract_details.insuree.id).update(is_rights_enable_for_insuree=True, is_payment_done_by_policy_holder=True)
                                #         result = ContractActivationService.activate_contract_contribution_plan_detail(ccpd)
                                #         logger.info(f"====  activate_contracted_policies  ==== activate_contract_contribution_plan_detail result  ====  {result}")
                                #         if not result:
                                #             logger.info("Contract contribution plan detail ccpd.id not activated")
                                #         else:
                                #             Insuree.objects.filter(id=ccpd.contract_details.insuree.id).update(status="ACTIVE")
                                #             insuree = Insuree.objects.filter(id=ccpd.contract_details.insuree.id).first()
                                #             logger.info(f"====  activate_contracted_policies  ====  insuree.status  ====  {insuree.status}")
                                #             family_members = Insuree.objects.filter(family_id=insuree.family.id, legacy_id=None).all()
                                #             all_insuree = True
                                #             for member in family_members:
                                #                 if member.status == 'APPROVED':
                                #                     Insuree.objects.filter(id=member.id).update(status="ACTIVE")
                                #             for member in family_members:
                                #                 if member.status != insuree.status:
                                #                     all_insuree = False
                                #                     break
                                #             if all_insuree:
                                #                 Family.objects.filter(id=insuree.family.id).update(status=insuree.status)
                                #                 logger.info("====  activate_contracted_policies  ====  family.status  ====  ACTIVE")
                                #     else:
                                #         PolicyHolderInsuree.objects.filter(policy_holder__uuid=ccpd.contract_details.contract.policy_holder.uuid, insuree_id=ccpd.contract_details.insuree.id).update(is_payment_done_by_policy_holder=True)
                                #         logger.info("====  activate_contracted_policies  ====  PolicyHolderInsuree  ====  is_payment_done_by_policy_holder=True")
                                # else:
                                #     logger.info(f"Policy can not be assigned for {ccpd}")
                                # # result = ContractActivationService.activate_contract_contribution_plan_detail(ccpd)
                                # # if not result:
                                # #     logger.info("Contract contribution plan detail ccpd.id not activated")
                            except Exception as e:
                                logger.error(f"Contract contribution plan detail ccpd not activated {e}")
                        # contract.state = Contract.STATE_EFFECTIVE
                        # __save_or_update_contract(contract, contract.user_updated)
    logger.info("====  activate_contracted_policies  ====  end  ====")


class ContractActivationService:

    @classmethod
    @register_service_signal('activate_contract_contribution_plan_detail')
    def activate_contract_contribution_plan_detail(cls, ccpd):
        logger.info("====  activate_contract_contribution_plan_detail  ====  start  ====")
        from core import datetime, datetimedelta
        insuree = ccpd.contract_details.insuree
        pi = InsureePolicy.objects.create(
            **{
                "insuree": insuree,
                "policy": ccpd.policy,
                "enrollment_date": ccpd.date_valid_from,
                "start_date": ccpd.date_valid_from,
                "effective_date": ccpd.date_valid_from,
                "expiry_date": ccpd.date_valid_to + datetimedelta(
                    ccpd.contribution_plan.get_contribution_length()
                ),
                "audit_user_id": -1,
            }
        )
        ccpd.policy.status = Policy.STATUS_ACTIVE
        ccpd.policy.save()
        logger.info(f"====  activate_contract_contribution_plan_detail  ====  InsureePolicy  ====  {pi}")
        logger.info("====  activate_contract_contribution_plan_detail  ====  end  ====")
        return pi


# def example_only_activate_employees_named_john(data=None, **kwargs):
#     ccpd = data[0][0]
#     if "John" in ccpd.contract_details.insuree.other_names:
#         raise Exception("John is not allowed to be insured")


# bind_service_signal(
#     'activate_contract_contribution_plan_detail',
#     example_only_activate_employees_named_john,
#     bind_type=ServiceSignalBindType.BEFORE
# )


def __save_json_external(user_id, datetime, message):
    return {
        "comments": [{
            "From": "Portal/webapp",
            "user": user_id,
            "date": datetime,
            "msg": message
        }]
    }


def __save_or_update_contract(contract, user):
    contract.save(username=user.username)
    historical_record = contract.history.all().first()
    contract.json_ext = __save_json_external(
        user_id=str(historical_record.user_updated.id),
        datetime=str(historical_record.date_updated),
        message=f"contract updated - state "
                f"{historical_record.state}"
    )
    contract.save(username=user.username)
    return contract


def __create_payment(contract, payment_service, contract_cpd, product_config=None):
    logger.info("__create_payment : ------------  start  ------------")
    from core import datetime
    now = datetime.datetime.now()
    # format payment data
    payment_data = {
        "expected_amount": contract.amount_due,
        "request_date": now,
        "contract": contract,
        "product_config": product_config,
    }
    logger.info(f"__create_payment : payment_data = {payment_data}")
    payment_details_data = payment_service.collect_payment_details(contract_cpd["contribution_plan_details"])
    logger.info("__create_payment : ------------  end  ------------")
    return payment_service.create(payment=payment_data, payment_details=payment_details_data)


def __send_email_notify_payment(contract_id,code, name, contact_name, amount_due, payment_reference, email,policy_holder_id,contract_approved_date):
    try:
        email_message = EmailMessage(
            subject='Contract payment notification',
            body=get_message_approved_contract(
                language=settings.LANGUAGE_CODE.split('-')[0],
                code=code,
                name=name,
                contact_name=contact_name,
                due_amount=amount_due,
                payment_reference=payment_reference
            ),
            from_email=settings.EMAIL_HOST_USER,
            to=[email],
        )
        # Attach the PDF file
        pdf_file = generate_report_for_employee_declaration(code, policy_holder_id, contract_approved_date, amount_due)
        email_message.attach('payment_receipt.pdf', pdf_file, 'application/pdf')
        # Attach the Excel file
        excel_file =  send_contract(contract_id)
        email_message.attach('employee_declaration.xlsx', excel_file,
                             'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        # Send the email
        email_message.send()
        return True
    except BadHeaderError:
        return ValueError('Invalid header found.')
