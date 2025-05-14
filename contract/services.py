import calendar
import json
import logging
from copy import copy
from datetime import datetime
from decimal import Decimal

from calculation.services import run_calculation_rules
from contribution.models import Premium
from contribution_plan.models import ContributionPlan, ContributionPlanBundleDetails
from core.constants import CONTRACT_UPDATE_NT, PAYMENT_CREATION_NT
from core.notification_service import create_camu_notification
from core.signals import *
from dateutil.relativedelta import relativedelta
from django.conf import settings
from django.contrib.auth.models import AnonymousUser
from django.core.mail import BadHeaderError, send_mail
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection
from django.db.models.query import Q
from django.forms.models import model_to_dict
from django.utils import timezone
from insuree.models import Insuree, InsureePolicy
from payment.models import Payment, PaymentDetail, PaymentPenaltyAndSanction
from payment.payment_utils import (
    create_paymentcode_openkmfolder,
    payment_code_generation,
)
from payment.services import update_or_create_payment
from policy.models import Policy
from policyholder.models import PolicyHolder, PolicyHolderInsuree

from contract.apps import ContractConfig
from contract.models import Contract as ContractModel
from contract.models import (
    ContractContributionPlanDetails as ContractContributionPlanDetailsModel,
)
from contract.models import ContractDetails as ContractDetailsModel
from contract.models import ContractPolicy
from contract.signals import signal_contract, signal_contract_approve
from contract.utils import get_due_payment_date

from .config import get_message_counter_contract

logger = logging.getLogger("openimis." + __file__)

# Map of department names to codes
DEPARTMENT_CODES = {
    "BOUENZA": "BOA",
    "CUVETTE": "CVT",
    "CUVETTE-OUEST": "CVO",
    "KOUILOU": "KLO",
    "LEKOUMOU": "LKM",
    "LIKOUALA": "LKA",
    "NIARI": "NRI",
    "PLATEAUX": "PTX",
    "POOL": "POL",
    "SANGHA": "SGH",
    "POINTE-NOIRE": "PNR",
    "BRAZZAVILLE": "BZV",
    "DJOUE-LEFINI": "DJL",
    "NKENI-ALIMA": "NKA",
    "CONGO-OUBANGUI": "COB",
}


def generate_contract_code(policy_holder, date):
    # Get department code from policy holder location
    department_code = None
    location = policy_holder.locations
    while location:
        if hasattr(location, "type") and location.type == "R":
            # Normalize location name - remove accents, convert to uppercase
            import unicodedata

            loc_name = "".join(
                c
                for c in unicodedata.normalize("NFD", location.name)
                if unicodedata.category(c) != "Mn"
            ).upper()
            # Find best matching department code
            for dept_name, code in DEPARTMENT_CODES.items():
                dept_name_clean = "".join(
                    c
                    for c in unicodedata.normalize("NFD", dept_name)
                    if unicodedata.category(c) != "Mn"
                )
                if dept_name_clean in loc_name or loc_name in dept_name_clean:
                    department_code = code
                    break
            break
        location = location.parent if hasattr(location, "parent") else None

    if not department_code:
        logger.error(
            f"No valid department (type 'R') found in location hierarchy for policy holder {policy_holder.id}"
        )
        raise ValueError(
            f"Could not find valid department (type 'R') in location hierarchy for policy holder {policy_holder.id}"
        )

    # Get month and year from current date
    month = date.strftime("%m")
    year = date.strftime("%Y")
    # Get increment by checking last contract from same month/year that starts with 'D'
    # Find contracts from same month/year and get highest increment
    contracts_this_month = ContractModel.objects.filter(
        code__startswith="D",
        date_created__month=date.month,
        date_created__year=date.year,
        is_deleted=False,
    ).order_by("-code")

    increment = 1
    if contracts_this_month.exists():
        # Get highest increment from existing contracts this month
        highest_contract = contracts_this_month.first()
        increment = int(highest_contract.code[-6:]) + 1

    # Format the new contract code and check if it exists
    while True:
        new_code = f"D{department_code}{month}{year}{increment:06d}"
        if not ContractModel.objects.filter(code=new_code, is_deleted=False).exists():
            break
        increment += 1
    logger.debug(f"====> Generated new contract code: {new_code}")
    # with connection.cursor() as cursor:
    #    cursor.execute("SELECT nextval('public.contract_code_seq')")
    #    sequence_value = cursor.fetchone()[0]

    # new_code = f"CONT{sequence_value:09}"
    return new_code


class ContractUpdateError(Exception):
    def __init__(self, msg=None):
        self.msg = msg

    def __str__(self):
        return f"ContractUpdateError: {self.msg}"


def check_authentication(function):
    def wrapper(self, *args, **kwargs):
        if type(self.user) is AnonymousUser or not self.user.id:
            return {
                "success": False,
                "message": "Authentication required",
                "detail": "PermissionDenied",
            }
        else:
            result = function(self, *args, **kwargs)
            return result

    return wrapper


class Contract(object):
    def __init__(self, user):
        self.user = user

    @check_authentication
    def create(self, contract):
        from policyholder.gql.gql_mutations.create_mutations import (
            get_and_set_waiting_period_for_insuree,
        )

        try:
            if contract["policy_holder_id"]:
                logger.debug(
                    f"====  Contract : create : policy_holder : {contract['policy_holder_id']}"
                )

                policy_holder = PolicyHolder.objects.filter(
                    id=contract["policy_holder_id"]
                ).first()
                policy_holder_insurees = PolicyHolderInsuree.objects.filter(
                    policy_holder=contract["policy_holder_id"]
                )

                print(
                    f"---------------------------policy_holder_insurees: {policy_holder_insurees}"
                )

                # Remove Income check to create contract
                # for policy_holder_insuree in policy_holder_insurees:
                #     json_ext = policy_holder_insuree.json_ext
                #     if json_ext:
                #         calculation_rule = json_ext.get('calculation_rule')
                #         income = calculation_rule.get('income', )
                #         if income:
                #             if calculation_rule:
                #                 if not income:
                #                     raise Exception("contract creation failed, without income!")
                #             else:
                #                 raise Exception("contract creation failed, without income!")
                #         else:
                #             raise Exception("contract creation failed, without income!")
                #     else:
                #         raise Exception("contract creation failed, without income!")
                sanction_exist = PaymentPenaltyAndSanction.objects.filter(
                    payment__contract__policy_holder=policy_holder,
                    penalty_type="Sanction",
                    status__lt=PaymentPenaltyAndSanction.PENALTY_APPROVED,
                ).first()
                print(
                    f"---------------------------sanction_exist: {sanction_exist}")
                if sanction_exist:
                    logger.debug(
                        f"====  Contract : create : sanction_exist : {sanction_exist.id}"
                    )
                    logger.debug(
                        "====  Contract : create : contract creation failed, Sanction is not approved!"
                    )
                    print(
                        "====  Contract : create : contract creation failed, Sanction is not approved!"
                    )
                    raise Exception(
                        "contract creation failed, Sanction is not approved!"
                    )

            incoming_code = generate_contract_code(
                policy_holder, contract.get("date_valid_from")
            )  # Generate a new unique code
            # Set the generated code into the contract
            contract["code"] = incoming_code
            # if check_unique_code(incoming_code):
            #     raise ValidationError(("Contract code %s already exists" % incoming_code))

            print(f"---------------------------contract: {contract}")

            c = ContractModel(**contract)
            c.state = ContractModel.STATE_DRAFT
            # set the process status to processing
            c.process_status = ContractModel.ProcessStatus.PROCESSING
            c.save(username=self.user.username)
            uuid_string = f"{c.id}"

            print(f"---------------------------c: {c}")
            # check if the PH is set
            if "policy_holder_id" in contract:
                # run services updateFromPHInsuree and Contract Valuation
                cd = ContractDetails(user=self.user)
                result_ph_insuree = cd.update_from_ph_insuree(
                    contract_details={
                        "policy_holder_id": contract["policy_holder_id"],
                        "contract_id": uuid_string,
                        "amendment": 0,
                    }
                )

                # get_and_set_waiting_period_for_insuree
                contract_details_to_update = ContractDetailsModel.objects.filter(
                    contract_id=uuid_string, is_deleted=False
                )
                for contract_detail in contract_details_to_update:
                    logger.info(
                        f"-----------------------------------*****----------- get_and_set_waiting_period_for_insuree: {contract_detail.insuree.id}, {policy_holder.id}"
                    )
                    get_and_set_waiting_period_for_insuree(
                        contract_detail.insuree.id, policy_holder.id
                    )

                # get_and_set_waiting_period_for_insuree

                print(
                    f"---------------------------result_ph_insuree: {result_ph_insuree}"
                )
                total_amount = self.evaluate_contract_valuation(
                    contract_details_result=result_ph_insuree,
                )["total_amount"]
                print(
                    f"---------------------------total_amount: {total_amount}")
                if total_amount is not None and total_amount > 0:
                    if isinstance(total_amount, str):
                        try:
                            print(
                                f"---------------------------total_amount: {total_amount}"
                            )
                            total_amount = float(total_amount)
                        except ValueError:
                            pass  # Keep the original value if it can't be converted to a number
                    rounded_total_amount = round(total_amount)
                    c.amount_notified = rounded_total_amount
                    c.use_bundle_contribution_plan_amount = True

                    contract_details_to_update = ContractDetailsModel.objects.filter(
                        contract_id=uuid_string, is_confirmed=False, is_deleted=False
                    )

                    forfait_total = 0

                    if rounded_total_amount > 0:
                        total_contract_detail = len(contract_details_to_update)
                        forfait_total = rounded_total_amount / total_contract_detail

                    print(
                        f"============================> forfait_total {forfait_total}")

                    for contract_detail in contract_details_to_update:
                        contract_detail.is_confirmed = True

                        contract_detail.json_ext = {
                            "calculation_rule": {"rate": 0, "income": 0},
                            "forfait_rule": {
                                "total": round(forfait_total),
                                "employerContribution": 0,
                                "salaryShare": 0,
                            },
                        }
                        contract_detail.save(username=self.user.username)
                        # logger.info(
                        #     f"-----------------------------------*****----------- get_and_set_waiting_period_for_insuree: {contract_detail.insuree.id}, {policy_holder.id}"
                        # )
                        # get_and_set_waiting_period_for_insuree(
                        #     contract_detail.insuree.id, policy_holder.id
                        # )

            print(f"---------------------------c-1: {c}")
            historical_record = c.history.all().last()
            print(
                f"---------------------------historical_record: {historical_record}")
            c.json_ext = _save_json_external(
                user_id=str(historical_record.user_updated.id),
                datetime=str(historical_record.date_updated),
                message=f"create contract status {historical_record.state}",
            )
            print(f"---------------------------c-2: {c}")
            # set the process status to created supposing that all have been done
            c.process_status = ContractModel.ProcessStatus.CREATED
            c.save(username=self.user.username)
            dict_representation = model_to_dict(c)
            dict_representation["id"], dict_representation["uuid"] = (
                str(uuid_string),
                str(uuid_string),
            )
            print(
                f"---------------------------dict_representation: {dict_representation}"
            )
        except Exception as exc:
            # set the process status to failed_to_create
            c.process_status = ContractModel.ProcessStatus.FAILED_TO_CREATE
            c.save(username=self.user.username)
            return _output_exception(
                model_name="Contract", method="create", exception=exc
            )
        return _output_result_success(dict_representation=dict_representation)

    def evaluate_contract_valuation(self, contract_details_result, save=False):
        print(
            f"---------------------------evaluate_contract_valuation {contract_details_result}"
        )

        ccpd = ContractContributionPlanDetails(
            user=self.user, contract=contract_details_result
        )
        print(
            f"---------------------------contract_details_result: {contract_details_result}"
        )
        result_contract_valuation = ccpd.contract_valuation(
            contract_contribution_plan_details={
                "contract_details": contract_details_result["data"],
                "save": save,
            }
        )
        print(
            f"---------------------------result_contract_valuation: {result_contract_valuation}"
        )
        if (
            not result_contract_valuation
            or result_contract_valuation["success"] is False
        ):
            logger.error("contract valuation failed %s",
                         str(result_contract_valuation))
            print(
                "--------------------------contract valuation failed %s",
                str(result_contract_valuation),
            )
            raise Exception(
                "contract valuation failed " + str(result_contract_valuation)
            )
        return result_contract_valuation["data"]

    # TODO update contract scenario according to wiki page
    @check_authentication
    def update(self, contract):
        try:
            # check rights for contract / amendments
            if not (
                self.user.has_perms(
                    ContractConfig.gql_mutation_update_contract_perms)
                or self.user.has_perms(
                    ContractConfig.gql_mutation_approve_ask_for_change_contract_perms
                )
            ):
                raise PermissionError("Unauthorized")
            updated_contract = ContractModel.objects.filter(
                id=contract["id"]).first()
            # updatable scenario
            if self.__check_rights_by_status(updated_contract.state) == "updatable":
                if "code" in contract:
                    raise ContractUpdateError(
                        "That fields are not editable in that permission!"
                    )
                return _output_result_success(
                    dict_representation=self.__update_contract_fields(
                        contract_input=contract, updated_contract=updated_contract
                    )
                )
            # approvable scenario
            if self.__check_rights_by_status(updated_contract.state) == "approvable":
                # in “Negotiable” changes are possible only with the authority “Approve/ask for change”
                if not self.user.has_perms(
                    ContractConfig.gql_mutation_approve_ask_for_change_contract_perms
                ):
                    raise PermissionError("Unauthorized")
                return _output_result_success(
                    dict_representation=self.__update_contract_fields(
                        contract_input=contract, updated_contract=updated_contract
                    )
                )
            if self.__check_rights_by_status(updated_contract.state) == "cannot_update":
                raise ContractUpdateError("In that state you cannot update!")
        except Exception as exc:
            return _output_exception(
                model_name="Contract", method="update", exception=exc
            )

    def __check_rights_by_status(self, status):
        state = "cannot_update"
        if status in [
            ContractModel.STATE_DRAFT,
            ContractModel.STATE_REQUEST_FOR_INFORMATION,
            ContractModel.STATE_COUNTER,
        ]:
            state = "updatable"
        if status == ContractModel.STATE_NEGOTIABLE:
            state = "approvable"
        return state

    def __update_contract_fields(self, contract_input, updated_contract):
        # get the current policy_holder value
        current_policy_holder_id = updated_contract.policy_holder_id
        [setattr(updated_contract, key, contract_input[key])
         for key in contract_input]
        # check if PH is set and not changed
        if current_policy_holder_id:
            if "policy_holder" in updated_contract.get_dirty_fields(
                check_relationship=True
            ):
                raise ContractUpdateError(
                    "You cannot update already set PolicyHolder in Contract!"
                )
        updated_contract.save(username=self.user.username)
        # save the communication
        historical_record = updated_contract.history.all().first()
        updated_contract.json_ext = _save_json_external(
            user_id=str(historical_record.user_updated.id),
            datetime=str(historical_record.date_updated),
            message="update contract status " + str(historical_record.state),
        )
        updated_contract.save(username=self.user.username)
        try:
            create_camu_notification(CONTRACT_UPDATE_NT, updated_contract)
            logger.info("Sent Notification.")
        except Exception as e:
            logger.error(f"Failed to call send notification: {e}")
        uuid_string = f"{updated_contract.id}"
        dict_representation = model_to_dict(updated_contract)
        dict_representation["id"], dict_representation["uuid"] = (
            str(uuid_string),
            str(uuid_string),
        )
        return dict_representation

    @check_authentication
    def submit(self, contract):
        try:
            # check for submittion right perms/authorites
            if not self.user.has_perms(
                ContractConfig.gql_mutation_submit_contract_perms
            ):
                raise PermissionError("Unauthorized")

            contract_id = f"{contract['id']}"
            contract_to_submit = ContractModel.objects.filter(
                id=contract_id).first()
            contract_details_list = {}
            contract_details_list["data"] = self.__gather_policy_holder_insuree(
                self.__validate_submission(
                    contract_to_submit=contract_to_submit),
                contract_to_submit.amendment,
                contract_date_valid_from=None,
            )
            # contract valuation
            contract_contribution_plan_details = self.evaluate_contract_valuation(
                contract_details_result=contract_details_list,
            )
            ar_amount = contract_contribution_plan_details["total_amount"]
            if ar_amount is not None:
                if isinstance(ar_amount, str):
                    try:
                        ar_amount = float(ar_amount)
                    except ValueError:
                        pass  # Keep the original value if it can't be converted to a number
                rounded_total_amount = round(ar_amount)
                contract_to_submit.amount_rectified = rounded_total_amount
            # send signal
            contract_to_submit.state = ContractModel.STATE_NEGOTIABLE
            signal_contract.send(
                sender=ContractModel, contract=contract_to_submit, user=self.user
            )
            dict_representation = model_to_dict(contract_to_submit)
            dict_representation["id"], dict_representation["uuid"] = (
                contract_id,
                contract_id,
            )
            try:
                create_camu_notification(
                    CONTRACT_UPDATE_NT, contract_to_submit)
                logger.info("Sent Notification.")
            except Exception as e:
                logger.error(f"Failed to call send notification: {e}")
            return _output_result_success(dict_representation=dict_representation)
        except Exception as exc:
            return _output_exception(
                model_name="Contract", method="submit", exception=exc
            )

    def __validate_submission(self, contract_to_submit):
        # check if we have a PolicyHoldes and any ContractDetails
        if not contract_to_submit.policy_holder:
            raise ContractUpdateError(
                "The contract does not contain PolicyHolder!")
        contract_details = ContractDetailsModel.objects.filter(
            contract_id=contract_to_submit.id, is_confirmed=True, is_deleted=False
        )
        if contract_details.count() == 0:
            raise ContractUpdateError(
                "The contract does not contain any insuree!")
        # variable to check if we have right for submit
        state_right = self.__check_rights_by_status(contract_to_submit.state)
        # check if we can submit
        if state_right == "cannot_update":
            raise ContractUpdateError(
                "The contract cannot be submitted because of current state!"
            )
        if state_right == "approvable":
            raise ContractUpdateError(
                "The contract has been already submitted!")
        return list(contract_details.values())

    def __gather_policy_holder_insuree(
        self, contract_details, amendment, contract_date_valid_from=None
    ):
        result = []
        for cd in contract_details:
            ph_insuree = PolicyHolderInsuree.objects.filter(
                Q(insuree_id=cd["insuree_id"], last_policy__isnull=False)
            ).first()
            policy_id = ph_insuree.last_policy.id if ph_insuree else None
            result.append(
                {
                    "id": f"{cd['id']}",
                    "contribution_plan_bundle": f"{cd['contribution_plan_bundle_id']}",
                    "policy_id": policy_id,
                    "json_ext": cd["json_ext"],
                    "contract_date_valid_from": contract_date_valid_from,
                    "insuree_id": cd["insuree_id"],
                    "amendment": amendment,
                }
            )
        return result

    def gather_policy_holder_insuree_2(
        self, contract_details, amendment, contract_date_valid_from=None
    ):
        result = []

        for cd in contract_details:
            ph_insuree = PolicyHolderInsuree.objects.filter(
                Q(insuree_id=cd["insuree_id"], last_policy__isnull=False)
            ).first()
            policy_id = ph_insuree.last_policy.id if ph_insuree else None
            result.append(
                {
                    "id": f"{cd['id']}",
                    "contribution_plan_bundle": f"{cd['contribution_plan_bundle_id']}",
                    "policy_id": policy_id,
                    "json_ext": cd["json_ext"],
                    "contract_date_valid_from": contract_date_valid_from,
                    "insuree_id": cd["insuree_id"],
                    "amendment": amendment,
                }
            )
        return result

    @check_authentication
    def approve(self, contract):
        try:
            logger.info("Approving contract")
            # check for approve/ask for change right perms/authorites
            if not self.user.has_perms(
                ContractConfig.gql_mutation_approve_ask_for_change_contract_perms
            ):
                raise PermissionError("Unauthorized")
            contract_id = f"{contract['id']}"
            logger.info(f"contract {contract['id']}")
            logger.info(
                f"contract service approve : contract_id = {contract_id}")
            contract_to_approve = ContractModel.objects.filter(
                id=contract_id).first()
            logger.info(
                f"contract service approve : contract_to_approve = {contract_to_approve.id}"
            )
            state_right = self.__check_rights_by_status(
                contract_to_approve.state)
            # check if we can submit
            if state_right != "approvable":
                raise ContractUpdateError(
                    "You cannot approve this contract! The status of contract is not Negotiable!"
                )
            contract_details_list = {}
            contract_details_list["contract"] = contract_to_approve
            contract_details_list["data"] = self.__gather_policy_holder_insuree(
                list(
                    ContractDetailsModel.objects.filter(
                        contract_id=contract_to_approve.id,
                        is_confirmed=True,
                        is_deleted=False,
                    ).values()
                ),
                contract_to_approve.amendment,
                contract_to_approve.date_valid_from,
            )
            logger.info(f"contract_details_list {contract_details_list}")
            # Adding previous details in current contract
            prev_contract = (
                ContractModel.objects.filter(
                    policy_holder__id=contract_to_approve.policy_holder.id,
                    is_deleted=False,
                )
                .exclude(id=contract_to_approve.id)
                .order_by("-date_created")
            )
            logger.info(f"prev_contract {prev_contract}")
            if len(prev_contract) > 0:
                logger.info(
                    f"contract service approve : prev_contract = {prev_contract[0]}"
                )
                contract_to_approve.parent = prev_contract[0]
                logger.info(f"contract_to_approve {contract_to_approve}")
            # send signal - approve contract
            ccpd_service = ContractContributionPlanDetails(
                user=self.user, contract=contract_to_approve
            )
            payment_service = PaymentService(user=self.user)
            logger.info("Signal contract approve")
            signal_contract_approve.send(
                sender=ContractModel,
                contract=contract_to_approve,
                user=self.user,
                contract_details_list=contract_details_list,
                service_object=self,
                payment_service=payment_service,
                ccpd_service=ccpd_service,
            )
            logger.info("Sent signal")
            # ccpd.create_contribution(contract_contribution_plan_details)
            dict_representation = {}
            id_contract_approved = f"{contract_to_approve.id}"
            logger.info(
                f"contract service approve : id_contract_approved = {id_contract_approved}"
            )
            dict_representation["id"], dict_representation["uuid"] = (
                id_contract_approved,
                id_contract_approved,
            )
            logger.info("Dict representation")
            payment_due_date = get_due_payment_date(contract_to_approve)
            ContractModel.objects.filter(id=contract_id).update(
                date_payment_due=payment_due_date
            )
            logger.info("Payment due date")
            try:
                create_camu_notification(
                    CONTRACT_UPDATE_NT, contract_to_approve)
                logger.info("Sent Notification.")
            except Exception as e:
                logger.error(f"Failed to call send notification: {e}")
            logger.info("Output result success")
            return _output_result_success(dict_representation=dict_representation)
        except Exception as exc:
            logger.exception("Exception in approve contract")
            return _output_exception(
                model_name="Contract", method="approve", exception=exc
            )

    @check_authentication
    def counter(self, contract):
        try:
            # check for approve/ask for change right perms/authorites
            if not self.user.has_perms(
                ContractConfig.gql_mutation_approve_ask_for_change_contract_perms
            ):
                raise PermissionError("Unauthorized")
            contract_id = f"{contract['id']}"
            contract_to_counter = ContractModel.objects.filter(
                id=contract_id).first()
            # variable to check if we have right to approve
            state_right = self.__check_rights_by_status(
                contract_to_counter.state)
            # check if we can submit
            if state_right != "approvable":
                raise ContractUpdateError(
                    "You cannot counter this contract! The status of contract is not Negotiable!"
                )
            contract_to_counter.state = ContractModel.STATE_COUNTER
            signal_contract.send(
                sender=ContractModel, contract=contract_to_counter, user=self.user
            )
            dict_representation = model_to_dict(contract_to_counter)
            dict_representation["id"], dict_representation["uuid"] = (
                contract_id,
                contract_id,
            )
            email = _send_email_notify_counter(
                code=contract_to_counter.code,
                name=contract_to_counter.policy_holder.trade_name,
                contact_name=contract_to_counter.policy_holder.contact_name,
                email=contract_to_counter.policy_holder.email,
            )
            return _output_result_success(dict_representation=dict_representation)
        except Exception as exc:
            return _output_exception(
                model_name="Contract", method="counter", exception=exc
            )

    @check_authentication
    def amend(self, contract):
        try:
            # check for amend right perms/authorites
            if not self.user.has_perms(
                ContractConfig.gql_mutation_amend_contract_perms
            ):
                raise PermissionError("Unauthorized")
            contract_id = f"{contract['id']}"
            contract_to_amend = ContractModel.objects.filter(
                id=contract_id).first()
            # variable to check if we have right to amend contract
            state_right = self.__check_rights_by_status(
                contract_to_amend.state)
            # check if we can amend
            if (
                state_right != "cannot_update"
                and contract_to_amend.state != ContractModel.STATE_TERMINATED
            ):
                raise ContractUpdateError("You cannot amend this contract!")
            # create copy of the contract
            amended_contract = copy(contract_to_amend)
            amended_contract.id = None
            amended_contract.amendment += 1
            amended_contract.state = ContractModel.STATE_DRAFT
            contract_to_amend.state = ContractModel.STATE_ADDENDUM
            from core import datetime

            contract_to_amend.date_valid_to = datetime.datetime.now()
            # update contract - also copy contract details etc
            contract.pop("id")
            [setattr(amended_contract, key, contract[key]) for key in contract]
            # check if chosen fields are not edited
            if any(
                dirty_field in ["policy_holder", "code", "date_valid_from"]
                for dirty_field in amended_contract.get_dirty_fields(
                    check_relationship=True
                )
            ):
                raise ContractUpdateError(
                    "You cannot update this field during amend contract!"
                )
            signal_contract.send(
                sender=ContractModel, contract=contract_to_amend, user=self.user
            )
            signal_contract.send(
                sender=ContractModel, contract=amended_contract, user=self.user
            )
            # copy also contract details
            self.__copy_details(
                contract_id=contract_id, modified_contract=amended_contract
            )
            # evaluate amended contract amount notified
            contract_details_list = {}
            contract_details_list["data"] = self.__gather_policy_holder_insuree(
                list(
                    ContractDetailsModel.objects.filter(
                        contract_id=amended_contract.id
                    ).values()
                ),
                contract_to_amend.amendment,
            )
            contract_contribution_plan_details = self.evaluate_contract_valuation(
                contract_details_result=contract_details_list, save=False
            )
            amended_contract.amount_notified = contract_contribution_plan_details[
                "total_amount"
            ]
            if "amount_notified" in amended_contract.get_dirty_fields():
                signal_contract.send(
                    sender=ContractModel, contract=amended_contract, user=self.user
                )
            amended_contract_dict = model_to_dict(amended_contract)
            id_new_amended = f"{amended_contract.id}"
            amended_contract_dict["id"], amended_contract_dict["uuid"] = (
                id_new_amended,
                id_new_amended,
            )
            return _output_result_success(dict_representation=amended_contract_dict)
        except Exception as exc:
            return _output_exception(
                model_name="Contract", method="amend", exception=exc
            )

    def __copy_details(self, contract_id, modified_contract):
        list_cd = list(
            ContractDetailsModel.objects.filter(contract_id=contract_id).all()
        )
        for cd in list_cd:
            cd_new = copy(cd)
            cd_new.id = None
            cd_new.contract = modified_contract
            cd_new.save(username=self.user.username)

    @check_authentication
    def renew(self, contract):
        try:
            # check rights for renew contract
            if not self.user.has_perms(
                ContractConfig.gql_mutation_renew_contract_perms
            ):
                raise PermissionError("Unauthorized")
            from core import datetime, datetimedelta

            contract_to_renew = ContractModel.objects.filter(
                id=contract["id"]).first()
            contract_id = contract["id"]
            # block renewing contract not in Updateable or Approvable state
            state_right = self.__check_rights_by_status(
                contract_to_renew.state)
            # check if we can renew
            if (
                state_right != "cannot_update"
                and contract_to_renew.state != ContractModel.STATE_TERMINATED
            ):
                raise ContractUpdateError("You cannot renew this contract!")
            # create copy of the contract - later we also copy contract detail
            renewed_contract = copy(contract_to_renew)
            # TO DO : if a policyholder is set, the contract details must be removed and PHinsuree imported again
            renewed_contract.id = None
            # Date to (the previous contract) became date From of the new contract (TBC if we need to add 1 day)
            # Date To of the new contract is calculated by DateFrom new contract + “Duration in month of previous contract“
            length_contract = (
                contract_to_renew.date_valid_to.year
                - contract_to_renew.date_valid_from.year
            ) * 12 + (
                contract_to_renew.date_valid_to.month
                - contract_to_renew.date_valid_from.month
            )
            renewed_contract.date_valid_from = (
                contract_to_renew.date_valid_to + datetimedelta(days=1)
            )
            renewed_contract.date_valid_to = (
                contract_to_renew.date_valid_to +
                datetimedelta(months=length_contract)
            )
            renewed_contract.state, renewed_contract.version = (
                ContractModel.STATE_DRAFT,
                1,
            )
            renewed_contract.amount_rectified, renewed_contract.amount_due = (
                0, 0)
            renewed_contract.save(username=self.user.username)
            historical_record = renewed_contract.history.all().first()
            renewed_contract.json_ext = _save_json_external(
                user_id=str(historical_record.user_updated.id),
                datetime=str(historical_record.date_updated),
                message=f"contract renewed - state {historical_record.state}",
            )
            renewed_contract.save(username=self.user.username)
            # copy also contract details
            self.__copy_details(
                contract_id=contract_id, modified_contract=renewed_contract
            )
            renewed_contract_dict = model_to_dict(renewed_contract)
            id_new_renewed = f"{renewed_contract.id}"
            renewed_contract_dict["id"], renewed_contract_dict["uuid"] = (
                id_new_renewed,
                id_new_renewed,
            )
            return _output_result_success(dict_representation=renewed_contract_dict)
        except Exception as exc:
            return _output_exception(
                model_name="Contract", method="renew", exception=exc
            )

    @check_authentication
    def delete(self, contract):
        try:
            # check rights for delete contract
            if not self.user.has_perms(
                ContractConfig.gql_mutation_delete_contract_perms
            ):
                raise PermissionError("Unauthorized")
            contract_to_delete = ContractModel.objects.filter(
                id=contract["id"]).first()
            # block deleting contract not in Updateable or Approvable state
            if (
                self.__check_rights_by_status(contract_to_delete.state)
                == "cannot_update"
            ):
                return {
                    "success": False,
                    "message": f"Contract {contract_to_delete.code} cannot be deleted!",
                    "detail": f"{contract_to_delete.code} cannot be deleted because of state {contract_to_delete.state}",
                    "data": "",
                }
            contract_to_delete.delete(username=self.user.username)
            return {
                "success": True,
                "message": "Ok",
                "detail": "",
            }
        except Exception as exc:
            logger.exception("Exception in delete contract")
            return _output_exception(
                model_name="Contract", method="delete", exception=exc
            )

    @check_authentication
    def tipl_contract_evaluation(self, contract):
        logger.info("Starting contract evaluation")

        try:
            temp_contract = self.create(contract)
            logger.info(f"Contract created successfully: {contract}")
        except Exception as e:
            logger.error(f"Error during contract creation: {str(e)}")
            raise

        data = temp_contract.get("data", None)
        if data:
            total_amount = data.get("amount_notified", Decimal(0))
            if total_amount <= 0:
                logger.warning(
                    f"Negative or zero 'amount_notified': {total_amount}")
            else:
                logger.info(f"Positive 'amount_notified': {total_amount}")
        else:
            logger.error("No data returned from contract creation")
            total_amount = Decimal(0)

        logger.debug(f"Total amount notified: {total_amount}")

        try:
            self.delete(data)
            logger.info(f"Contract deleted successfully: {contract}")
        except Exception as e:
            logger.error(f"Error during contract deletion: {str(e)}")
            raise

        logger.info("Contract evaluation completed successfully")
        return total_amount

    @check_authentication
    def terminate_contract(self):
        try:
            # TODO - add this service to the tasks.py in apscheduler once a day
            #  to check if contract might be terminated
            from core import datetime

            contracts_to_terminate = list(
                ContractModel.objects.filter(
                    Q(date_valid_to__lt=datetime.datetime.now(), state=7)
                )
            )
            if len(contracts_to_terminate) > 0:
                for contract in contracts_to_terminate:
                    # we can marked that contract as a terminated
                    contract.state = ContractModel.STATE_TERMINATED
                    contract.save(username=self.user.username)
                    historical_record = contract.history.all().first()
                    contract.json_ext = _save_json_external(
                        user_id=str(historical_record.user_updated.id),
                        datetime=str(historical_record.date_updated),
                        message=f"contract terminated - state "
                        f"{historical_record.state}",
                    )
                    contract.save(username=self.user.username)
                return {
                    "success": True,
                    "message": "Ok",
                    "detail": "",
                }
            else:
                return {
                    "success": False,
                    "message": "No contracts to terminate!",
                    "detail": "We do not have any contract to be terminated!",
                }
        except Exception as exc:
            return _output_exception(
                model_name="Contract", method="terminateContract", exception=exc
            )

    @check_authentication
    def get_negative_amount_amendment(self, credit_note):
        try:
            if not self.user.has_perms(ContractConfig.gql_query_contract_perms):
                raise PermissionError("Unauthorized")

            contract_output_list = []
            payment_detail = (
                PaymentDetail.get_queryset(
                    PaymentDetail.objects.filter(payment__id=credit_note["id"])
                )
                .prefetch_related(
                    "premium__contract_contribution_plan_details__contract_details__contract"
                )
                .prefetch_related("premium__contract_contribution_plan_details")
                .filter(premium__contract_contribution_plan_details__isnull=False)
            )

            if len(list(payment_detail)) > 0:
                contribution_list_id = [pd.premium.id for pd in payment_detail]
                contract_list = ContractModel.objects.filter(
                    contractdetails__contractcontributionplandetails__contribution__id__in=contribution_list_id
                ).distinct()
                for contract in contract_list:
                    # look for approved contract (amendement)
                    if (
                        contract.state
                        in [
                            ContractModel.STATE_EFFECTIVE,
                            ContractModel.STATE_EXECUTABLE,
                        ]
                        and contract.amendment > 0
                    ):
                        # get the contract which has the negative amount due
                        if contract.amount_due < 0:
                            contract_dict = model_to_dict(contract)
                            contract_id = f"{contract.id}"
                            contract_dict["id"], contract_dict["uuid"] = (
                                contract_id,
                                contract_id,
                            )
                            contract_output_list.append(contract_dict)
            # TODO not only get that contracts - but also do another things (it must be specified on wiki page)
            return _output_result_success(dict_representation=contract_output_list)
        except Exception as exc:
            return _output_exception(
                model_name="Contract",
                method="getNegativeAmountAmendment",
                exception=exc,
            )


class ContractDetails(object):
    def __init__(self, user):
        self.user = user

    @check_authentication
    def update_from_ph_insuree(self, contract_details):
        try:
            contract_insuree_list = []
            policy_holder_insuree = PolicyHolderInsuree.objects.filter(
                policy_holder__id=contract_details["policy_holder_id"],
            )
            print("policy_holder_insuree : ", policy_holder_insuree)
            logger.info(
                f"update_from_ph_insuree : policy_holder_insuree : {policy_holder_insuree}"
            )
            if (
                len(policy_holder_insuree) > 0
                and policy_holder_insuree[0].contribution_plan_bundle.periodicity == 12
            ):
                exclude_phi = []
                for phi in policy_holder_insuree:
                    phi_cd = (
                        ContractDetailsModel.objects.filter(
                            insuree_id=phi.insuree.id,
                            contract__policy_holder__id=contract_details[
                                "policy_holder_id"
                            ],
                            contribution_plan_bundle_id=phi.contribution_plan_bundle.id,
                            contract__is_deleted=False,
                            is_deleted=False,
                        )
                        .order_by("insuree__id", "-date_created")
                        .distinct("insuree__id")
                    )
                    print("phi_cd : ", phi_cd)
                    logger.info(f"update_from_ph_insuree : phi_cd : {phi_cd}")

                    if phi_cd and len(phi_cd) > 0:
                        for cd in phi_cd:
                            print("cd : ", cd)
                            logger.info(f"update_from_ph_insuree : cd : {cd}")
                            ccpd = ContractContributionPlanDetailsModel.objects.filter(
                                contract_details_id=cd.id, is_deleted=False
                            ).first()
                            contract = ContractModel.objects.filter(
                                id=contract_details["contract_id"]
                            ).first()

                            desired_start_policy_day = 6
                            try:
                                product_config = (
                                    ccpd.contribution_plan.benefit_plan.config_data
                                )
                            except Exception as e:
                                product_config = None
                            print("product_config : ", product_config)
                            if product_config:
                                last_date_to_create_payment = product_config.get(
                                    "PaymentEndDate", None
                                )
                                if last_date_to_create_payment:
                                    last_date_to_create_payment = datetime.strptime(
                                        last_date_to_create_payment, "%Y-%m-%d"
                                    ).date()
                                    last_date_day_to_create_payment = (
                                        last_date_to_create_payment.day
                                    )
                                    desired_start_policy_day = (
                                        last_date_day_to_create_payment + 1
                                    )

                            desired_month_gap_policy_contract = 4
                            if ccpd.contribution_plan.benefit_plan.policy_waiting_period:
                                desired_month_gap_policy_contract = ccpd.contribution_plan.benefit_plan.policy_waiting_period

                            # last_date_covered is the policy Start date
                            policy_start_date = contract.date_valid_from.date()
                            policy_start_date = policy_start_date.replace(
                                day=desired_start_policy_day
                            )
                            policy_start_date = policy_start_date + relativedelta(
                                months=desired_month_gap_policy_contract
                            )

                            print("policy_start_date : ", policy_start_date)
                            print("ccpd.policy.expiry_date : ",
                                  ccpd.policy.expiry_date)
                            print("ccpd.policy.id : ", ccpd.policy.id)
                            print("phi.insuree.id : ", phi.insuree.id)
                            print("phi.id : ", phi.id)

                            logger.info(
                                f"update_from_ph_insuree : policy_start_date : {policy_start_date}"
                            )
                            logger.info(
                                f"update_from_ph_insuree : ccpd.policy.expiry_date : {ccpd.policy.expiry_date}"
                            )
                            logger.info(
                                f"update_from_ph_insuree : ccpd.policy.id : {ccpd.policy.id}"
                            )
                            logger.info(
                                f"update_from_ph_insuree : phi.insuree.id : {phi.insuree.id}"
                            )
                            logger.info(
                                f"update_from_ph_insuree : phi.id : {phi.id}")

                            if ccpd.policy.expiry_date > policy_start_date:
                                exclude_phi.append(phi.id)

                print("exclude_phi : ", exclude_phi)
                logger.info(
                    f"update_from_ph_insuree : exclude_phi : {exclude_phi}")
                if len(exclude_phi) > 0:
                    policy_holder_insuree = policy_holder_insuree.exclude(
                        id__in=exclude_phi
                    )

            for phi in policy_holder_insuree:
                # TODO add the validity condition also!
                if phi.is_deleted is False and phi.contribution_plan_bundle:
                    cd = ContractDetailsModel(
                        **{
                            "contract_id": contract_details["contract_id"],
                            "insuree_id": phi.insuree.id,
                            "contribution_plan_bundle_id": f"{phi.contribution_plan_bundle.id}",
                            "json_ext": phi.json_ext,
                        }
                    )
                    # TODO add only the caclulation_rule section
                    cd.save(username=self.user.username)
                    uuid_string = f"{cd.id}"
                    dict_representation = model_to_dict(cd)
                    dict_representation["id"], dict_representation["uuid"] = (
                        uuid_string,
                        uuid_string,
                    )
                    dict_representation["policy_id"] = (
                        phi.last_policy.id if phi.last_policy else None
                    )
                    dict_representation["amendment"] = contract_details["amendment"]
                    dict_representation["contract_date_valid_from"] = (
                        cd.contract.date_valid_from
                    )
                    contract_insuree_list.append(dict_representation)
        except Exception as exc:
            return _output_exception(
                model_name="ContractDetails",
                method="updateFromPHInsuree",
                exception=exc,
            )
        return _output_result_success(dict_representation=contract_insuree_list)

    @check_authentication
    def ph_insuree_to_contract_details(self, contract, ph_insuree):
        try:
            phi = PolicyHolderInsuree.objects.get(id=f"{ph_insuree['id']}")
            # check for update contract right perms/authorites
            if not self.user.has_perms(
                ContractConfig.gql_mutation_update_contract_perms
            ):
                raise PermissionError("Unauthorized")
            if phi.is_deleted is False and phi.contribution_plan_bundle:
                updated_contract = ContractModel.objects.get(
                    id=f"{contract['id']}")
                if updated_contract.state not in [
                    ContractModel.STATE_DRAFT,
                    ContractModel.STATE_REQUEST_FOR_INFORMATION,
                    ContractModel.STATE_COUNTER,
                ]:
                    raise ContractUpdateError(
                        "You cannot update contract by adding insuree - contract not in updatable state!"
                    )
                if updated_contract.policy_holder is None:
                    raise ContractUpdateError(
                        "There is no policy holder in contract!")
                cd = ContractDetailsModel(
                    **{
                        "contract_id": contract["id"],
                        "insuree_id": phi.insuree.id,
                        "contribution_plan_bundle_id": f"{phi.contribution_plan_bundle.id}",
                    }
                )
                cd.save(username=self.user.username)
                uuid_string = f"{cd.id}"
                dict_representation = model_to_dict(cd)
                dict_representation["id"], dict_representation["uuid"] = (
                    uuid_string,
                    uuid_string,
                )
                return _output_result_success(dict_representation=dict_representation)
            else:
                raise ContractUpdateError(
                    "You cannot insuree - is deleted or not enough data to create contract!"
                )
        except Exception as exc:
            return _output_exception(
                model_name="ContractDetails",
                method="PHInsureToCDetatils",
                exception=exc,
            )


class ContractContributionPlanDetails(object):
    def __init__(self, user, contract=None):
        self.user = user
        self.contract = contract
        print(f"---------------------------user: {self.user}")
        print(f"---------------------------contract: {self.contract}")

    @check_authentication
    def create_ccpd(self, ccpd, insuree_id):
        """ "
        method to create contract contribution plan details
        """
        print(
            f"---------------------------ContractContributionPlanDetails ccpd: {ccpd}"
        )
        # get the relevant policy from the related product of contribution plan
        # policy objects get all related to this product
        insuree = Insuree.objects.filter(id=insuree_id).first()
        # @TODO : remove policies
        policies = self.__get_policy(
            insuree=insuree,
            date_valid_from=ccpd.date_valid_from,
            date_valid_to=ccpd.date_valid_to,
            product=ccpd.contribution_plan.benefit_plan,
            ccpd=ccpd,
        )
        return self.__create_contribution_from_policy(ccpd, policies)

    def __create_contribution_from_policy(self, ccpd, policies):
        print(
            f"---------------------------ContractContributionPlanDetails policies: {policies}"
        )

        if policies:
            ccpd.policy = policies[0]
            ccpd.save(username=self.user.username)
            return [ccpd]
        # if len(policies) == 1:
        #     ccpd.policy = policies[0]
        #     ccpd.save(username=self.user.username)
        #     return [ccpd]
        # else:
        #     # create second ccpd because another policy was created - copy object and save
        #     ccpd_new = copy(ccpd)
        #     ccpd_new.date_valid_from = ccpd.date_valid_from
        #     ccpd_new.date_valid_to = policies[0].expiry_date
        #     ccpd_new.policy = policies[0]
        #     ccpd.date_valid_from = policies[0].expiry_date
        #     ccpd.date_valid_to = ccpd.date_valid_to
        #     ccpd.policy = policies[1]
        #     ccpd_new.save(username=self.user.username)
        #     ccpd.save(username=self.user.username)
        #     return [ccpd_new, ccpd]

    def __get_policy(self, insuree, date_valid_from, date_valid_to, product, ccpd):
        logger.info(f"__get_policy : date_valid_from : {date_valid_from}")
        logger.info(f"__get_policy : date_valid_to : {date_valid_to}")
        print(
            f"------------------------ ContractContributionPlanDetails : get_policy : date_valid_from : {date_valid_from}"
        )
        from core import datetime

        policy_output = []
        # get all policies related to the product and insuree
        policies = (
            Policy.objects.filter(product=product)
            .filter(family__head_insuree=insuree)
            .filter(start_date__lte=date_valid_to, expiry_date__gte=date_valid_from)
        )
        # get covered policy, use count to run a COUNT query
        if policies.count() > 0:
            policies_covered = list(policies.order_by("start_date"))
        else:
            policies_covered = []
        missing_coverage = []
        # make sure the policies covers the contract :
        last_date_covered = date_valid_from
        # get the start date of the new contract by updating last_date_covered to the policy.stop_date
        while last_date_covered < date_valid_to and len(policies_covered) > 0:
            cur_policy = policies_covered.pop()
            # to check if it does take the first
            if cur_policy.start_date <= last_date_covered:
                # Really unlikely we might create a policy that stop at curPolicy.startDate
                # (start at curPolicy.startDate - product length) and add it to policy_output
                # last_date_covered = cur_policy.expiry_date #commented by ajay for new requirement
                policy_output.append(cur_policy)
            elif cur_policy.expiry_date <= date_valid_to:
                # missing_coverage.append({'start': cur_policy.start_date, 'stop': last_date_covered}) #commented by ajay for new requirement
                # last_date_covered = cur_policy.expiry_date #commented by ajay for new requirement
                policy_output.append(cur_policy)

        for data in missing_coverage:
            print(
                f"----------------------- missing_coverage {missing_coverage}")
            logger.info(f"__get_policy : data['start'] : {data['start']}")
            logger.info(f"__get_policy : data['stop'] : {data['stop']}")
            policy_created, last_date_covered = self.create_contract_details_policies(
                insuree, product, data["start"], data["stop"], ccpd
            )
            if policy_created is not None and len(policy_created) > 0:
                policy_output += policy_created

        # now we create new policy
        # @Note: Code commented temporary for CAMU Requirement
        while last_date_covered < date_valid_to:
            print(
                f"----------------------- last_date_covered {last_date_covered}")
            print(f"----------------------- date_valid_to {date_valid_to}")
            logger.info(
                f"__get_policy : last_date_covered : {last_date_covered}")
            logger.info(f"__get_policy : date_valid_to : {date_valid_to}")
            policy_created, last_date_covered = self.create_contract_details_policies(
                insuree, product, last_date_covered, date_valid_to, ccpd
            )
            if policy_created is not None and len(policy_created) > 0:
                policy_output += policy_created
        return policy_output

    def create_contract_details_policies(
        self, insuree, product, last_date_covered, date_valid_to, ccpd
    ):
        # create policy for insuree family
        # TODO Policy with status - new open=32 in policy-be_py module
        logger.info(
            "create_contract_details_policies : --------- Start ---------")
        policy_output = []
        print(
            f"------------------------ ContractContributionPlanDetails : create_contract_details_policies : last_date_covered : {last_date_covered}"
        )

        # Get the contract
        policy_holder = ccpd.contract_details.contract.policy_holder
        has_other_contracts = policy_holder.contract_set.exclude(
            id=ccpd.contract_details.contract.id
        ).exists()

        logger.info(
            f"create_contract_details_policies : product.insurance_period : {product.insurance_period}"
        )
        logger.info(
            f"create_contract_details_policies : contract.parent : {ccpd.contract_details.contract.parent}"
        )
        logger.info(
            f"create_contract_details_policies : BU : last_date_covered : {last_date_covered}"
        )

        print("product.insurance_period : ", product.insurance_period)
        print("contract.parent : ", ccpd.contract_details.contract.parent)
        print("BU : last_date_covered : ", last_date_covered)

        while last_date_covered < date_valid_to:
            expiry_date = last_date_covered + relativedelta(
                months=product.insurance_period
            )
            product_config = product.config_data
            # Changing start date and end date of policy with insurance period 1 as per CAMU Requirement
            if product.insurance_period == 1:
                # desired_start_policy_day is a policy start day in month
                desired_start_policy_day = 6
                if product_config:
                    last_date_to_create_payment = product_config.get(
                        "PaymentEndDate", None
                    )
                    if last_date_to_create_payment:
                        last_date_to_create_payment = datetime.strptime(
                            last_date_to_create_payment, "%Y-%m-%d"
                        ).date()
                        last_date_day_to_create_payment = (
                            last_date_to_create_payment.day
                        )
                        desired_start_policy_day = last_date_day_to_create_payment + 1

                # desired_month_gap_policy_contract is a gap of policy from contract
                # desired_month_gap_policy_contract = 2
                desired_month_gap_policy_contract = 1
                # if product.policy_waiting_period:
                #     desired_month_gap_policy_contract = product.policy_waiting_period

                # last_date_covered is the policy Start date
                last_date_covered = last_date_covered.replace(
                    day=desired_start_policy_day
                )
                last_date_covered = last_date_covered + relativedelta(
                    months=desired_month_gap_policy_contract
                )

                # expiry_date is the policy End date
                expiry_date = last_date_covered + relativedelta(
                    months=product.insurance_period
                )
                expiry_date = expiry_date.replace(
                    day=desired_start_policy_day - 1)

            # Changing start date and end date of policy with insurance period 3 as per CAMU Requirement
            if product.insurance_period == 3:
                # desired_start_policy_day is a policy start day in month
                desired_start_policy_day = 6
                if product_config:
                    last_date_to_create_payment = product_config.get(
                        "PaymentEndDate", None
                    )
                    if last_date_to_create_payment:
                        last_date_to_create_payment = datetime.strptime(
                            last_date_to_create_payment, "%Y-%m-%d"
                        ).date()
                        last_date_day_to_create_payment = (
                            last_date_to_create_payment.day
                        )
                        desired_start_policy_day = last_date_day_to_create_payment + 1
                if ccpd.contract_details.contract.parent:
                    # desired_month_gap_policy_contract is a gap of policy from contract
                    desired_month_gap_policy_contract = 1
                    # if product.policy_waiting_period:
                    #     desired_month_gap_policy_contract = product.policy_waiting_period

                    # last_date_covered is the policy Start date
                    last_date_covered = last_date_covered.replace(
                        day=desired_start_policy_day
                    )
                    last_date_covered = last_date_covered + relativedelta(
                        months=desired_month_gap_policy_contract
                    )

                    # expiry_date is the policy End date
                    expiry_date = last_date_covered + relativedelta(
                        months=product.insurance_period + 1
                    )
                    expiry_date = expiry_date.replace(
                        day=desired_start_policy_day - 1)
                else:
                    # desired_month_gap_policy_contract is a gap of policy from contract
                    desired_month_gap_policy_contract = 3
                    if product.policy_waiting_period:
                        desired_month_gap_policy_contract = (
                            product.policy_waiting_period
                        )

                    # last_date_covered is the policy Start date
                    last_date_covered = last_date_covered.replace(
                        day=desired_start_policy_day
                    )
                    last_date_covered = last_date_covered + relativedelta(
                        months=desired_month_gap_policy_contract
                    )

                    # expiry_date is the policy End date
                    expiry_date = last_date_covered + relativedelta(
                        months=product.insurance_period
                    )
                    expiry_date = expiry_date.replace(
                        day=desired_start_policy_day - 1)

            if product.insurance_period == 12:
                desired_start_policy_day = 6
                if product_config:
                    last_date_to_create_payment = product_config.get(
                        "PaymentEndDate", None
                    )
                    if last_date_to_create_payment:
                        last_date_to_create_payment = datetime.strptime(
                            last_date_to_create_payment, "%Y-%m-%d"
                        ).date()
                        last_date_day_to_create_payment = (
                            last_date_to_create_payment.day
                        )
                        desired_start_policy_day = last_date_day_to_create_payment + 1

                desired_month_gap_policy_contract = 4
                if product.policy_waiting_period:
                    desired_month_gap_policy_contract = product.policy_waiting_period

                print("desired_start_policy_day : ", desired_start_policy_day)
                # last_date_covered is the policy Start date

                # last_date_covered = last_date_covered.replace(
                # day=desired_start_policy_day)
                # last_date_covered = last_date_covered + relativedelta(
                # months=desired_month_gap_policy_contract)

                # last_date_covered = last_date_covered.replace(
                # day=desired_start_policy_day)
                last_date_covered = last_date_covered + relativedelta(
                    months=desired_month_gap_policy_contract
                )

                if has_other_contracts:
                    months_to_substract = desired_month_gap_policy_contract - 6
                    last_date_covered = last_date_covered + relativedelta(
                        months=months_to_substract
                    )

                last_date_covered = last_date_covered.replace(
                    day=desired_start_policy_day
                )
                print("last_date_covered : ", last_date_covered)
                # expiry_date is the policy End date

                expiry_date = last_date_covered + relativedelta(
                    months=product.insurance_period
                )

                # if the contract is the 1st one it should take 12 months - 3
                # if the contract has parent then it should take 12 months \
                # to substract
                # if has_other_contracts:
                #     # months_to_substract = product.insurance_period - 3
                #     expiry_date = last_date_covered + relativedelta(
                # months=months_to_substract)

                expiry_date = expiry_date.replace(
                    day=desired_start_policy_day - 1)

                last_date_covered = self.__handle_twelve_month_first_policy(
                    insuree, last_date_covered
                )

                print("=======>last_date_covered : ", last_date_covered)
                print("=======>expiry_date : ", expiry_date)

            logger.info(
                f"create_contract_details_policies : AU : last_date_covered : {last_date_covered}"
            )
            logger.info(
                f"create_contract_details_policies : expiry_date : {expiry_date}"
            )

            print("AU : last_date_covered : ", last_date_covered)
            print("expiry_date : ", expiry_date)

            # policy_status = self._get_policy_status(insuree, policy_holder)

            # logger.info(f"=====> create_contract_details_policies : policy_status : {policy_status}")

            cur_policy = Policy.objects.create(
                **{
                    "family": insuree.family,
                    "is_valid": False,
                    "product": product,
                    "status": Policy.STATUS_LOCKED,
                    "stage": Policy.STAGE_NEW,
                    "enroll_date": last_date_covered,
                    "start_date": last_date_covered,
                    "validity_from": last_date_covered,
                    "effective_date": last_date_covered,
                    "expiry_date": expiry_date,
                    "validity_to": None,
                    "audit_user_id": -1,
                }
            )
            last_date_covered = expiry_date
            policy_output.append(cur_policy)

            logger.info(f"=======++++++++++++ self.contract {self.contract}")

            ContractPolicy.objects.create(
                contract=self.contract["contract"],
                policy=cur_policy,
                insuree=insuree,
                policy_holder=policy_holder,
            )

        logger.info(
            "create_contract_details_policies : --------- End ---------")
        return policy_output, last_date_covered

    def __handle_twelve_month_first_policy(self, insuree, last_date_covered):
        check_already_insuree_policy = Policy.objects.filter(
            family=insuree.family
        ).first()

        if check_already_insuree_policy:
            return last_date_covered

        return last_date_covered + relativedelta(months=3)

    @check_authentication
    def contract_valuation(self, contract_contribution_plan_details):
        try:
            print(
                f"------------------------ ContractContributionPlanDetails : contract_valuation : contract_contribution_plan_details : {contract_contribution_plan_details}"
            )
            logger.info("contract_valuation : --------- Start ---------")
            dict_representation = {}
            ccpd_list = []
            total_amount = 0
            amendment = 0
            for contract_details in contract_contribution_plan_details[
                "contract_details"
            ]:
                logger.info(
                    f"contract_valuation : contract_details : {contract_details}"
                )
                cpbd_list = ContributionPlanBundleDetails.objects.filter(
                    contribution_plan_bundle__id=str(
                        contract_details["contribution_plan_bundle"]
                    ),
                    is_deleted=False,
                )
                logger.info(f"contract_valuation : cpbd_list : {cpbd_list}")
                amendment = contract_details["amendment"]
                logger.info(f"contract_valuation : amendment : {amendment}")
                for cpbd in cpbd_list:
                    logger.info(f"contract_valuation : cpbd : {cpbd}")
                    logger.info(
                        f"contract_valuation : contract_details['id'] : {contract_details['id']}"
                    )
                    logger.info(
                        f"contract_valuation : cpbd.contribution_plan.id : {cpbd.contribution_plan.id}"
                    )
                    logger.info(
                        f"contract_valuation : contract_details['policy_id'] : {contract_details['policy_id']}"
                    )
                    ccpd = ContractContributionPlanDetailsModel(
                        **{
                            "contract_details_id": contract_details["id"],
                            "contribution_plan_id": f"{cpbd.contribution_plan.id}",
                            "policy_id": contract_details["policy_id"],
                        }
                    )
                    print(
                        f"******--------------------------------- ccpd {ccpd}")
                    logger.info(f"contract_valuation : ccpd : {ccpd}")
                    # rc - result of calculation
                    calculated_amount = 0
                    print(
                        f"------------------------ ContractContributionPlanDetails : calculated_amount: {calculated_amount}"
                    )
                    print("-------------------- ccpd DATA ----------------------")
                    print(ccpd)
                    print("-------------------- ccpd DATA ----------------------")

                    print("-------------------- self.user DATA ----------------------")
                    print(self.user)
                    print("-------------------- self.user DATA ----------------------")
                    rc = run_calculation_rules(ccpd, "create", self.user)
                    print(
                        f"------------------------ ContractContributionPlanDetails : rc: {rc}"
                    )
                    logger.info(f"contract_valuation : rc : {rc}")
                    if rc:
                        calculated_amount = (
                            rc[0][1] if rc[0][1] not in [None, False] else 0
                        )
                        total_amount = float(total_amount)
                        total_amount += float(calculated_amount)
                        logger.info(
                            f"contract_valuation : calculated_amount : {calculated_amount}"
                        )
                        logger.info(
                            f"contract_valuation : calculated_amount : {total_amount}"
                        )

                    ccpd_record = model_to_dict(ccpd)
                    logger.info(
                        f"contract_valuation : ccpd_record : {ccpd_record}")
                    ccpd_record["calculated_amount"] = calculated_amount
                    print(
                        f"***------------------ contract_details {contract_details}")
                    if contract_contribution_plan_details["save"]:
                        ccpd_list, total_amount, ccpd_record = (
                            self.__append_contract_cpd_to_list(
                                ccpd=ccpd,
                                cp=cpbd.contribution_plan,
                                date_valid_from=contract_details[
                                    "contract_date_valid_from"
                                ],
                                insuree_id=contract_details["insuree_id"],
                                total_amount=total_amount,
                                calculated_amount=calculated_amount,
                                ccpd_list=ccpd_list,
                                ccpd_record=ccpd_record,
                            )
                        )
                    if "id" not in ccpd_record:
                        ccpd_list.append(ccpd_record)
            if amendment > 0:
                amendment = float(amendment)
                # get the payment from the previous version of the contract
                contract_detail_id = contract_contribution_plan_details[
                    "contract_details"
                ][0]["id"]
                cd = ContractDetailsModel.objects.get(id=contract_detail_id)
                contract_previous = ContractModel.objects.filter(
                    Q(amendment=amendment - 1, code=cd.contract.code)
                ).first()
                premium = (
                    ContractContributionPlanDetailsModel.objects.filter(
                        contract_details__contract__id=f"{contract_previous.id}"
                    )
                    .first()
                    .contribution
                )
                payment_detail_contribution = PaymentDetail.objects.filter(
                    premium=premium
                ).first()
                payment_id = payment_detail_contribution.payment.id
                payment_object = Payment.objects.get(id=payment_id)
                received_amount = (
                    payment_object.received_amount
                    if payment_object.received_amount
                    else 0
                )
                total_amount = float(total_amount) - float(received_amount)
            dict_representation["total_amount"] = total_amount
            dict_representation["contribution_plan_details"] = ccpd_list
            logger.info(f"contract_valuation : total_amount : {total_amount}")
            logger.info(f"contract_valuation : ccpd_list : {ccpd_list}")
            logger.info("contract_valuation : --------- End ---------")
            return _output_result_success(dict_representation=dict_representation)
        except Exception as exc:
            return _output_exception(
                model_name="ContractContributionPlanDetails",
                method="contractValuation",
                exception=exc,
            )

    def __append_contract_cpd_to_list(
        self,
        ccpd,
        cp,
        date_valid_from,
        insuree_id,
        total_amount,
        calculated_amount,
        ccpd_list,
        ccpd_record,
    ):
        """helper private function to gather results to the list
        ccpd - contract contribution plan details
        cp - contribution plan
        return ccpd list and total amount
        """
        print(
            f"------------------------ ContractContributionPlanDetails : __append_contract_cpd_to_list : ccpd : {ccpd}"
        )
        print(
            f"------------------------ ContractContributionPlanDetails : __append_contract_cpd_to_list : cp : {cp}"
        )
        print(
            f"------------------------ ContractContributionPlanDetails : __append_contract_cpd_to_list : date_valid_from : {date_valid_from}"
        )
        print(
            f"------------------------ ContractContributionPlanDetails : __append_contract_cpd_to_list : insuree_id : {insuree_id}"
        )
        print(
            f"------------------------ ContractContributionPlanDetails : __append_contract_cpd_to_list : total_amount : {total_amount}"
        )
        print(
            f"------------------------ ContractContributionPlanDetails : __append_contract_cpd_to_list : calculated_amount : {calculated_amount}"
        )
        logger.info("__append_contract_cpd_to_list : --------- Start ---------")
        from core import datetime, datetimedelta

        # TODO - catch grace period from calculation rule if is defined
        #  grace_period = cp.calculation_rule etc
        #  length = cp.get_contribution_length(grace_period)
        length = cp.get_contribution_length()
        ccpd.date_valid_from = date_valid_from

        # get the last day of the month data_valid_from and transform it to date_valid_to eg if data_valid_from is 01 feb 2025 then date_valid_to should be 28 feb 2025
        last_day = calendar.monthrange(
            date_valid_from.year, date_valid_from.month)[1]
        ccpd.date_valid_to = date_valid_from.replace(day=last_day)
        # ccpd.date_valid_to = date_valid_from + datetimedelta(months=length)
        # TODO: calculate the number of CCPD to create in order to cover the contract length
        ccpd_results = self.create_ccpd(ccpd, insuree_id)
        ccpd_record = model_to_dict(ccpd)
        ccpd_record["calculated_amount"] = calculated_amount
        # TODO: support more that 2 CCPD
        # case 1 - single contribution
        if len(ccpd_results) == 1:
            uuid_string = f"{ccpd_results[0].id}"
            ccpd_record["id"], ccpd_record["uuid"] = (uuid_string, uuid_string)
            ccpd_list.append(ccpd_record)
        # case 2 - 2 contributions with 2 policies
        else:
            # there is additional contribution - we have to calculate/recalculate
            total_amount = float(total_amount) - float(calculated_amount)
            logger.info(
                f"__append_contract_cpd_to_list : total_amount = {total_amount}"
            )
            for ccpd_result in ccpd_results:
                logger.info(
                    f"__append_contract_cpd_to_list : ccpd_result = {ccpd_result}"
                )
                length_ccpd = float(
                    (ccpd_result.date_valid_to.year -
                     ccpd_result.date_valid_from.year)
                    * 12
                    + (
                        ccpd_result.date_valid_to.month
                        - ccpd_result.date_valid_from.month
                    )
                )
                logger.info(
                    f"__append_contract_cpd_to_list : length_ccpd = {length_ccpd}"
                )
                periodicity = float(ccpd_result.contribution_plan.periodicity)
                logger.info(
                    f"__append_contract_cpd_to_list : periodicity = {periodicity}"
                )
                # time part of split as a fraction to count contribution value for that split period properly
                part_time_period = length_ccpd / periodicity
                logger.info(
                    f"__append_contract_cpd_to_list : part_time_period = {part_time_period}"
                )
                # rc - result calculation
                rc = run_calculation_rules(ccpd, "update", self.user)
                if rc:
                    logger.info(
                        f"__append_contract_cpd_to_list : run_calculation_rules = {rc}"
                    )
                    calculated_amount = (
                        float(rc[0][1]) * float(part_time_period)
                        if rc[0][1] not in [None, False]
                        else 0
                    )
                    total_amount = float(total_amount)
                    total_amount += calculated_amount
                    logger.info(
                        f"__append_contract_cpd_to_list : for calculated_amount = {calculated_amount}"
                    )
                    logger.info(
                        f"__append_contract_cpd_to_list : for total_amount = {total_amount}"
                    )
                ccpd_record = model_to_dict(ccpd_result)
                ccpd_record["calculated_amount"] = calculated_amount
                uuid_string = f"{ccpd_result.id}"
                ccpd_record["id"], ccpd_record["uuid"] = (
                    uuid_string, uuid_string)
                ccpd_list.append(ccpd_record)
        logger.info("__append_contract_cpd_to_list : --------- End ---------")
        return ccpd_list, total_amount, ccpd_record

    @check_authentication
    def create_contribution(self, contract_contribution_plan_details):
        try:
            print(
                f"------------------------ ContractContributionPlanDetails : create_contribution : contract_contribution_plan_details : {contract_contribution_plan_details}"
            )
            dict_representation = {}
            contribution_list = []
            from core import datetime

            now = datetime.datetime.now()
            for ccpd in contract_contribution_plan_details["contribution_plan_details"]:
                contract_details = ContractDetailsModel.objects.get(
                    id=f"{ccpd['contract_details']}"
                )
                # create the contributions based on the ContractContributionPlanDetails
                if ccpd["contribution"] is None:
                    contribution = Premium.objects.create(
                        **{
                            "policy_id": ccpd["policy"],
                            "amount": ccpd["calculated_amount"],
                            "audit_user_id": -1,
                            "pay_date": now,
                            # TODO Temporary value pay_type - I have to get to know about this field what should be here
                            #  also ask about audit_user_id and pay_date value
                            "pay_type": " ",
                        }
                    )
                    ccpd_object = ContractContributionPlanDetailsModel.objects.get(
                        id=ccpd["id"]
                    )
                    ccpd_object.contribution = contribution
                    ccpd_object.save(username=self.user.username)
                    contribution_record = model_to_dict(contribution)
                    contribution_list.append(contribution_record)
                    dict_representation["contributions"] = contribution_list
            return _output_result_success(dict_representation=dict_representation)
        except Exception as exc:
            return _output_exception(
                model_name="ContractContributionPlanDetails",
                method="createContribution",
                exception=exc,
            )


# This function is used in payment module
def get_policy_status(insuree, policy_holder):
    from policyholder.models import PolicyHolderContributionPlan

    from contract.models import InsureeWaitingPeriod

    logger.info("get_policy_status : --------- Start ---------")

    try:
        logger.info(f"get_policy_status : policy_holder : {policy_holder}")
        policy_holder_contribution_plan = PolicyHolderContributionPlan.objects.filter(
            policy_holder_id=policy_holder.id, is_deleted=False
        ).first()

        logger.info(
            f"get_policy_status : policy_holder_contribution_plan : {policy_holder_contribution_plan}"
        )

        insuree_waiting_period = InsureeWaitingPeriod.objects.filter(
            insuree=insuree,
            policy_holder_contribution_plan=policy_holder_contribution_plan,
        ).first()

        logger.info(
            f"get_policy_status : insuree_waiting_period : {insuree_waiting_period}"
        )

        if not insuree_waiting_period:
            logger.info(
                f"get_policy_status : insuree_waiting_period : {insuree_waiting_period}"
            )
            return Policy.STATUS_LOCKED

        # policy_status = Policy.STATUS_LOCKED

        waiting_period = insuree_waiting_period.waiting_period
        periodicity = insuree_waiting_period.contribution_periodicity

        if periodicity == 1 or periodicity == 3:
            if waiting_period > 0:
                waiting_period = waiting_period - periodicity

        if periodicity == 12:
            waiting_period = 0

        logger.info(
            f"**************get_policy_status : waiting_period : {waiting_period}"
        )

        InsureeWaitingPeriod.objects.filter(id=insuree_waiting_period.id).update(
            waiting_period=waiting_period
        )

        logger.info(f"get_policy_status : waiting_period : {waiting_period}")

        if waiting_period == 0:
            logger.info(
                f"get_policy_status : waiting_period : {waiting_period}")
            return Policy.STATUS_READY
        else:
            logger.info(
                f"get_policy_status : waiting_period : {waiting_period}")
            return Policy.STATUS_LOCKED
    except Exception as e:
        logger.error(f"Error getting policy status: {e}")
        return Policy.STATUS_LOCKED


class PaymentService(object):
    def __init__(self, user):
        self.user = user

    @check_authentication
    def create(self, payment, payment_details=None):
        try:
            dict_representation = {}
            payment_list = []
            from core import datetime

            now = datetime.datetime.now()
            p = update_or_create_payment(data=payment, user=self.user)
            try:
                if payment_details and len(payment_details) > 0:
                    payment_code = payment_code_generation(
                        payment_details[0]["premium"]
                    )
                    p.payment_code = payment_code
                    print(p.payment_code)
                    p.save()
                    try:
                        create_paymentcode_openkmfolder(payment_code, p)
                    except Exception as e:
                        pass
                    try:
                        create_camu_notification(PAYMENT_CREATION_NT, p)
                        logger.info("Sent Notification.")
                    except Exception as e:
                        logger.error(f"Failed to call send notification: {e}")
            except Exception as e:
                logger.exception("Payment code generation or saving failed")
            dict_representation = model_to_dict(p)
            dict_representation["id"], dict_representation["uuid"] = (
                p.id, p.uuid)
            if payment_details:
                for payment_detail in payment_details:
                    pd = PaymentDetail.objects.create(
                        payment=Payment.objects.get(id=p.id),
                        audit_user_id=-1,
                        validity_from=now,
                        product_code=payment_detail["product_code"],
                        insurance_number=payment_detail["insurance_number"],
                        expected_amount=payment_detail["expected_amount"],
                        premium=payment_detail["premium"],
                    )
                    pd_record = model_to_dict(pd)
                    pd_record["id"] = pd.id
                    payment_list.append(pd_record)
            dict_representation["payment_details"] = payment_list
            return _output_result_success(dict_representation=dict_representation)
        except Exception as exc:
            logger.exception("Payment.createPayment failed")
            return _output_exception(
                model_name="Payment", method="createPayment", exception=exc
            )

    @check_authentication
    def collect_payment_details(self, contract_contribution_plan_details):
        payment_details_data = []
        for ccpd in contract_contribution_plan_details:
            product_code = ContributionPlan.objects.get(
                id=ccpd["contribution_plan"]
            ).benefit_plan.code
            insurance_number = ContractDetailsModel.objects.get(
                id=ccpd["contract_details"]
            ).insuree.chf_id
            contribution = ContractContributionPlanDetailsModel.objects.get(
                id=ccpd["id"]
            ).contribution
            expected_amount = ccpd["calculated_amount"]
            payment_details_data.append(
                {
                    "product_code": product_code,
                    "insurance_number": insurance_number,
                    "expected_amount": expected_amount,
                    "premium": contribution,
                }
            )
        return payment_details_data


class ContractToInvoiceService(object):
    def __init__(self, user):
        self.user = user

    @classmethod
    @register_service_signal("create_invoice_from_contract")
    def create_invoice(self, instance, convert_to="Invoice", **kwargs):
        """run convert the ContractContributionPlanDetails of the contract to invoice lines"""
        pass


def _output_exception(model_name, method, exception):
    return {
        "success": False,
        "message": f"Failed to {method} {model_name}",
        "detail": f"{exception}",
        "data": "",
    }


def _output_result_success(dict_representation):
    return {
        "success": True,
        "message": "Ok",
        "detail": "",
        "data": json.loads(json.dumps(dict_representation, cls=DjangoJSONEncoder)),
    }


def _save_json_external(user_id, datetime, message):
    return {
        "comments": [
            {"From": "Portal/webapp", "user": user_id,
                "date": datetime, "msg": message}
        ]
    }


def _send_email_notify_counter(code, name, contact_name, email):
    try:
        email_to_send = send_mail(
            subject="Contract counter notification",
            message=get_message_counter_contract(
                language=settings.LANGUAGE_CODE.split("-")[0],
                code=code,
                name=name,
                contact_name=contact_name,
            ),
            from_email=settings.EMAIL_HOST_USER,
            recipient_list=[email],
            fail_silently=False,
        )
        return email_to_send
    except BadHeaderError:
        return ValueError("Invalid header found.")


def check_unique_code(code):
    if ContractModel.objects.filter(code=code, is_deleted=False).exists():
        return [{"message": "Contract code %s already exists" % code}]
    return []
