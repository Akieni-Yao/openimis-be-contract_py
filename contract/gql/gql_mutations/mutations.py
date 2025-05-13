import logging

from core import TimeUtils
from core.constants import CONTRACT_CREATION_NT
from core.gql.gql_mutations import ObjectNotExistException
from core.notification_service import create_camu_notification
from core.schema import OpenIMISMutation
from django.contrib.auth.models import AnonymousUser
from django.core.exceptions import ValidationError
from rest_framework.response import Response

from contract.apps import ContractConfig
from contract.erp_integrations import erp_submit_contract
from contract.models import Contract, ContractDetails, ContractMutation
from contract.services import Contract as ContractService
from contract.services import ContractDetails as ContractDetailsService
from contract.services import ContractToInvoiceService
from contract.tasks import create_contract_async

# from contract.views import re_evaluate_contract_details
from contract.views import re_evaluate_contract_details, update_salary

logger = logging.getLogger(__name__)


class ContractCreateMutationMixin:
    @property
    def _model(self):
        raise NotImplementedError()

    @classmethod
    def _validate_mutation(cls, user, **data):
        if (
            type(user) is AnonymousUser
            or not user.id
            or not user.has_perms(ContractConfig.gql_mutation_create_contract_perms)
        ):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        client_mutation_id = data.get("client_mutation_id")
        if "client_mutation_id" in data:
            data.pop("client_mutation_id")
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")

        # Start the async task
        create_contract_async.delay(
            user_id=user.id,
            contract_data=data,
            client_mutation_id=client_mutation_id
        )

        # Return task ID for tracking
        return Response({
            "success": True,
            "message": "Contract creation started"
        })

    @classmethod
    def create_contract(cls, user, contract):
        contract_service = ContractService(user=user)
        output_data = contract_service.create(contract=contract)
        return output_data


class ContractUpdateMutationMixin:
    @property
    def _model(self):
        raise NotImplementedError()

    @classmethod
    def _validate_mutation(cls, user, **data):
        if (
            type(user) is AnonymousUser
            or not user.id
            or not user.has_perms(ContractConfig.gql_mutation_update_contract_perms)
        ):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop("client_mutation_id")
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")
        output = cls.update_contract(user=user, contract=data)
        return (
            None
            if output["success"]
            else f"Error! - {output['message']}: {output['detail']}"
        )

    @classmethod
    def update_contract(cls, user, contract):
        contract_service = ContractService(user=user)
        output_data = contract_service.update(contract=contract)
        return output_data


class ContractDetailsUpdateMutationMixin:
    @property
    def _model(self):
        raise NotImplementedError()

    @classmethod
    def _validate_mutation(cls, user, **data):
        if (
            type(user) is AnonymousUser
            or not user.id
            or not user.has_perms(ContractConfig.gql_mutation_update_contract_perms)
        ):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _update_salary(cls, contract_details, value):
        logger.info(
            f"================= ContractDetailsUpdateMutationMixin contract_details: {contract_details}"
        )
        if not contract_details.json_ext:
            return value

        current_income = contract_details.json_ext.get("calculation_rule", {}).get(
            "income"
        )
        logger.info(
            f"================= ContractDetailsUpdateMutationMixin current_income: {current_income}"
        )
        new_income = value.get("calculation_rule", {}).get("income")
        logger.info(
            f"================= ContractDetailsUpdateMutationMixin new_income: {new_income}"
        )
        if current_income != new_income:
            logger.info(
                f"================= ContractDetailsUpdateMutationMixin current_income: {current_income} new_income: {new_income}"
            )
            return update_salary(contract_details.json_ext, new_income)
        return None

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop("client_mutation_id")
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")

        contract_details = ContractDetails.objects.filter(
            id=data["id"],
            contract_id=data["contract_id"],
            insuree_id=data["insuree_id"],
            contribution_plan_bundle_id=data["contribution_plan_bundle_id"],
        ).first()

        logger.info(
            f"================= ContractDetailsUpdateMutationMixin contract_details: {contract_details}"
        )

        try:
            if not contract_details:
                logger.info(
                    "================= ContractDetailsUpdateMutationMixin contract_details not found"
                )
                return "Error! - Contract details not found"
            for key, value in data.items():
                if key == "is_confirmed":
                    contract_details.is_confirmed = value
                elif key == "is_new_insuree":
                    contract_details.is_new_insuree = value
                elif key == "json_param":
                    contract_details.json_param = value
                elif key == "json_ext":
                    new_json_ext = cls._update_salary(contract_details, value)
                    if new_json_ext:
                        contract_details.json_ext = new_json_ext

            logger.info(
                f"================= ContractDetailsUpdateMutationMixin contract_details: {contract_details}"
            )

            contract_details.save(username=user.username)
            logger.info(
                "================= ContractDetailsUpdateMutationMixin contract_details saved"
            )
            re_evaluate_contract_details(
                data["contract_id"], user, user.username)
            return None
        except Exception as e:
            logger.error(f"Error updating contract details: {e}")
            return f"Error updating contract details {e}"


class ContractDetailsCreateMutationMixin:
    @property
    def _model(self):
        raise NotImplementedError()

    @classmethod
    def _validate_mutation(cls, user, **data):
        if (
            type(user) is AnonymousUser
            or not user.id
            or not user.has_perms(ContractConfig.gql_mutation_update_contract_perms)
        ):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop("client_mutation_id")
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")

        contract_details = ContractDetails.objects.filter(
            contract_id=data["contract_id"],
            insuree_id=data["insuree_id"],
            contribution_plan_bundle_id=data["contribution_plan_bundle_id"],
        ).first()

        logger.info(
            f"================= ContractDetailsCreateMutationMixin contract_details: {contract_details}"
        )

        if contract_details:
            logger.info(
                "================= ContractDetailsCreateMutationMixin contract_details already exists"
            )
            return "Error! - Contract details already exists"

        try:
            contract_details = ContractDetails(
                contract_id=data["contract_id"],
                insuree_id=data["insuree_id"],
                contribution_plan_bundle_id=data["contribution_plan_bundle_id"],
            )

            logger.info(
                f"================= ContractDetailsCreateMutationMixin contract_details: {contract_details}"
            )

            for key, value in data.items():
                if key == "is_confirmed":
                    contract_details.is_confirmed = value
                elif key == "is_new_insuree":
                    contract_details.is_new_insuree = value
                elif key == "json_param":
                    contract_details.json_param = value
                elif key == "json_ext":
                    # new_gross_salary = value.get("calculation_rule", {}).get("income")
                    # json_data = update_salary(value, new_gross_salary)
                    contract_details.json_ext = value

            logger.info(
                f"================= ContractDetailsCreateMutationMixin contract_details: {contract_details}"
            )

            contract_details.is_new_insuree = True
            contract_details.save(username=user.username)
            re_evaluate_contract_details(
                data["contract_id"], user, user.username)
            return None
        except Exception as e:
            logger.error(f"Error creating contract details: {e}")
            return f"Error creating contract details {e}"


class ContractDeleteMutationMixin:
    @property
    def _model(self):
        raise NotImplementedError()

    @classmethod
    def _object_not_exist_exception(cls, obj_uuid):
        raise ObjectNotExistException(cls._model, obj_uuid)

    @classmethod
    def _validate_mutation(cls, user, **data):
        cls._validate_user(user)

    @classmethod
    def _validate_user(cls, user):
        if type(user) is AnonymousUser or not user.id:
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, uuid):
        output = cls.delete_contract(user=user, contract={"id": uuid})
        return (
            None
            if output["success"]
            else f"Error! - {output['message']}: {output['detail']}"
        )

    @classmethod
    def delete_contract(cls, user, contract):
        contract_service = ContractService(user=user)
        output_data = contract_service.delete(contract=contract)
        return output_data


class ContractSubmitMutationMixin:
    @property
    def _model(self):
        raise NotImplementedError()

    @classmethod
    def _validate_mutation(cls, user, **data):
        if (
            type(user) is AnonymousUser
            or not user.id
            or not user.has_perms(ContractConfig.gql_mutation_submit_contract_perms)
        ):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop("client_mutation_id")
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")
        output = cls.submit_contract(user=user, contract=data)
        return (
            None
            if output["success"]
            else f"Error! - {output['message']}: {output['detail']}"
        )

    @classmethod
    def submit_contract(cls, user, contract):
        contract_service = ContractService(user=user)
        output_data = contract_service.submit(contract=contract)
        return output_data


class ContractApproveMutationMixin:
    @property
    def _model(self):
        raise NotImplementedError()

    @classmethod
    def _validate_mutation(cls, user, **data):
        if (
            type(user) is AnonymousUser
            or not user.id
            or not user.has_perms(
                ContractConfig.gql_mutation_approve_ask_for_change_contract_perms
            )
        ):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop("client_mutation_id")
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")
        output = cls.approve_contract(user=user, contract=data)
        return (
            None
            if output["success"]
            else f"Error! - {output['message']}: {output['detail']}"
        )

    @classmethod
    def approve_contract(cls, user, contract):
        contract_service = ContractService(user=user)
        output_data = contract_service.approve(contract=contract)
        try:
            erp_submit_contract(contract["id"], user)
            logger.info("ERP contract submission was successful.")
        except Exception as e:
            logger.error(f"Failed to submit ERP contract: {e}")
        return output_data


class ContractCounterMutationMixin:
    @property
    def _model(self):
        raise NotImplementedError()

    @classmethod
    def _validate_mutation(cls, user, **data):
        if (
            type(user) is AnonymousUser
            or not user.id
            or not user.has_perms(
                ContractConfig.gql_mutation_approve_ask_for_change_contract_perms
            )
        ):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop("client_mutation_id")
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")
        output = cls.counter_contract(user=user, contract=data)
        return (
            None
            if output["success"]
            else f"Error! - {output['message']}: {output['detail']}"
        )

    @classmethod
    def counter_contract(cls, user, contract):
        contract_service = ContractService(user=user)
        output_data = contract_service.counter(contract=contract)
        return output_data


class ContractAmendMutationMixin:
    @property
    def _model(self):
        raise NotImplementedError()

    @classmethod
    def _validate_mutation(cls, user, **data):
        if (
            type(user) is AnonymousUser
            or not user.id
            or not user.has_perms(ContractConfig.gql_mutation_amend_contract_perms)
        ):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        client_mutation_id = data.get("client_mutation_id")
        if "client_mutation_id" in data:
            data.pop("client_mutation_id")
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")
        output = cls.amend_contract(user=user, contract=data)
        if output["success"]:
            contract = Contract.objects.get(id=output["data"]["id"])
            ContractMutation.object_mutated(
                user, client_mutation_id=client_mutation_id, contract=contract
            )
            return None
        else:
            return f"Error! - {output['message']}: {output['detail']}"

    @classmethod
    def amend_contract(cls, user, contract):
        contract_service = ContractService(user=user)
        output_data = contract_service.amend(contract=contract)
        return output_data


class ContractRenewMutationMixin:
    @property
    def _model(self):
        raise NotImplementedError()

    @classmethod
    def _validate_mutation(cls, user, **data):
        if (
            type(user) is AnonymousUser
            or not user.id
            or not user.has_perms(ContractConfig.gql_mutation_renew_contract_perms)
        ):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        client_mutation_id = data.get("client_mutation_id")
        if "client_mutation_id" in data:
            data.pop("client_mutation_id")
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")
        output = cls.renew_contract(user=user, contract=data)
        if output["success"]:
            contract = Contract.objects.get(id=output["data"]["id"])
            ContractMutation.object_mutated(
                user, client_mutation_id=client_mutation_id, contract=contract
            )
            return None
        else:
            return f"Error! - {output['message']}: {output['detail']}"

    @classmethod
    def renew_contract(cls, user, contract):
        contract_service = ContractService(user=user)
        output_data = contract_service.renew(contract=contract)
        return output_data


class ContractDetailsFromPHInsureeMutationMixin:
    @property
    def _model(self):
        raise NotImplementedError()

    @classmethod
    def _validate_mutation(cls, user, **data):
        if (
            type(user) is AnonymousUser
            or not user.id
            or not user.has_perms(ContractConfig.gql_mutation_update_contract_perms)
        ):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop("client_mutation_id")
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")
        output = cls.create_cd_from_ph_insuree(user=user, data=data)
        return (
            None
            if output["success"]
            else f"Error! - {output['message']}: {output['detail']}"
        )

    @classmethod
    def create_cd_from_ph_insuree(cls, user, data):
        contract_details_service = ContractDetailsService(user=user)
        data_contract = {"id": data["contract_id"]}
        data_insuree = {"id": data["policy_holder_insuree_id"]}
        output_data = contract_details_service.ph_insuree_to_contract_details(
            contract=data_contract, ph_insuree=data_insuree
        )
        return output_data


class ContractCreateInvoiceMutationMixin:
    @property
    def _model(self):
        raise NotImplementedError()

    @classmethod
    def _validate_mutation(cls, user, **data):
        if (
            type(user) is AnonymousUser
            or not user.id
            or not user.has_perms(ContractConfig.gql_invoice_create_perms)
        ):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        client_mutation_id = data.get("client_mutation_id")
        if "client_mutation_id" in data:
            data.pop("client_mutation_id")
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")
        output = cls.create_contract_invoice(user=user, data=data)
        if output["success"]:
            return None
        else:
            return f"Error! - {output['message']}: {output['detail']}"

    @classmethod
    def create_contract_invoice(cls, user, data):
        queryset = Contract.objects.filter(id=data["id"])
        if queryset.count() == 1:
            contract = queryset.first()
            contract_to_invoice_service = ContractToInvoiceService(user=user)
            output_data = contract_to_invoice_service.create_invoice(
                instance=contract, convert_to="InvoiceLine", user=user
            )
            return output_data
