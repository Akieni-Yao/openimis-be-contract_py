import base64
import os

import graphene
from core.gql.gql_mutations import (
    DeleteInputType,
    mutation_on_uuids_from_filter_business_model,
)
from core.gql.gql_mutations.base_mutation import BaseDeleteMutation, BaseMutation
from django.conf import settings
from graphene import Mutation, String
from kombu.exceptions import OperationalError

from contract.exceptions import CeleryWorkerError
from contract.gql.gql_mutations.input_types import (
    ContractAmendInputType,
    ContractApproveBulkInputType,
    ContractApproveInputType,
    ContractCounterBulkInputType,
    ContractCounterInputType,
    ContractCreateInputType,
    ContractCreateInvoiceBulkInputType,
    ContractRenewInputType,
    ContractSubmitInputType,
    ContractUpdateInputType,
)
from contract.gql.gql_types import ContractGQLType
from contract.models import Contract
from contract.tasks import (
    approve_contract_async,
    approve_contracts,
    counter_contracts,
    create_contract_async,
    create_invoice_from_contracts,
)
from contract.utils import generate_report_for_contract_receipt

from .mutations import (
    ContractAmendMutationMixin,
    ContractApproveMutationMixin,
    ContractCounterMutationMixin,
    ContractCreateInvoiceMutationMixin,
    ContractCreateMutationMixin,
    ContractDeleteMutationMixin,
    ContractRenewMutationMixin,
    ContractSubmitMutationMixin,
    ContractUpdateMutationMixin,
)

# class CreateContractMutation(ContractCreateMutationMixin, BaseMutation):
#     _mutation_class = "CreateContractMutation"
#     _mutation_module = "contract"
#     _model = Contract

#     class Input(ContractCreateInputType):
#         pass


# import graphene
# from graphql import GraphQLError
# from django.core.exceptions import ValidationError

# from contract.services import ContractService
# from contract.tasks import create_contract_async
# from contract.models import Contract
# from contract.gql.types import ContractCreateInputType
# from contract.config import ContractConfig
# from django.contrib.auth.models import AnonymousUser
# from graphql import GraphQLError

class CreateContractMutation(graphene.Mutation):
    success = graphene.Boolean()
    message = graphene.String()
    task_id = graphene.String()

    # Flatten all input fields here
    class Arguments:
        code = graphene.String(required=True)
        policy_holder_id = graphene.UUID(required=False)
        amount_notified = graphene.Decimal(required=False)
        amount_rectified = graphene.Decimal(required=False)
        amount_due = graphene.Decimal(required=False)
        date_approved = graphene.DateTime(required=False)
        date_payment_due = graphene.Date(required=False)
        payment_reference = graphene.String(required=False)
        amendment = graphene.Int(required=False)
        date_valid_from = graphene.Date(required=False)
        date_valid_to = graphene.Date(required=False)
        json_ext = graphene.types.json.JSONString(required=False)
        penalty_waive_off_contract = graphene.Boolean(required=True)
        penalty_waive_off_payment = graphene.Boolean(required=True)
        penalty_waive_off_contract_reason = graphene.String(required=False)
        penalty_waive_off_payment_reason = graphene.String(required=False)
        client_mutation_id = graphene.String(required=False)
        client_mutation_label = graphene.String(required=False)

    @classmethod
    def mutate(cls, root, info, **kwargs):
        from django.contrib.auth.models import AnonymousUser
        from graphql import GraphQLError

        from contract.apps import ContractConfig

        user = info.context.user

        if (
            isinstance(user, AnonymousUser)
            or not user.id
            or not user.has_perms(ContractConfig.gql_mutation_create_contract_perms)
        ):
            raise GraphQLError("mutation.authentication_required")

        client_mutation_id = kwargs.pop("client_mutation_id", None)
        kwargs.pop("client_mutation_label", None)

        try:
            task = create_contract_async.delay(
                user_id=user.id,
                contract_data=kwargs,
                client_mutation_id=client_mutation_id,
            )

            return CreateContractMutation(
                success=True,
                message="Contract creation started",
                task_id=task.id,
            )
        except Exception as e:
            return CreateContractMutation(
                success=False,
                message=f"An error occurred: {str(e)}",
                task_id=None,
            )


class UpdateContractMutation(ContractUpdateMutationMixin, BaseMutation):
    _mutation_class = "UpdateContractMutation"
    _mutation_module = "contract"
    _model = Contract

    class Input(ContractUpdateInputType):
        pass


class DeleteContractMutation(ContractDeleteMutationMixin, BaseDeleteMutation):
    _mutation_class = "DeleteContractMutation"
    _mutation_module = "contract"
    _model = Contract

    class Input(DeleteInputType):
        pass


class SubmitContractMutation(ContractSubmitMutationMixin, BaseMutation):
    _mutation_class = "SubmitContractMutation"
    _mutation_module = "contract"
    _model = Contract

    class Input(ContractSubmitInputType):
        pass


class ApproveContractMutation(graphene.Mutation):
    success = graphene.Boolean()
    message = graphene.String()
    task_id = graphene.String()

    class Arguments:
        id = graphene.UUID(required=True)
        client_mutation_id = graphene.String(required=False)
        client_mutation_label = graphene.String(required=False)

    @classmethod
    def mutate(cls, root, info, **kwargs):
        from django.contrib.auth.models import AnonymousUser
        from graphql import GraphQLError

        from contract.apps import ContractConfig

        user = info.context.user

        if (
            isinstance(user, AnonymousUser)
            or not user.id
            or not user.has_perms(
                ContractConfig
                .gql_mutation_approve_ask_for_change_contract_perms
            )
        ):
            raise GraphQLError("mutation.authentication_required")

        client_mutation_id = kwargs.pop("client_mutation_id", None)
        kwargs.pop("client_mutation_label", None)

        try:
            task = approve_contract_async.delay(
                user_id=user.id,
                contract_id=kwargs["id"],
                client_mutation_id=client_mutation_id
            )

            return ApproveContractMutation(
                success=True,
                message="Contract approval started",
                task_id=task.id,
            )
        except Exception as e:
            return ApproveContractMutation(
                success=False,
                message=f"An error occurred: {str(e)}",
                task_id=None,
            )


class ApproveContractBulkMutation(ContractApproveMutationMixin, BaseMutation):
    _mutation_class = "ApproveContractBulkMutation"
    _mutation_module = "contract"
    _model = Contract

    @classmethod
    @mutation_on_uuids_from_filter_business_model(
        Contract, ContractGQLType, "extended_filters", {}
    )
    def async_mutate(cls, user, **data):
        error_message = None
        if "client_mutation_id" in data:
            data.pop("client_mutation_id")
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")
        if "contract_uuids" in data or "uuids" in data:
            error_message = cls.approve_contracts(user=user, contracts=data)
        return error_message

    def _check_celery_status(cls):
        try:
            from openIMIS.celery import app as celery_app

            connection = celery_app.broker_connection().ensure_connection(max_retries=3)
            if not connection:
                raise CeleryWorkerError(
                    "Celery worker not found. Please, contact your system administrator."
                )
        except (IOError, OperationalError) as e:
            raise CeleryWorkerError(
                f"Celery connection has failed. Error: {e} \n Please, contact your system administrator."
            )

    @classmethod
    def approve_contracts(cls, user, contracts):
        try:
            cls._check_celery_status(cls)
        except CeleryWorkerError as e:
            return f"Celery connection has failed. Please, contact your system administrator."
        if "uuids" in contracts:
            contracts["uuids"] = list(
                contracts["uuids"].values_list("id", flat=True))
            approve_contracts.delay(
                user_id=f"{user.id}", contracts=contracts["uuids"])
        else:
            if "contract_uuids" in contracts:
                approve_contracts.delay(
                    user_id=f"{user.id}", contracts=contracts["contract_uuids"]
                )

    class Input(ContractApproveBulkInputType):
        pass


class CounterContractMutation(ContractCounterMutationMixin, BaseMutation):
    _mutation_class = "CounterContractMutation"
    _mutation_module = "contract"
    _model = Contract

    class Input(ContractCounterInputType):
        pass


class PrintContractReceiptMutation(Mutation):
    success = graphene.Boolean()
    message = graphene.String()
    data = graphene.String()

    class Arguments:
        contract_id = String(required=True)

    def mutate(self, info, contract_id):
        try:
            pdf = generate_report_for_contract_receipt(contract_id, info)

            if pdf:
                pdf_path = os.path.join(
                    settings.BASE_DIR, "payment_receipt.pdf")
                with open(pdf_path, "wb") as pdf_file:
                    pdf_file.write(pdf)
                with open(pdf_path, "rb") as pdf_file:
                    pdf_data = pdf_file.read()
                os.remove(pdf_path)
                encoded_pdf_data = base64.b64encode(pdf_data).decode()
                return PrintContractReceiptMutation(
                    success=True,
                    message="PDF generated successfully",
                    data=encoded_pdf_data,
                )
            else:
                return PrintContractReceiptMutation(
                    success=False, message="PDF generation failed", data=None
                )
        except Exception as e:
            return PrintContractReceiptMutation(
                success=False, message="An error occurred: {}".format(str(e)), data=None
            )


class ContractCreateInvoiceBulkMutation(
    ContractCreateInvoiceMutationMixin, BaseMutation
):
    _mutation_class = "ContractCreateInvoiceBulkMutation"
    _mutation_module = "contract"
    _model = Contract

    @classmethod
    @mutation_on_uuids_from_filter_business_model(
        Contract, ContractGQLType, "extended_filters", {}
    )
    def async_mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop("client_mutation_id")
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")
        if "contract_uuids" in data or "uuids" in data:
            cls.create_contract_invoice(user=user, contracts=data)
        return None

    @classmethod
    def create_contract_invoice(cls, user, contracts):
        if "uuids" in contracts:
            contracts["uuids"] = list(
                contracts["uuids"].values_list("id", flat=True))
            create_invoice_from_contracts.delay(
                user_id=f"{user.id}", contracts=contracts["uuids"]
            )
        else:
            if "contract_uuids" in contracts:
                create_invoice_from_contracts.delay(
                    user_id=f"{user.id}", contracts=contracts["contract_uuids"]
                )

    class Input(ContractCreateInvoiceBulkInputType):
        pass


class CounterContractBulkMutation(ContractCounterMutationMixin, BaseMutation):
    _mutation_class = "CounterContractBulkMutation"
    _mutation_module = "contract"
    _model = Contract

    @classmethod
    @mutation_on_uuids_from_filter_business_model(
        Contract, ContractGQLType, "extended_filters", {}
    )
    def async_mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop("client_mutation_id")
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")
        if "contract_uuids" in data or "uuids" in data:
            cls.counter_contracts(user=user, contracts=data)
        return None

    @classmethod
    def counter_contracts(cls, user, contracts):
        if "uuids" in contracts:
            contracts["uuids"] = list(
                contracts["uuids"].values_list("id", flat=True))
            counter_contracts.delay(
                user_id=f"{user.id}", contracts=contracts["uuids"])
        else:
            if "contract_uuids" in contracts:
                counter_contracts.delay(
                    user_id=f"{user.id}", contracts=contracts["contract_uuids"]
                )

    class Input(ContractCounterBulkInputType):
        pass


class AmendContractMutation(ContractAmendMutationMixin, BaseMutation):
    _mutation_class = "AmendContractMutation"
    _mutation_module = "contract"
    _model = Contract

    class Input(ContractAmendInputType):
        pass


class RenewContractMutation(ContractRenewMutationMixin, BaseMutation):
    _mutation_class = "RenewContractMutation"
    _mutation_module = "contract"
    _model = Contract

    class Input(ContractRenewInputType):
        pass
