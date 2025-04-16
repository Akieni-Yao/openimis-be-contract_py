from calendar import monthrange
from datetime import timedelta
import os
import logging

import graphene
import graphene_django_optimizer as gql_optimizer
from dateutil.relativedelta import relativedelta

from django.db.models import Q

from policyholder.models import PolicyHolder, PolicyHolderContributionPlan
from .services import check_unique_code
from core.gql_queries import ValidationMessageGQLType
from core.schema import (
    signal_mutation_module_before_mutating,
    OrderedDjangoFilterConnectionField,
)
from core.utils import append_validity_filter
from contract.models import (
    Contract,
    ContractDetails,
    ContractContributionPlanDetails,
    ContractMutation,
)
from contract.gql.gql_types import (
    ContractGQLType,
    ContractDetailsGQLType,
    ContractContributionPlanDetailsGQLType,
)

from contract.gql.gql_mutations.contract_mutations import (
    CreateContractMutation,
    UpdateContractMutation,
    DeleteContractMutation,
    SubmitContractMutation,
    ApproveContractMutation,
    ApproveContractBulkMutation,
    CounterContractMutation,
    AmendContractMutation,
    RenewContractMutation,
    CounterContractBulkMutation,
    ContractCreateInvoiceBulkMutation,
    PrintContractReceiptMutation,
)
from contract.gql.gql_mutations.contract_details_mutations import (
    CreateContractDetailsMutation,
    UpdateContractDetailsMutation,
    DeleteContractDetailsMutation,
    CreateContractDetailByPolicyHolderInsureeMutation,
)
from contract.apps import ContractConfig
from contract.utils import filter_amount_contract

logger = logging.getLogger(__name__)
erp_url = os.environ.get("ERP_HOST", "https://camu-staging-15480786.dev.odoo.com")


class Query(graphene.ObjectType):
    contract = OrderedDjangoFilterConnectionField(
        ContractGQLType,
        client_mutation_id=graphene.String(),
        insuree=graphene.UUID(),
        orderBy=graphene.List(of_type=graphene.String),
        dateValidFrom__Gte=graphene.DateTime(),
        dateValidTo__Lte=graphene.DateTime(),
        amount_from=graphene.Decimal(),
        amount_to=graphene.Decimal(),
        applyDefaultValidityFilter=graphene.Boolean(),
    )

    contract_details = OrderedDjangoFilterConnectionField(
        ContractDetailsGQLType,
        client_mutation_id=graphene.String(),
        orderBy=graphene.List(of_type=graphene.String),
        is_confirmed=graphene.Boolean(),
    )

    contract_contribution_plan_details = OrderedDjangoFilterConnectionField(
        ContractContributionPlanDetailsGQLType,
        insuree=graphene.UUID(),
        contributionPlanBundle=graphene.UUID(),
        orderBy=graphene.List(of_type=graphene.String),
    )

    validate_contract_code = graphene.Field(
        ValidationMessageGQLType,
        contract_code=graphene.String(required=True),
        description="Check that the specified contract code is unique.",
    )

    validate_enddate_by_periodicity = graphene.String(
        start_date=graphene.Date(required=False),
        policyholder_id=graphene.UUID(required=True),
    )

    validate_startdate_based_on_last_contract = graphene.String(
        policyholder_id=graphene.UUID(required=True)
    )

    def resolve_validate_enddate_by_periodicity(
        self, info, start_date=None, policyholder_id=None
    ):
        try:
            policy_holder = PolicyHolder.objects.filter(id=policyholder_id).first()
            if not policy_holder:
                return {"error": "Policy Holder not found"}

            ph_cpb = PolicyHolderContributionPlan.objects.filter(
                policy_holder=policy_holder, is_deleted=False
            ).first()

            contract = (
                Contract.objects.filter(
                    policy_holder__id=policyholder_id, is_deleted=False
                )
                .order_by("-date_valid_to")
                .first()
            )

            if ph_cpb:
                contribution_plan_bundle = ph_cpb.contribution_plan_bundle
                periodicity = contribution_plan_bundle.periodicity

                # Determine the start date if not provided
                if start_date is None and contract:
                    next_day = contract.date_valid_to + timedelta(days=1)
                    if next_day.day != 1:
                        start_date = (next_day + relativedelta(months=1)).replace(day=1)
                    else:
                        start_date = next_day
                elif start_date is None:
                    return {
                        "error": "No start date provided and no previous contract found"
                    }

                if start_date and periodicity is not None:
                    if 1 <= periodicity <= 3:
                        if periodicity == 1:
                            _, last_day_of_month = monthrange(
                                start_date.year, start_date.month
                            )
                            end_date = start_date + timedelta(
                                days=(periodicity * last_day_of_month) - 1
                            )
                        else:
                            end_date = start_date + relativedelta(months=periodicity)
                            end_date -= timedelta(days=1)
                    elif periodicity == 12:
                        logger.info(f"======= periodicity {periodicity}")
                        is_exist = Contract.objects.filter(
                            policy_holder__id=policyholder_id,
                            is_deleted=False,
                            date_valid_from__gte=start_date,
                        )
                        logger.info(f"======= is_exist {is_exist}")
                        logger.info(f"======= contract {contract}")
                        if not is_exist and not contract:
                            end_date = start_date + relativedelta(months=periodicity)
                            end_date -= timedelta(days=1)
                            logger.info(f"======= end_date 1 {end_date}")
                        elif (
                            not is_exist
                            and start_date.date()
                            >= (contract.date_valid_from + timedelta(days=1)).date()
                        ):
                            end_date = start_date + relativedelta(months=periodicity)
                            end_date -= timedelta(days=1)
                            logger.info(f"======== end_date 2 {end_date}")
                        else:
                            logger.info(
                                "======= Invalid Month! Contract of Current or Previous Month is already created."
                            )
                            return {
                                "error": "Invalid Month! Contract of Current or Previous Month is already created."
                            }
                    else:
                        return {"error": f"Invalid periodicity value: {periodicity}"}
                else:
                    return {
                        "error": "Periodicity is not defined for this Contribution Plan Bundle"
                    }

                # Return both start_date and end_date
                return {"start_date": str(start_date), "end_date": str(end_date)}
            else:
                return {
                    "error": "Contribution Plan Bundle not found for this Policy Holder"
                }

        except Exception as e:
            print(f"Error: {e}")
            return {"error": str(e)}

    def resolve_validate_contract_code(self, info, **kwargs):
        if not info.context.user.has_perms(ContractConfig.gql_query_contract_perms):
            if not info.context.user.has_perms(
                ContractConfig.gql_query_contract_policyholder_portal_perms
            ):
                raise PermissionError("Unauthorized")
        errors = check_unique_code(code=kwargs["contract_code"])
        if errors:
            return ValidationMessageGQLType(False)
        else:
            return ValidationMessageGQLType(True)

    def resolve_contract(self, info, **kwargs):
        if not info.context.user.has_perms(ContractConfig.gql_query_contract_perms):
            if not info.context.user.has_perms(
                ContractConfig.gql_query_contract_policyholder_portal_perms
            ):
                raise PermissionError("Unauthorized")

        filters = append_validity_filter(**kwargs)
        client_mutation_id = kwargs.get("client_mutation_id", None)
        if client_mutation_id:
            filters.append(
                Q(mutations__mutation__client_mutation_id=client_mutation_id)
            )

        insuree = kwargs.get("insuree", None)
        if insuree:
            filters.append(Q(contractdetails__insuree__uuid=insuree))

        # amount filters
        amount_from = kwargs.get("amount_from", None)
        amount_to = kwargs.get("amount_to", None)
        if amount_from or amount_to:
            filters.append(filter_amount_contract(**kwargs))
        return gql_optimizer.query(Contract.objects.filter(*filters).all(), info)

    def resolve_contract_details(self, info, **kwargs):
        if not info.context.user.has_perms(ContractConfig.gql_query_contract_perms):
            if not info.context.user.has_perms(
                ContractConfig.gql_query_contract_policyholder_portal_perms
            ):
                raise PermissionError("Unauthorized")

        filters = []
        client_mutation_id = kwargs.get("client_mutation_id", None)
        if client_mutation_id:
            filters.append(
                Q(mutations__mutation__client_mutation_id=client_mutation_id)
            )
        is_confirmed = kwargs.get("is_confirmed", None)
        if is_confirmed:
            filters.append(Q(is_confirmed=is_confirmed))

        return gql_optimizer.query(ContractDetails.objects.filter(*filters).all(), info)

    def resolve_contract_contribution_plan_details(self, info, **kwargs):
        if not info.context.user.has_perms(ContractConfig.gql_query_contract_perms):
            if not info.context.user.has_perms(
                ContractConfig.gql_query_contract_policyholder_portal_perms
            ):
                raise PermissionError("Unauthorized")

        query = ContractContributionPlanDetails.objects.all()

        insuree = kwargs.get("insuree", None)
        contribution_plan_bundle = kwargs.get("contributionPlanBundle", None)

        if insuree:
            query = query.filter(contract_details__insuree__uuid=insuree)

        if contribution_plan_bundle:
            query = query.filter(
                contract_details__contribution_plan_bundle__id=contribution_plan_bundle
            )

        return gql_optimizer.query(query.all(), info)


class Mutation(graphene.ObjectType):
    create_contract = CreateContractMutation.Field()
    update_contract = UpdateContractMutation.Field()
    delete_contract = DeleteContractMutation.Field()
    submit_contract = SubmitContractMutation.Field()
    approve_contract = ApproveContractMutation.Field()
    approve_bulk_contract = ApproveContractBulkMutation.Field()
    counter_contract = CounterContractMutation.Field()
    counter_bulk_contract = CounterContractBulkMutation.Field()
    amend_contract = AmendContractMutation.Field()
    renew_contract = RenewContractMutation.Field()
    create_contract_invoice_bulk = ContractCreateInvoiceBulkMutation.Field()
    print_contract_receipt = PrintContractReceiptMutation.Field()

    create_contract_details = CreateContractDetailsMutation.Field()
    update_contract_details = UpdateContractDetailsMutation.Field()
    delete_contract_details = DeleteContractDetailsMutation.Field()
    create_contract_details_by_ph_insuree = (
        CreateContractDetailByPolicyHolderInsureeMutation.Field()
    )


def on_contract_mutation(sender, **kwargs):
    uuids = kwargs["data"].get("uuids", [])
    if not uuids:
        uuid = kwargs["data"].get("uuid", None)
        uuids = [uuid] if uuid else []
    if not uuids:
        return []
    impacted_contracts = Contract.objects.filter(id__in=uuids).all()
    for contract in impacted_contracts:
        ContractMutation.objects.update_or_create(
            contract=contract, mutation_id=kwargs["mutation_log_id"]
        )
    return []


signal_mutation_module_before_mutating["contract"].connect(on_contract_mutation)
