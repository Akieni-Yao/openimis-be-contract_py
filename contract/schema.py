from calendar import monthrange
from datetime import timedelta

import graphene
import graphene_django_optimizer as gql_optimizer
from dateutil.relativedelta import relativedelta

from django.db.models import Q

from policyholder.models import PolicyHolder, PolicyHolderContributionPlan
from .services import check_unique_code
from core.gql_queries import ValidationMessageGQLType
from core.schema import signal_mutation_module_before_mutating, OrderedDjangoFilterConnectionField
from core.utils import append_validity_filter
from contract.models import Contract, ContractDetails, \
    ContractContributionPlanDetails, ContractMutation
from contract.gql.gql_types import ContractGQLType, ContractDetailsGQLType, \
    ContractContributionPlanDetailsGQLType

from contract.gql.gql_mutations.contract_mutations import CreateContractMutation, \
    UpdateContractMutation, DeleteContractMutation, SubmitContractMutation, ApproveContractMutation, \
    ApproveContractBulkMutation, CounterContractMutation, \
    AmendContractMutation, RenewContractMutation, CounterContractBulkMutation, ContractCreateInvoiceBulkMutation
from contract.gql.gql_mutations.contract_details_mutations import CreateContractDetailsMutation, \
    UpdateContractDetailsMutation, DeleteContractDetailsMutation, \
    CreateContractDetailByPolicyHolderInsureeMutation
from contract.apps import ContractConfig
from contract.utils import filter_amount_contract


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
        applyDefaultValidityFilter=graphene.Boolean()
    )

    contract_details = OrderedDjangoFilterConnectionField(
        ContractDetailsGQLType,
        client_mutation_id=graphene.String(),
        orderBy=graphene.List(of_type=graphene.String),
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
        description="Check that the specified contract code is unique."
    )

    validate_enddate_by_periodicity = graphene.String(
        start_date=graphene.Date(required=True),
        policyholder_id=graphene.UUID(required=True)
    )

    def resolve_validate_enddate_by_periodicity(self, info, start_date, policyholder_id):
        try:
            policy_holder = PolicyHolder.objects.filter(id=policyholder_id).first()
            if policy_holder:
                ph_cpb = PolicyHolderContributionPlan.objects.filter(policy_holder=policy_holder,
                                                                     is_deleted=False).first()
                contract = Contract.objects.filter(policy_holder__id=policyholder_id, is_deleted=False) \
                    .order_by('-date_valid_to') \
                    .first()

                if ph_cpb:
                    contribution_plan_bundle = ph_cpb.contribution_plan_bundle
                    periodicity = contribution_plan_bundle.periodicity

                    if contract and periodicity != 12:
                        contract_last_date = contract.date_valid_to
                        if start_date != (contract_last_date + timedelta(days=1)).date():
                            return "Please create a contract for the previous month first"

                    if periodicity is not None:
                        if 1 <= periodicity <= 3:
                            if periodicity == 1:
                                _, last_day_of_month = monthrange(start_date.year, start_date.month)
                                end_date = start_date + timedelta(days=(periodicity * last_day_of_month) - 1)
                            else:
                                end_date = start_date + relativedelta(months=periodicity)
                                end_date -= timedelta(days=1)
                            return str(end_date)
                        elif periodicity == 12:
                            is_exist = Contract.objects.filter(policy_holder__id=policyholder_id, is_deleted=False,
                                                               date_valid_from__gte=start_date)
                            if not is_exist and start_date >= (contract.date_valid_from + timedelta(days=1)).date():
                                end_date = start_date + relativedelta(months=periodicity)
                                end_date -= timedelta(days=1)
                                return str(end_date)
                            else:
                                return "Please create a contract for the previous month first" #TODO need to cheange error message
                        else:
                            return f"Invalid periodicity value: {periodicity}"
                    else:
                        return "Periodicity is not defined for this Contribution Plan Bundle"
                else:
                    return "Contribution Plan Bundle not found for this Policy Holder"
            else:
                return "Policy Holder not found"
        except Exception as e:
            print(f"Error: {e}")
            return str(e)

    def resolve_validate_contract_code(self, info, **kwargs):
        if not info.context.user.has_perms(ContractConfig.gql_query_contract_perms):
            if not info.context.user.has_perms(ContractConfig.gql_query_contract_policyholder_portal_perms):
                raise PermissionError("Unauthorized")
        errors = check_unique_code(code=kwargs['contract_code'])
        if errors:
            return ValidationMessageGQLType(False)
        else:
            return ValidationMessageGQLType(True)

    def resolve_contract(self, info, **kwargs):
        if not info.context.user.has_perms(ContractConfig.gql_query_contract_perms):
            if not info.context.user.has_perms(ContractConfig.gql_query_contract_policyholder_portal_perms):
                raise PermissionError("Unauthorized")

        filters = append_validity_filter(**kwargs)
        client_mutation_id = kwargs.get("client_mutation_id", None)
        if client_mutation_id:
            filters.append(Q(mutations__mutation__client_mutation_id=client_mutation_id))

        insuree = kwargs.get('insuree', None)
        if insuree:
            filters.append(Q(contractdetails__insuree__uuid=insuree))

        # amount filters
        amount_from = kwargs.get('amount_from', None)
        amount_to = kwargs.get('amount_to', None)
        if amount_from or amount_to:
            filters.append(filter_amount_contract(**kwargs))
        return gql_optimizer.query(Contract.objects.filter(*filters).all(), info)

    def resolve_contract_details(self, info, **kwargs):
        if not info.context.user.has_perms(ContractConfig.gql_query_contract_perms):
            if not info.context.user.has_perms(ContractConfig.gql_query_contract_policyholder_portal_perms):
                raise PermissionError("Unauthorized")

        filters = []
        client_mutation_id = kwargs.get("client_mutation_id", None)
        if client_mutation_id:
            filters.append(Q(mutations__mutation__client_mutation_id=client_mutation_id))

        return gql_optimizer.query(ContractDetails.objects.filter(*filters).all(), info)

    def resolve_contract_contribution_plan_details(self, info, **kwargs):
        if not info.context.user.has_perms(ContractConfig.gql_query_contract_perms):
            if not info.context.user.has_perms(ContractConfig.gql_query_contract_policyholder_portal_perms):
                raise PermissionError("Unauthorized")

        query = ContractContributionPlanDetails.objects.all()

        insuree = kwargs.get('insuree', None)
        contribution_plan_bundle = kwargs.get('contributionPlanBundle', None)

        if insuree:
            query = query.filter(
                contract_details__insuree__uuid=insuree
            )

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

    create_contract_details = CreateContractDetailsMutation.Field()
    update_contract_details = UpdateContractDetailsMutation.Field()
    delete_contract_details = DeleteContractDetailsMutation.Field()
    create_contract_details_by_ph_insuree = CreateContractDetailByPolicyHolderInsureeMutation.Field()


def on_contract_mutation(sender, **kwargs):
    uuids = kwargs['data'].get('uuids', [])
    if not uuids:
        uuid = kwargs['data'].get('uuid', None)
        uuids = [uuid] if uuid else []
    if not uuids:
        return []
    impacted_contracts = Contract.objects.filter(id__in=uuids).all()
    for contract in impacted_contracts:
        ContractMutation.objects.update_or_create(contract=contract, mutation_id=kwargs['mutation_log_id'])
    return []


signal_mutation_module_before_mutating["contract"].connect(on_contract_mutation)
