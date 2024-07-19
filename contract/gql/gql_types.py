import graphene

from contribution_plan.models import ContributionPlanBundleDetails
from core import prefix_filterset, ExtendedConnection
from graphene_django import DjangoObjectType
from contract.models import Contract, ContractDetails, ContractContributionPlanDetails, \
    ContractMutation, ContractDetailsMutation
from insuree.schema import InsureeGQLType
from contribution_plan.gql.gql_types import ContributionPlanGQLType, ContributionPlanBundleGQLType
from contribution.gql_queries import PremiumGQLType
from policyholder.gql.gql_types import PolicyHolderGQLType
from policyholder.models import PolicyHolderInsuree
from pprint import pprint


class ContractGQLType(DjangoObjectType):
    class Meta:
        model = Contract
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "code": ["exact", "istartswith", "icontains", "iexact"],
            **prefix_filterset("policy_holder__", PolicyHolderGQLType._meta.filter_fields),
            "amount_notified": ["exact", "lt", "lte", "gt", "gte"],
            "amount_rectified": ["exact", "lt", "lte", "gt", "gte"],
            "amount_due": ["exact", "lt", "lte", "gt", "gte"],
            "date_payment_due": ["exact", "lt", "lte", "gt", "gte"],
            "state": ["exact"],
            "payment_reference": ["exact", "istartswith", "icontains", "iexact"],
            "amendment": ["exact"],
            "date_created": ["exact", "lt", "lte", "gt", "gte"],
            "date_updated": ["exact", "lt", "lte", "gt", "gte"],
            "is_deleted": ["exact"],
            "version": ["exact"],
            "date_valid_from": ["exact", "gt", "gte", "isnull"],
            "date_valid_to": ["exact", "lt", "lte", "isnull"],
        }

        connection_class = ExtendedConnection

        @classmethod
        def get_queryset(cls, queryset, info):
            return Contract.get_queryset(queryset, info)

    amount = graphene.Float()


class ContractDetailsGQLType(DjangoObjectType):
    custom_field = graphene.String()

    class Meta:
        model = ContractDetails
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            **prefix_filterset("contract__", ContractGQLType._meta.filter_fields),
            **prefix_filterset("insuree__", InsureeGQLType._meta.filter_fields),
            **prefix_filterset("contribution_plan_bundle__", ContributionPlanBundleGQLType._meta.filter_fields),
            "date_created": ["exact", "lt", "lte", "gt", "gte"],
            "date_updated": ["exact", "lt", "lte", "gt", "gte"],
            "is_deleted": ["exact"],
            "version": ["exact"],
        }

        connection_class = ExtendedConnection

        @classmethod
        def get_queryset(cls, queryset, info):
            return ContractDetails.get_queryset(queryset, info)

    def resolve_custom_field(self, info):
        try:
            cpb = self.contribution_plan_bundle
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

            insuree = self.insuree
            contract = self.contract
            phn_json = ContractDetails.objects.filter(
                insuree=insuree.id,
                contribution_plan_bundle=cpb.id,
                contract=contract.id,
                is_deleted=False,
            ).first()
            if phn_json and phn_json.json_ext:
                json_data = phn_json.json_ext
                ei = float(json_data.get('calculation_rule', {}).get('income', 0))
            employer_contribution = round(ei * ercp / 100, 2) if ercp and ei is not None else 0
            salary_share = round(ei * eecp / 100, 2) if eecp and ei is not None else 0
            total = salary_share + employer_contribution
            response = {
                'total': total,
                'employerContribution': employer_contribution,
                'salaryShare': salary_share,
            }
            return response
        except Exception as e:
            return None


class ContractContributionPlanDetailsGQLType(DjangoObjectType):
    class Meta:
        model = ContractContributionPlanDetails
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            **prefix_filterset("contract_details__", ContractDetailsGQLType._meta.filter_fields),
            **prefix_filterset("contribution_plan__", ContributionPlanGQLType._meta.filter_fields),
            **prefix_filterset("contribution__", PremiumGQLType._meta.filter_fields),
            "date_created": ["exact", "lt", "lte", "gt", "gte"],
            "date_updated": ["exact", "lt", "lte", "gt", "gte"],
            "is_deleted": ["exact"],
            "version": ["exact"],
        }

        connection_class = ExtendedConnection

        @classmethod
        def get_queryset(clscls, queryset, info):
            return ContractContributionPlanDetails.get_queryset(queryset, info)


class ContractMutationGQLType(DjangoObjectType):
    class Meta:
        model = ContractMutation


class ContractDetailsMutationGQLType(DjangoObjectType):
    class Meta:
        model = ContractDetailsMutation
