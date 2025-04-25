from core.gql.gql_mutations import DeleteInputType
from core.gql.gql_mutations.base_mutation import (
    BaseMutation,
    BaseDeleteMutation,
    BaseHistoryModelCreateMutationMixin,
    BaseHistoryModelUpdateMutationMixin,
    BaseHistoryModelDeleteMutationMixin,
)
from .mutations import (
    ContractDetailsCreateMutationMixin,
    ContractDetailsFromPHInsureeMutationMixin,
    ContractDetailsUpdateMutationMixin,
)
from contract.gql.gql_mutations import (
    ContractDetailsCreateInputType,
    ContractDetailsUpdateInputType,
    ContractDetailsCreateFromInsureeInputType,
)
from contract.models import ContractDetails, ContractDetailsMutation


class CreateContractDetailsMutation(ContractDetailsCreateMutationMixin, BaseMutation):
    _mutation_class = "ContractDetailsMutation"
    _mutation_module = "contract"
    _model = ContractDetails

    class Input(ContractDetailsCreateInputType):
        pass


class UpdateContractDetailsMutation(ContractDetailsUpdateMutationMixin, BaseMutation):
    _mutation_class = "ContractDetailsMutation"
    _mutation_module = "contract"
    _model = ContractDetails

    class Input(ContractDetailsUpdateInputType):
        pass


class DeleteContractDetailsMutation(
    BaseHistoryModelDeleteMutationMixin, BaseDeleteMutation
):
    _mutation_class = "ContractDetailsMutation"
    _mutation_module = "contract"
    _model = ContractDetails

    class Input(DeleteInputType):
        pass


class CreateContractDetailByPolicyHolderInsureeMutation(
    ContractDetailsFromPHInsureeMutationMixin, BaseMutation
):
    _mutation_class = "CreateContractDetailByPolicyHolderInsureetMutation"
    _mutation_module = "contract"
    _model = ContractDetails

    class Input(ContractDetailsCreateFromInsureeInputType):
        pass
