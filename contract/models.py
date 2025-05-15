import uuid

from contribution.models import Premium
from contribution_plan.models import ContributionPlan, ContributionPlanBundle
from core import fields
from core import models as core_models
from django.conf import settings
from django.db import models
from graphql import ResolveInfo
from insuree.models import Insuree
from policy.models import Policy
from policyholder.models import PolicyHolder, PolicyHolderContributionPlan


class ContractManager(models.Manager):
    def filter(self, *args, **kwargs):
        keys = [x for x in kwargs if "itemsvc" in x]
        for key in keys:
            new_key = key.replace("itemsvc", self.model.model_prefix)
            kwargs[new_key] = kwargs.pop(key)
        return super(ContractManager, self).filter(*args, **kwargs)


class Contract(core_models.HistoryBusinessModel):
    class ProcessStatus(models.TextChoices):
        PROCESSING = "processing", "Processing"
        CREATED = "created", "Created"
        UPLOADING = "uploading", "Uploading"
        UPLOADED = "uploaded", "Uploaded"
        PROCESSING_UPLOADED_DATA = "processing_uploaded_data", "Processing Uploaded Data"
        FAILED_TO_CREATE = "failed_to_create", "Failed to Create"
        FAILED_TO_UPLOAD = "failed_to_upload", "Failed to Upload"

    code = models.CharField(db_column="Code", max_length=64, null=False)
    policy_holder = models.ForeignKey(
        PolicyHolder,
        db_column="PolicyHolderUUID",
        on_delete=models.deletion.DO_NOTHING,
        blank=True,
        null=True,
    )
    amount_notified = models.FloatField(
        db_column="AmountNotified", blank=True, null=True
    )
    amount_rectified = models.FloatField(
        db_column="AmountRectified", blank=True, null=True
    )
    amount_due = models.FloatField(
        db_column="AmountDue", blank=True, null=True)
    date_approved = fields.DateTimeField(
        db_column="DateApproved", blank=True, null=True
    )
    date_payment_due = fields.DateField(
        db_column="DatePaymentDue", blank=True, null=True
    )
    state = models.SmallIntegerField(db_column="State", blank=True, null=True)
    payment_reference = models.CharField(
        db_column="PaymentReference", max_length=255, blank=True, null=True
    )
    amendment = models.IntegerField(
        db_column="Amendment", blank=False, null=False, default=0
    )

    penalty_raised = models.BooleanField(
        db_column="PenaltyRaised", default=False)
    penalty_raised_date = fields.DateField(
        db_column="PenaltyRaisedDate", null=True)
    # penalty_amount = models.FloatField(db_column='PenaltyAmount', null=True)
    # penalty_paid = models.BooleanField(db_column='PenaltyPaid', null=True)
    # penalty_paid_date = fields.DateField(db_column='PenaltyPaidDate', null=True)
    penalty_waive_off_contract = models.BooleanField(
        db_column="PenaltyWaiveOffContract", default=False
    )
    penalty_waive_off_payment = models.BooleanField(
        db_column="PenaltyWaiveOffPayment", default=False
    )
    penalty_waive_off_contract_reason = models.CharField(
        db_column="PenaltyWaiveOffContractReason", max_length=255, null=True
    )
    penalty_waive_off_payment_reason = models.CharField(
        db_column="PenaltyWaiveOffPaymentReason", max_length=255, null=True
    )
    parent = models.ForeignKey(
        "self", on_delete=models.deletion.DO_NOTHING, db_column="Parent", null=True
    )
    # parent_contract_pending = models.BooleanField(db_column='ParentPaymentPending', default=False)
    # parent_penalty_paid = models.BooleanField(db_column='ParentPaymentPaid', null=True)
    gap_from_parent = models.IntegerField(db_column="GapFromParent", null=True)
    erp_contract_id = models.IntegerField(db_column="ErpContractID", null=True)
    erp_invoice_access_id = models.CharField(
        db_column="ErpInvoiceAccessID", max_length=255, null=True
    )

    # total_amount = models.FloatField(db_column='TotalAmount', null=True)
    use_bundle_contribution_plan_amount = models.BooleanField(default=False)
    process_status = models.CharField(
        max_length=50,
        null=True,
        blank=True,
        choices=ProcessStatus.choices,
    )

    objects = ContractManager()

    @property
    def amount(self):
        amount = 0
        if self.state in [1, 2]:
            amount = self.amount_notified
        elif self.state in [4, 11, 3]:
            amount = self.amount_rectified
        elif self.state in [5, 6, 7, 8, 9, 10]:
            amount = self.amount_due
        else:
            amount = self.amount_due
        return amount

    @classmethod
    def get_queryset(cls, queryset, user):
        queryset = cls.filter_queryset(queryset)
        if isinstance(user, ResolveInfo):
            user = user.context.user
        if settings.ROW_SECURITY and user.is_anonymous:
            return queryset.filter(id=-1)
        if settings.ROW_SECURITY:
            pass
        return queryset

    class Meta:
        db_table = "tblContract"

    STATE_REQUEST_FOR_INFORMATION = 1
    STATE_DRAFT = 2
    STATE_OFFER = 3
    STATE_NEGOTIABLE = 4
    STATE_EXECUTABLE = 5
    STATE_ADDENDUM = 6
    STATE_EFFECTIVE = 7
    STATE_EXECUTED = 8
    STATE_DISPUTED = 9
    STATE_TERMINATED = 10
    STATE_COUNTER = 11


class ContractDetailsManager(models.Manager):
    def filter(self, *args, **kwargs):
        keys = [x for x in kwargs if "itemsvc" in x]
        for key in keys:
            new_key = key.replace("itemsvc", self.model.model_prefix)
            kwargs[new_key] = kwargs.pop(key)
        return super(ContractDetailsManager, self).filter(*args, **kwargs)


class ContractDetails(core_models.HistoryModel):
    contract = models.ForeignKey(
        Contract, db_column="ContractUUID", on_delete=models.deletion.CASCADE
    )
    insuree = models.ForeignKey(
        Insuree, db_column="InsureeID", on_delete=models.deletion.DO_NOTHING
    )
    contribution_plan_bundle = models.ForeignKey(
        ContributionPlanBundle,
        db_column="ContributionPlanBundleUUID",
        on_delete=models.deletion.DO_NOTHING,
    )

    json_param = models.JSONField(
        db_column="Json_param", blank=True, null=True)

    objects = ContractDetailsManager()

    is_confirmed = models.BooleanField(default=False)

    is_new_insuree = models.BooleanField(default=False)

    @classmethod
    def get_queryset(cls, queryset, user):
        queryset = cls.filter_queryset(queryset)
        if isinstance(user, ResolveInfo):
            user = user.context.user
        if settings.ROW_SECURITY and user.is_anonymous:
            return queryset.filter(id=-1)
        if settings.ROW_SECURITY:
            pass
        return queryset

    class Meta:
        db_table = "tblContractDetails"


class ContractContributionPlanDetailsManager(models.Manager):
    def filter(self, *args, **kwargs):
        keys = [x for x in kwargs if "itemsvc" in x]
        for key in keys:
            new_key = key.replace("itemsvc", self.model.model_prefix)
            kwargs[new_key] = kwargs.pop(key)
        return super(ContractContributionPlanDetailsManager, self).filter(
            *args, **kwargs
        )


class ContractContributionPlanDetails(core_models.HistoryBusinessModel):
    contribution_plan = models.ForeignKey(
        ContributionPlan,
        db_column="ContributionPlanUUID",
        on_delete=models.deletion.DO_NOTHING,
    )
    policy = models.ForeignKey(
        Policy, db_column="PolicyID", on_delete=models.deletion.DO_NOTHING
    )
    contract_details = models.ForeignKey(
        ContractDetails,
        db_column="ContractDetailsUUID",
        on_delete=models.deletion.CASCADE,
    )
    contribution = models.ForeignKey(
        Premium,
        db_column="ContributionId",
        related_name="contract_contribution_plan_details",
        on_delete=models.deletion.DO_NOTHING,
        blank=True,
        null=True,
    )

    objects = ContractContributionPlanDetailsManager()

    @classmethod
    def get_queryset(cls, queryset, user):
        queryset = cls.filter_queryset(queryset)
        if isinstance(user, ResolveInfo):
            user = user.context.user
        if settings.ROW_SECURITY and user.is_anonymous:
            return queryset.filter(id=-1)
        if settings.ROW_SECURITY:
            pass
        return queryset

    class Meta:
        db_table = "tblContractContributionPlanDetails"


class ContractMutation(core_models.UUIDModel, core_models.ObjectMutation):
    contract = models.ForeignKey(
        Contract, models.DO_NOTHING, related_name="mutations")
    mutation = models.ForeignKey(
        core_models.MutationLog, models.DO_NOTHING, related_name="contracts"
    )

    class Meta:
        managed = True
        db_table = "contract_contractMutation"


class ContractDetailsMutation(core_models.UUIDModel, core_models.ObjectMutation):
    contract_detail = models.ForeignKey(
        ContractDetails, models.DO_NOTHING, related_name="mutations"
    )
    mutation = models.ForeignKey(
        core_models.MutationLog, models.DO_NOTHING, related_name="contract_details"
    )

    class Meta:
        managed = True
        db_table = "contract_contractDetailsMutation"


class InsureeWaitingPeriod(core_models.UUIDModel):
    policy_holder_contribution_plan = models.ForeignKey(
        PolicyHolderContributionPlan, models.DO_NOTHING
    )
    insuree = models.ForeignKey(Insuree, models.DO_NOTHING)
    waiting_period = models.PositiveIntegerField()
    contribution_periodicity = models.PositiveIntegerField()

    class Meta:
        managed = True
        unique_together = ("policy_holder_contribution_plan", "insuree")
        db_table = "tblInsureeWaitingPeriod"


class ContractPolicy(core_models.UUIDModel):
    contract = models.ForeignKey(Contract, models.DO_NOTHING)
    policy = models.ForeignKey(Policy, models.DO_NOTHING)
    insuree = models.ForeignKey(Insuree, models.DO_NOTHING)
    policy_holder = models.ForeignKey(PolicyHolder, models.DO_NOTHING)
    # timestamp
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = "tblContractPolicy"
        unique_together = ("contract", "policy")

    def __str__(self):
        return f"{self.contract.id} - {self.policy.id}"
