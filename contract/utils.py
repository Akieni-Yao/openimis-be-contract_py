from django.db.models import Q

from contract.models import Contract


def filter_amount_contract(arg='amount_from', arg2='amount_to', **kwargs):
    amount_from = kwargs.get(arg)
    amount_to = kwargs.get(arg2)

    status_notified = [1, 2]
    status_rectified = [4, 11, 3]
    status_due = [5, 6, 7, 8, 9, 10]

    # scenario - only amount_to set
    if not amount_from and amount_to:
        return (
             Q(amount_notified__lte=amount_to, state__in=status_notified) |
             Q(amount_rectified__lte=amount_to, state__in=status_rectified) |
             Q(amount_due__lte=amount_to, state__in=status_due)
        )

    # scenario - only amount_from set
    if amount_from and not amount_to:
        return (
            Q(amount_notified__gte=amount_from, state__in=status_notified) |
            Q(amount_rectified__gte=amount_from, state__in=status_rectified) |
            Q(amount_due__gte=amount_from, state__in=status_due)
        )

    # scenario - both filters set
    if amount_from and amount_to:
        return (
            Q(amount_notified__gte=amount_from, amount_notified__lte=amount_to, state__in=status_notified) |
            Q(amount_rectified__gte=amount_from, amount_rectified__lte=amount_to, state__in=status_rectified) |
            Q(amount_due__gte=amount_from, amount_due__lte=amount_to, state__in=status_due)
        )


def generate_report_for_contract_receipt(contract_id):
    from core import datetime

    now = datetime.datetime.now()
    try:
        contract = Contract.objects.filter(id=contract_id, is_deleted=False).first()
        if contract:
            contract_details
            # ccpd = ContractContributionPlanDetails.objects.filter(
            #     contribution_id=pd.premium.id
            # ).first()
            # if ccpd and ccpd.contract_details and ccpd.contract_details.contract:
            # contract = ccpd.contract_details.contract
            policy_holder = contract.policy_holder
            # payment = pd.payment
            received_amount = str(payment.received_amount)
            received_date = payment.received_date
            if received_date:
                formatted_date = str(received_date.strftime("%d-%m-%Y"))
            else:
                formatted_date = ""
            receipt_number = str(payment.receipt_no)
            report_name = "payment_receipt"
            activity_code = policy_holder.json_ext["jsonExt"]["activityCode"]
            converted_activity_code = convert_activity_data(activity_code)
            converted_creation_date = str(now.strftime("%d-%m-%Y"))
            data = {
                "data": {
                    "camu_code": policy_holder.code if policy_holder else "",
                    "activity_code": (
                        policy_holder.trade_name if policy_holder else ""
                    ),
                    "reg_date": (
                        converted_creation_date if converted_creation_date else ""
                    ),
                    "niu": (
                        policy_holder.json_ext["jsonExt"]["niu"]
                        if hasattr(policy_holder, "json_ext")
                        and "jsonExt" in policy_holder.json_ext
                        and "niu" in policy_holder.json_ext["jsonExt"]
                        else ""
                    ),
                    "muncipality": (
                        policy_holder.locations.parent.name
                        if policy_holder.locations
                        else ""
                    ),
                    "address": (
                        policy_holder.address["address"] if policy_holder else ""
                    ),
                    "receipt_number": receipt_number if receipt_number else "",
                    "received_amount": received_amount if received_amount else "",
                    "tbd": "",  # Add logic for tbd
                    "contract_number": (
                        payment.payment_code if payment.payment_code else ""
                    ),
                    "phone": (
                        policy_holder.phone if policy_holder else ""
                    ),  # Add logic for phone
                    "receiveddate": formatted_date or "",
                }
            }
            report_config = ReportConfig.get_report(report_name)
            if not report_config:
                raise Http404("Report configuration does not exist")
            report_definition = (
                report_name, report_config["default_report"]
            )
            template_dict = json.loads(report_definition)
            pdf = generate_report(report_name, template_dict, data)
            print("Report generated successfully.")
            return pdf
    except Exception as e:
        print(f"An exception occurred: {str(e)}")
        raise  # Re-raise the exception or handle it according to your requirements
    print("PDF not generated.")
    return None  # Handle the case where no PDF is generated