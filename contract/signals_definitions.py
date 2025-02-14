from core.signals import Signal

_contract_signal_params = ["contract", "user"]
_contract_approve_signal_params = ["contract", "user", "contract_details_list", "service_object", "payment_service",
                                   "ccpd_service"]

signal_contract = Signal(providing_args=_contract_signal_params)
signal_contract_approve = Signal(providing_args=_contract_signal_params) 