from django.urls import path

from contract import views, erp_integrations

urlpatterns = [
    path("export/<contract_id>/contracts", views.multi_contract),
    path('payment-methods/<int:journal_id>', erp_integrations.erp_payment_method_line),

]
