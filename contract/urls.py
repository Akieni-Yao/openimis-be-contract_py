from django.urls import path

from contract import erp_integrations, views

urlpatterns = [
    path("export/<contract_id>/contracts", views.multi_contract),
    path("import/<contract_id>/contracts/salaries",
         views.update_contract_salaries),
    path('payment-methods/<int:journal_id>',
         erp_integrations.erp_payment_method_line),
    path('task-status/<str:task_id>', views.task_status),
]
