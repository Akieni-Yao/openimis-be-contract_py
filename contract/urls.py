from django.urls import path

from contract import views, erp_integrations

urlpatterns = [
    path("export/<contract_id>/contracts", views.multi_contract),
    path("import/<contract_id>/contracts/salaries", views.update_contract_salaries),
]
