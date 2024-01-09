from django.urls import path

from contract import views

urlpatterns = [
    path("export/<id>/contract", views.single_contract),
    path("export/<contract_id>/contracts", views.multi_contract),
]
