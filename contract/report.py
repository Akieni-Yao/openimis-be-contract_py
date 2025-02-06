from contract.reports import contract_referrals
from contract.reports.contract_referrals import contract_referrals_query

report_definitions = [
    {
        "name": "contract_referrals",
        "engine": 0,
        "default_report": contract_referrals.template,
        "description": "Déclaration états de sortie",
        "module": "contract",
        "python_query": contract_referrals_query,
        "permission": ["131214"],
    },
]
