CONTRACT_STATE = {
    "ContractState": [
        {
            "value": "1",
            "label":
                {
                    "fr": "Demande d'information",
                    "en": "Request for information"
                }
        },
        {
            "value": "2",
            "label":
                {
                    "fr": "Brouillon",
                    "en": "Draft"
                }
        },
        {
            "value": "3",
            "label":
                {
                    "fr": "Offre",
                    "en": "offer"
                }
        },
        {
            "value": "4",
            "label":
                {
                    "fr": "En negociation",
                    "en": "Negotiable"

                }
        },
        {
            "value": "5",
            "label":
                {
                    "fr": "Apprové",
                    "en": "executable"

                }
        },
        {
            "value": "6",
            "label":
                {
                    "fr": "addendum",
                    "en": "addendum"
                }
        },
        {
            "value": "7",
            "label":
                {
                    "fr": "En cours",
                    "en": "effective"
                }
        },
        {
            "value": "8",
            "label":
                {
                    "fr": "Appliqué",
                    "en": "executed"

                }
        },
        {
            "value": "9",
            "label":
                {
                    "fr": "Suspendu",
                    "en": "Disputed"
                }
        },
        {
            "value": "10",
            "label":
                {
                    "fr": "Terminé",
                    "en": "terminated"
                }
        },
        {
            "value": "11",
            "label":
                {
                    "fr": "révision demandée",
                    "en": "counter"
                }
        }]
}


def get_message_approved_contract(code, name, contact_name, due_amount, payment_reference, language='en'):
    message_payment_notification = {
        "payment_notification":
            {"en":
                 F"""
                 Dear {contact_name} 
                 
                 The contract {code} - {name} was approved.
                 Please proceed to the payment of {due_amount} with the reference {payment_reference}. 
                 
                 Best regards, 
                 """
                ,
             "fr":
                 F"""
                 Monsieur, Madame {contact_name}
                 
                 le contract {code} - {name} à été approuvé.
                 Veuillez faire un paiement de {due_amount} avec la référence {payment_reference}.
                 
                 Meilleurs Salutations 
                 """
             }
    }
    return message_payment_notification["payment_notification"][language]


def get_message_counter_contract(code, name, contact_name, language='en'):
    message_payment_notification = {
        "payment_notification":
            {"en":
                 F"""
                 Dear {contact_name} 

                 The contract {code} - {name} was countered.
                 Please proceed recheck the information and correct the issues, in case of questions please check contact us. 

                 Best regards, 
                 """
                ,
             "fr":
                 F"""
                 Monsieur, Madame {contact_name}

                 le contract {code} - {name} à été contré.
                 Veuillez verifier les informations saisies, en cas de question veuillez nous contacter.

                 Meilleures Salutations 
                 """
             }
    }
    return message_payment_notification["payment_notification"][language]
