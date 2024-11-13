from models.event import ScamFlagEventProperties, AddCreditCardEventProperties, ChargebackEventProperties

def get_event_properties_map():
    return {
        "scam_flag": ScamFlagEventProperties,
        "add_credit_card": AddCreditCardEventProperties,
        "chargeback": ChargebackEventProperties,
    }

