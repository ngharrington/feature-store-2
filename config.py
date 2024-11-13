from models.event import ScamFlagEventProperties, AddCreditCardEventProperties, PurchaseEventProperties, ChargebackEventProperties
from models.aggregate import EventAggregateConfig
from services.event_registry import EventSchemaRegistry


DEFAULT_AGGREGATE_CONFIG_DICT = {
        "scam_flag": [
            {
                "type": "count",
                "name": "total_scam_flags",
                "field": None,
            }
        ],
        "add_credit_card": [
            {
                "type": "distinct_count",
                "name": "credit_card_distinct_zips",
                "field": "zipcode",
            },
            {
                "type": "count",
                "name": "total_credit_cards",
                "field": None,
            },
        ],
        "chargeback": [
            {"type": "sum", "name": "total_chargeback_amount", "field": "amount"}
        ],
        "purchase": [{"type": "sum", "name": "total_purchase_amount", "field": "amount"}],
}

def get_aggregate_configs(config_dict: dict):
    configs = []
    for event_name, config_list in config_dict.items():
        for config in config_list:
            configs.append(EventAggregateConfig(event_name=event_name, **config))
    return configs

class ConfigError(Exception):
    pass

def get_event_properties_map():
    return {
        "scam_flag": ScamFlagEventProperties,
        "add_credit_card": AddCreditCardEventProperties,
        "chargeback": ChargebackEventProperties,
        "purchase": PurchaseEventProperties,
    }


def validate_event_aggregate_config(config: EventAggregateConfig, schema_registry: EventSchemaRegistry):
    schema = schema_registry.event_schemas.get(config.event_name)
    if config.field and not hasattr(schema, config.field):
        raise ConfigError(f"Field '{config.field}' not found in event properties schema for event '{config.event_name}'")