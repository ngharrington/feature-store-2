import asyncio

from models.event import Event
from models.aggregate import EventAggregateStore
from services.feature_registry import PlatformFeaturesRegistry
from services.user_feature import UserFeatureService
from models.rules import RulesStore

class EventProcessor:
    def __init__(self, aggregate_store: EventAggregateStore, rule_store: RulesStore, feature_registry: PlatformFeaturesRegistry, user_feature_service: UserFeatureService):
        self.agg_store = aggregate_store
        self.rule_store = rule_store
        self.feature_registry = feature_registry
        self.user_feature_service = user_feature_service

    async def process_event(self, event: Event):
        # Assume for the purposes of this exercise that
        # we never get duplicate events.
        # In a real case we need to ensure that we update aggregates
        # exactly once per distinct event (i.e. with a store of uuids)
        aggregates = await self.agg_store.get_aggregates_by_event_name(event.name)
        # keep track of any Rules associated with the aggregates
        all_rules = set()
        for agg in aggregates:
            agg.update(event.event_properties.user_id, event)
            r = await self.rule_store.get_rules_by_aggregate(agg.name)
            for rule in r:
                all_rules.add(rule)
        
        
        failed_rules = set()
        for rule in all_rules:
            if not rule.abides(event.event_properties.user_id):
                failed_rules.add(rule)

        impacted_features = set()
        for rule in failed_rules:
            features = await self.feature_registry.get_features_by_rule(rule.name)
            impacted_features.update(features)

        for feature in impacted_features:
            failed_rules = False
            for rule in feature.rules:
                if not rule.abides(event.event_properties.user_id):
                    failed_rules = True
                    break
            if failed_rules:
                await self.user_feature_service.revoke(event.event_properties.user_id, feature)
            else:
                await self.user_feature_service.grant(event.event_properties.user_id, feature)


class EventConsumer:
    def __init__(self, queue: asyncio.Queue, event_processor: EventProcessor):
        self.queue = queue
        self.event_processor = event_processor

    async def consume(self):
        try:
            while True:
                print("waiting")
                item = await self.queue.get()
                print("got item")
                await self.event_processor.process_event(item)
                self.queue.task_done()
        except asyncio.CancelledError:
            print("Consumer cancelled")
            return
        except Exception as e:
            print(f"Consumer error: {e}")
            raise
