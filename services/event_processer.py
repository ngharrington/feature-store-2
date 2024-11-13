import asyncio

from models.event import Event
from models.aggregate import EventAggregateStore
from services.feature_registry import PlatformFeaturesRegistry
from models.rules import RulesStore

class EventProcessor:
    def __init__(self, aggregate_store: EventAggregateStore, rule_store: RulesStore, feature_registry: PlatformFeaturesRegistry):
        self.agg_store = aggregate_store
        self.rule_store = rule_store
        self.feature_registry = feature_registry

    async def process_event(self, event: Event):
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

        print(f"impacted features: {list(impacted_features)}")

        print("processed event")


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
