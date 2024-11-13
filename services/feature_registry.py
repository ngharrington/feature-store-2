from collections import defaultdict
import asyncio

from models.rules import PlatformFeature

class PlatformFeaturesRegistry:
    def __init__(self):
        self.features = {}
        self._features_by_rule = defaultdict(list)
        self._lock = asyncio.Lock()

    def add_feature(self, feature: PlatformFeature):
        if feature.name in self.features:
            raise ValueError(f"Feature {feature.name} already exists.")
        self.features[feature.name] = feature
        for rule in feature.rules:
            self._features_by_rule[rule.name].append(feature)

    async def get_feature_by_name(self, name: str):
        async with self._lock:
            if name not in self.features:
                raise ValueError(f"Feature {name} not found.")
            return self.features[name]

    async def get_features_by_rule(self, name: str):
        async with self._lock:
            return self._features_by_rule[name]

    async def list_features(self):
        async with self._lock:
            return list(self.features.values())
