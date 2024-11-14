import pytest
from freezegun import freeze_time

from services.user_feature import UserFeatureService
from services.notifications import NotificationsService
import logging


class MockPlatformFeature:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"MockPlatformFeature(name='{self.name}')"


class MockPlatformFeaturesRegistry:
    def __init__(self):
        self.test_feature = MockPlatformFeature("test_feature")
        self.features = [self.test_feature]

    def list_features(self):
        return self.features


@pytest.mark.asyncio
async def test_circuit_breaker_opens_when_denial_rate_exceeds_threshold():
    feature_registry = MockPlatformFeaturesRegistry()
    notifications_service = NotificationsService()
    service = UserFeatureService(feature_registry, notifications_service, logger=logging.getLogger(__name__))

    feature = feature_registry.test_feature
    user_ids = [f"user_{i}" for i in range(100)]

    # Freeze time at a fixed point
    with freeze_time("2023-01-01 12:00:00") as _:
        # Revoke access for 6 users (6% denial rate)
        for user_id in user_ids[:6]:
            await service.revoke(user_id, feature)
            await service.has_grant(user_id, feature)

        # Grant access for the remaining users
        for user_id in user_ids[6:]:
            await service.grant(user_id, feature)
            await service.has_grant(user_id, feature)

        # Manually call the evaluate_circuit_breakers method
        await service._evaluate_circuit_breakers_once()

        # Circuit should be open (broken) because denial rate > 5%
        assert not service._circuits[feature]


@pytest.mark.asyncio
async def test_access_allowed_when_circuit_breaker_is_open():
    feature_registry = MockPlatformFeaturesRegistry()
    notifications_service = NotificationsService()
    service = UserFeatureService(feature_registry, notifications_service, logger=logging.getLogger(__name__))

    feature = feature_registry.test_feature
    user_id = "user_1"

    await service.revoke(user_id, feature)

    # Manually break the circuit
    service._circuits[feature] = False

    has_access = await service.has_grant(user_id, feature)
    assert has_access


@pytest.mark.asyncio
async def test_access_denied_when_circuit_breaker_is_closed_and_no_grant():
    feature_registry = MockPlatformFeaturesRegistry()
    notifications_service = NotificationsService()
    service = UserFeatureService(feature_registry, notifications_service, logger=logging.getLogger(__name__))

    feature = feature_registry.test_feature
    user_id = "user_1"

    await service.revoke(user_id, feature)

    service._circuits[feature] = True

    has_access = await service.has_grant(user_id, feature)
    assert not has_access


@pytest.mark.asyncio
async def test_access_granted_when_circuit_breaker_is_closed_and_user_has_grant():
    feature_registry = MockPlatformFeaturesRegistry()
    notifications_service = NotificationsService()
    service = UserFeatureService(feature_registry, notifications_service, logger=logging.getLogger(__name__))

    feature = feature_registry.test_feature
    user_id = "user_1"

    await service.grant(user_id, feature)

    service._circuits[feature] = True

    # User should have access
    has_access = await service.has_grant(user_id, feature)
    assert has_access
