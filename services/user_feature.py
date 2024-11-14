import asyncio
from collections import defaultdict, deque
from typing import List

from models.rules import PlatformFeature
from services.feature_registry import PlatformFeaturesRegistry
from services.notifications import NotificationsService
from models.event import Event
import datetime
import uuid

class UserFeatureService:
    def __init__(self, feature_registry: PlatformFeaturesRegistry, notifications_service: NotificationsService):
        features = feature_registry.list_features()
        self._grants = defaultdict(lambda: self._generate_default_grants(features))
        self._notifications_service = notifications_service
        self._circuits = self._generate_default_grants(features)
        self._lock = asyncio.Lock()
        self._access_logs = defaultdict(lambda: deque())  # Logs of (timestamp, user_id, success)
        self._total_users = defaultdict(set)  # Track all users per feature
        self._denied_users = defaultdict(set)  # Track denied users per feature

    async def grant(self, user_id: str, feature: PlatformFeature):
        async with self._lock:
            if self._has_grant(user_id, feature):
                return
            self._grants[user_id][feature] = True
            self._send_state_change_message(user_id, feature.name, True)

    async def revoke(self, user_id: str, feature: PlatformFeature):
        async with self._lock:
            if not self._has_grant(user_id, feature):
                return
            self._grants[user_id][feature] = False
            self._send_state_change_message(user_id, feature.name, False)

    async def has_grant(self, user_id: str, feature: PlatformFeature) -> bool:
        async with self._lock:
            grant = self._has_grant(user_id, feature)
            circuit_broken = not self._circuits[feature]

            # If the circuit is broken, allow all access
            has_access = circuit_broken or grant
            # log the real grant
            self._log_access_attempt(user_id, feature, success=grant)
            return has_access

    def _log_access_attempt(self, user_id: str, feature: PlatformFeature, success: bool):
        now = datetime.datetime.now()
        print(f"current time is {now}")
        log = self._access_logs[feature]
        log.append((now, user_id, success))
        # Maintain a sliding window of 10 minutes
        cutoff = now - datetime.timedelta(minutes=10)
        print("Cutoff: ", cutoff)
        print(log[0][0])
        while log and log[0][0] < cutoff:
            print('cleaning')
            _, old_user_id, old_success = log.popleft()
            self._total_users[feature].discard(old_user_id)
            if not old_success:
                self._denied_users[feature].discard(old_user_id)

        self._total_users[feature].add(user_id)
        if not success:
            self._denied_users[feature].add(user_id)

    def _has_grant(self, user_id: str, feature: PlatformFeature) -> bool:
        return self._grants[user_id][feature]

    def _generate_default_grants(self, features):
        return dict.fromkeys(features, True)

    def _send_state_change_message(self, user_id: str, feature_name: str, new_grant_state: bool):
        payload = {
            "event_properties": {
                "user_id": user_id,
                "feature": feature_name,
            },
        }
        event = Event(
            name="access_granted" if new_grant_state else "access_revoked",
            uuid=str(uuid.uuid4()),
            timestamp=datetime.datetime.now(),
            event_properties=payload,
        )
        self._notifications_service.send_notification(event)

    async def evaluate_circuit_breakers(self):
        """
        Periodically evaluate whether to open or close feature circuits based on user access metrics.
        """
        while True:
            await self._evaluate_circuit_breakers_once()
            await asyncio.sleep(15)  # Evaluate every minute

    async def _evaluate_circuit_breakers_once(self):
        print("Evaluating circuit breakers")
        now = datetime.datetime.now()
        async with self._lock:
            for feature, total_users in self._total_users.items():
                total_user_count = len(total_users)
                denied_user_count = len(self._denied_users[feature])
                if total_user_count == 0:
                    continue
                
                # Calculate the denial percentage
                denial_rate = 0 if total_user_count == 0 else denied_user_count / total_user_count
                print(f"Denial rate for {feature}: {denial_rate}")
                # Open or close the circuit based on the 5% threshold
                if denial_rate > 0.05:
                    print(f"Breaking circuit for {feature}")
                    self._circuits[feature] = False
                else:
                    print(f"Opening circuit for {feature}")
                    self._circuits[feature] = True


