import asyncio
import enum
import logging
import re
from collections import defaultdict
from typing import Union

from models.aggregate import EventAggregate


class PlatformFeatureNotFoundError(Exception):
    pass


class RuleOperation(enum.Enum):
    DIVIDE = "DIVIDE"
    VALUE = "VALUE"


class RuleCondition(enum.Enum):
    GREATER_THAN = ">"
    LESS_THAN = "<"


class Rule:
    def __init__(
        self,
        name: str,
        operation: RuleOperation,
        aggregate1: EventAggregate,
        aggregate2: EventAggregate,
        value: Union[float, int],
        condition: RuleCondition,
        denom_min=None,  # minimum value for the denomiter for DIVIDES. If the denominator is below this value the rule always abides.
        logger: logging.Logger = logging.getLogger(__name__),
    ):
        self.name = name
        self.operation = operation
        self.value = value
        self.condition = condition
        self.aggregate1 = aggregate1
        self.aggregate2 = aggregate2
        self.denom_min = denom_min
        self.logger = logger
        if operation == RuleOperation.DIVIDE and aggregate2 is None:
            raise ValueError(f"Aggregate2 is required for {operation} operation.")
        elif operation == RuleOperation.VALUE and aggregate2 is not None:
            raise ValueError(f"Aggregate2 is not required for {operation} operation.")
        elif operation == RuleOperation.VALUE and denom_min is not None:
            raise ValueError(f"Denom_min is not allowed for {operation} operation.")

    def _evaluate(self, user_id: str):
        self.logger.info(f"Evaluating rule {self.name} for user {user_id}")
        override = False
        value = None
        if self.operation == RuleOperation.DIVIDE:
            denom = self.aggregate2.get_user_aggregate(user_id)
            if self.denom_min and denom < self.denom_min:
                override = True
            if denom == 0:
                value = 0
            value = self.aggregate1.get_user_aggregate(user_id) / denom
        elif self.operation == RuleOperation.VALUE:
            value = self.aggregate1.get_user_aggregate(user_id)

        return value, override

    def abides(self, user_id: str):
        value, override = self._evaluate(user_id)
        if override:
            return True
        if self.condition == RuleCondition.GREATER_THAN:
            return value > self.value
        elif self.condition == RuleCondition.LESS_THAN:
            return value < self.value


class RulesStore:
    def __init__(self):
        self.rules = {}
        self._rules_by_aggregate = defaultdict(list)
        self._lock = asyncio.Lock()

    def add_rule(self, rule: Rule):
        if rule.name in self.rules:
            raise ValueError(f"Rule {rule.name} already exists.")
        self.rules[rule.name] = rule
        self._rules_by_aggregate[rule.aggregate1.name].append(rule)
        if rule.operation == RuleOperation.DIVIDE:
            self._rules_by_aggregate[rule.aggregate2.name].append(rule)

    async def get_rule_by_name(self, name: str):
        async with self._lock:
            if name not in self.rules:
                raise ValueError(f"Rule {name} not found.")
            return self.rules[name]

    async def get_rules_by_aggregate(self, name: str):
        async with self._lock:
            return self._rules_by_aggregate[name]


class PlatformFeature:
    def __init__(self, name, rules):
        # we want to seriously limit the valid names here for simplicity.
        if not re.fullmatch(r"[a-z]+", name):
            raise ValueError(
                "The PlatformFeature name must contain only lowercase ASCII letters."
            )
        self.name = name
        self.rules = rules
        self._user_flags = defaultdict(lambda: True)
        self._lock = asyncio.Lock()

    async def disable(self, user_id):
        async with self._lock:
            self._user_flags[user_id] = False

    async def can_access(self, user_id):
        async with self._lock:
            return self._user_flags[user_id]
