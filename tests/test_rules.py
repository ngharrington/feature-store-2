import pytest
from models.rules import Rule, RuleOperation, RuleCondition
from unittest.mock import Mock

@pytest.mark.asyncio
async def test_rule_evaluate_divide():
    aggregate1 = Mock()
    aggregate2 = Mock()

    aggregate1.get_user_aggregate.return_value = 10
    aggregate2.get_user_aggregate.return_value = 2

    rule = Rule(
        name="test_rule",
        operation=RuleOperation.DIVIDE,
        aggregate1=aggregate1,
        aggregate2=aggregate2,
        value=4,
        condition=RuleCondition.GREATER_THAN
    )

    result = rule._evaluate(user_id="user1")
    assert result[0] == 5.0  # 10 / 2

@pytest.mark.asyncio
async def test_rule_abides_true():
    aggregate1 = Mock()
    aggregate2 = Mock()

    aggregate1.get_user_aggregate.return_value = 8
    aggregate2.get_user_aggregate.return_value = 2

    rule = Rule(
        name="test_rule",
        operation=RuleOperation.DIVIDE,
        aggregate1=aggregate1,
        aggregate2=aggregate2,
        value=3,
        condition=RuleCondition.GREATER_THAN
    )

    result = rule.abides(user_id="user1")
    assert result is True  # (8 / 2) > 3

@pytest.mark.asyncio
async def test_rule_abides_false():
    aggregate1 = Mock()
    aggregate2 = Mock()

    aggregate1.get_user_aggregate.return_value = 4
    aggregate2.get_user_aggregate.return_value = 2

    rule = Rule(
        name="test_rule",
        operation=RuleOperation.DIVIDE,
        aggregate1=aggregate1,
        aggregate2=aggregate2,
        value=3,
        condition=RuleCondition.GREATER_THAN
    )

    result = rule.abides(user_id="user1")
    assert result is False  # (4 / 2) is not > 3


@pytest.mark.asyncio
async def test_rule_evaluate_value():
    # Create a mock aggregate
    aggregate1 = Mock()
    aggregate1.get_user_aggregate.return_value = 10

    # Create a Rule instance with VALUE operation
    rule = Rule(
        name="test_rule_value",
        operation=RuleOperation.VALUE,
        aggregate1=aggregate1,
        aggregate2=None,  # Not required for VALUE
        value=5,
        condition=RuleCondition.GREATER_THAN
    )

    # Evaluate the rule for a specific user
    result = rule._evaluate(user_id="user1")
    assert result[0] == 10

@pytest.mark.asyncio
async def test_rule_abides_value_greater_than():
    # Create a mock aggregate
    aggregate1 = Mock()
    aggregate1.get_user_aggregate.return_value = 10

    rule = Rule(
        name="test_rule_value_gt",
        operation=RuleOperation.VALUE,
        aggregate1=aggregate1,
        aggregate2=None,
        value=5,
        condition=RuleCondition.GREATER_THAN
    )

    result = rule.abides(user_id="user1")
    assert result is True  # 10 > 5

@pytest.mark.asyncio
async def test_rule_abides_value_less_than():
    aggregate1 = Mock()
    aggregate1.get_user_aggregate.return_value = 2

    rule = Rule(
        name="test_rule_value_lt",
        operation=RuleOperation.VALUE,
        aggregate1=aggregate1,
        aggregate2=None,
        value=5,
        condition=RuleCondition.LESS_THAN
    )

    result = rule.abides(user_id="user1")
    assert result is True  # 2 < 5

@pytest.mark.asyncio
async def test_rule_abides_denom_min_not_met():
    aggregate1 = Mock()
    aggregate2 = Mock()

    aggregate1.get_user_aggregate.return_value = 10  # Numerator
    aggregate2.get_user_aggregate.return_value = 1   # Denominator (below denom_min)

    # Create a rule with denom_min set to 2
    rule = Rule(
        name="test_rule_denom_min",
        operation=RuleOperation.DIVIDE,
        aggregate1=aggregate1,
        aggregate2=aggregate2,
        value=5,
        condition=RuleCondition.GREATER_THAN,
        denom_min=2
    )

    result = rule.abides(user_id="user1")

    # Since denom_min is not met, the rule should abide regardless of condition
    assert result is True

