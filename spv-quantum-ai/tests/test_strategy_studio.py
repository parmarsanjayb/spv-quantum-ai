import pytest
from sqlalchemy import delete
from database.connection import async_session
from database.models import StrategyDefinitionModel
from strategies.studio import strategy_studio, validate_definition, StrategyValidationError
from strategies.engine import strategy_engine


VALID_DEFINITION = {
    "name": "studio_test_strategy",
    "version": "1.0.0",
    "description": "Test strategy",
    "enabled": True,
    "rules": {
        "operator": "AND",
        "conditions": [
            {"source": "indicator", "key": "EMA_9", "operator": ">", "target": "EMA_20"},
            {"source": "indicator", "key": "RSI", "operator": ">", "value": 50.0},
        ],
    },
    "exit_rules": {
        "operator": "AND",
        "conditions": [
            {"source": "indicator", "key": "EMA_9", "operator": "<", "target": "EMA_20"},
        ],
    },
    "actions": {
        "matched": {"action": "SIGNAL_BUY", "confidence": 85.0, "reason": "Entry"},
        "exit": {"action": "SIGNAL_SELL", "confidence": 80.0, "reason": "Exit"},
    },
}


async def _clean(name: str) -> None:
    async with async_session() as session:
        await session.execute(delete(StrategyDefinitionModel).where(StrategyDefinitionModel.strategy_name == name))
        await session.commit()


# ── Validation ──────────────────────────────────────────────────────────────

def test_validate_definition_accepts_valid_strategy():
    errors = validate_definition(VALID_DEFINITION)
    assert errors == []


def test_validate_definition_rejects_unknown_indicator():
    bad = dict(VALID_DEFINITION)
    bad["rules"] = {
        "operator": "AND",
        "conditions": [{"source": "indicator", "key": "NOT_A_REAL_INDICATOR", "operator": ">", "value": 1.0}],
    }
    errors = validate_definition(bad)
    assert any("NOT_A_REAL_INDICATOR" in e for e in errors)


def test_validate_definition_rejects_empty_conditions():
    bad = dict(VALID_DEFINITION)
    bad["rules"] = {"operator": "AND", "conditions": []}
    errors = validate_definition(bad)
    assert any("at least one condition" in e for e in errors)


def test_validate_definition_requires_matched_action():
    bad = dict(VALID_DEFINITION)
    bad["actions"] = {}
    errors = validate_definition(bad)
    assert any("actions.matched" in e for e in errors)


# ── CRUD / versioning ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_save_new_version_creates_v1_and_activates():
    await _clean("studio_test_strategy")
    result = await strategy_studio.save_new_version("studio_test_strategy", VALID_DEFINITION)
    assert result["version"] == 1
    assert result["is_active"] is True

    active = await strategy_studio.get_active("studio_test_strategy")
    assert active["version"] == 1
    await _clean("studio_test_strategy")


@pytest.mark.asyncio
async def test_save_new_version_increments_and_deactivates_previous():
    await _clean("studio_test_strategy")
    await strategy_studio.save_new_version("studio_test_strategy", VALID_DEFINITION)
    v2_def = dict(VALID_DEFINITION)
    v2_def["description"] = "v2 edit"
    result = await strategy_studio.save_new_version("studio_test_strategy", v2_def)
    assert result["version"] == 2

    versions = await strategy_studio.list_versions("studio_test_strategy")
    assert len(versions) == 2
    active_versions = [v for v in versions if v["is_active"]]
    assert len(active_versions) == 1
    assert active_versions[0]["version"] == 2
    await _clean("studio_test_strategy")


@pytest.mark.asyncio
async def test_save_new_version_rejects_invalid_definition():
    await _clean("studio_test_strategy")
    bad = dict(VALID_DEFINITION)
    bad["actions"] = {}
    with pytest.raises(StrategyValidationError):
        await strategy_studio.save_new_version("studio_test_strategy", bad)
    await _clean("studio_test_strategy")


@pytest.mark.asyncio
async def test_clone_strategy_copies_definition_under_new_name():
    await _clean("studio_test_strategy")
    await _clean("studio_test_strategy_clone")
    await strategy_studio.save_new_version("studio_test_strategy", VALID_DEFINITION)

    result = await strategy_studio.clone_strategy("studio_test_strategy", "studio_test_strategy_clone")
    assert result["strategy_name"] == "studio_test_strategy_clone"
    assert result["version"] == 1

    cloned = await strategy_studio.get_active("studio_test_strategy_clone")
    assert cloned["definition"]["name"] == "studio_test_strategy_clone"
    assert cloned["definition"]["rules"] == VALID_DEFINITION["rules"]

    await _clean("studio_test_strategy")
    await _clean("studio_test_strategy_clone")


@pytest.mark.asyncio
async def test_activate_version_switches_active_flag():
    await _clean("studio_test_strategy")
    await strategy_studio.save_new_version("studio_test_strategy", VALID_DEFINITION)
    await strategy_studio.save_new_version("studio_test_strategy", VALID_DEFINITION)  # v2, active

    await strategy_studio.activate_version("studio_test_strategy", 1)
    active = await strategy_studio.get_active("studio_test_strategy")
    assert active["version"] == 1
    await _clean("studio_test_strategy")


@pytest.mark.asyncio
async def test_load_from_db_registers_active_strategy_in_live_engine():
    await _clean("studio_test_strategy")
    await strategy_studio.save_new_version("studio_test_strategy", VALID_DEFINITION)

    await strategy_engine.load_from_db()
    registered = strategy_engine.registry.get_strategy("studio_test_strategy")
    assert registered is not None
    assert registered.enabled is True

    await strategy_studio.delete_strategy("studio_test_strategy")
    await strategy_engine.load_from_db()
    assert strategy_engine.registry.get_strategy("studio_test_strategy") is None
