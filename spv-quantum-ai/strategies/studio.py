from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from sqlalchemy import select, func, update
from pydantic import ValidationError

from database.connection import async_session
from database.models import StrategyDefinitionModel
from strategies.models import Strategy, RuleGroup
from indicators.registry import INDICATOR_REGISTRY
from core.logging import get_logger

logger = get_logger("strategy_studio")

VALID_SOURCES = {"indicator", "market_regime", "risk_status", "market_data", "time", "session"}
VALID_OPERATORS = {">", "<", ">=", "<=", "==", "!=", "between", "inside_range", "outside_range", "crosses_above", "crosses_below"}


class StrategyValidationError(Exception):
    def __init__(self, errors: List[str]) -> None:
        self.errors = errors
        super().__init__("; ".join(errors))


def _walk_conditions(group: Dict[str, Any], errors: List[str], path: str) -> None:
    operator = group.get("operator", "")
    if operator.upper() not in ("AND", "OR", "NOT"):
        errors.append(f"{path}: group operator must be AND/OR/NOT, got '{operator}'")

    conditions = group.get("conditions") or []
    if not conditions:
        errors.append(f"{path}: must have at least one condition")

    for i, cond in enumerate(conditions):
        sub_path = f"{path}.conditions[{i}]"
        if "conditions" in cond:  # nested group
            _walk_conditions(cond, errors, sub_path)
            continue
        source = cond.get("source")
        if source not in VALID_SOURCES:
            errors.append(f"{sub_path}: unknown source '{source}' (must be one of {sorted(VALID_SOURCES)})")
        op = cond.get("operator", "")
        if op not in VALID_OPERATORS:
            errors.append(f"{sub_path}: unknown operator '{op}'")
        if source == "indicator":
            key = cond.get("key")
            if key and key not in INDICATOR_REGISTRY:
                errors.append(f"{sub_path}: indicator '{key}' is not in INDICATOR_REGISTRY")
            target = cond.get("target")
            if target and target not in INDICATOR_REGISTRY:
                errors.append(f"{sub_path}: target indicator '{target}' is not in INDICATOR_REGISTRY")
        if op in ("crosses_above", "crosses_below") and not cond.get("target") and cond.get("value") is None:
            errors.append(f"{sub_path}: '{op}' needs either a target indicator or a comparison value")


def validate_definition(definition: Dict[str, Any]) -> List[str]:
    """
    Validates a raw strategy definition dict. Returns a list of human-
    readable errors (empty list = valid). Two layers: pydantic schema
    shape (Strategy/RuleGroup/Condition), then semantic checks (real
    indicators, sane operators) the schema alone can't catch.
    """
    errors: List[str] = []

    try:
        Strategy(**definition)
    except ValidationError as e:
        for err in e.errors():
            loc = ".".join(str(p) for p in err["loc"])
            errors.append(f"{loc}: {err['msg']}")
        return errors  # shape is broken; semantic checks would just be noise

    rules = definition.get("rules")
    if rules:
        _walk_conditions(rules, errors, "rules")
    exit_rules = definition.get("exit_rules")
    if exit_rules:
        _walk_conditions(exit_rules, errors, "exit_rules")

    actions = definition.get("actions") or {}
    if "matched" not in actions:
        errors.append("actions.matched is required (what to do when entry rules match)")
    if exit_rules and "exit" not in actions:
        errors.append("actions.exit is required when exit_rules is defined")

    return errors


class StrategyStudio:
    """
    CRUD + versioning service backing the visual Strategy Studio. Every save
    is a new immutable version; exactly one version per strategy_name is
    active. This is purely a persistence/authoring layer — evaluation stays
    in strategies/engine.py, unchanged, via the same Strategy schema.
    """

    async def validate(self, definition: Dict[str, Any]) -> List[str]:
        return validate_definition(definition)

    async def list_strategies(self) -> List[Dict[str, Any]]:
        async with async_session() as session:
            result = await session.execute(
                select(StrategyDefinitionModel).where(StrategyDefinitionModel.is_active == True)  # noqa: E712
            )
            active_versions = result.scalars().all()

            result_all = await session.execute(
                select(
                    StrategyDefinitionModel.strategy_name,
                    func.max(StrategyDefinitionModel.version).label("latest_version"),
                    func.count(StrategyDefinitionModel.id).label("version_count"),
                ).group_by(StrategyDefinitionModel.strategy_name)
            )
            counts = {row.strategy_name: row for row in result_all.all()}

        return [
            {
                "strategy_name": v.strategy_name,
                "active_version": v.version,
                "latest_version": counts[v.strategy_name].latest_version if v.strategy_name in counts else v.version,
                "version_count": counts[v.strategy_name].version_count if v.strategy_name in counts else 1,
                "description": v.description,
                "enabled": (v.definition or {}).get("enabled", True),
            }
            for v in active_versions
        ]

    async def list_versions(self, strategy_name: str) -> List[Dict[str, Any]]:
        async with async_session() as session:
            result = await session.execute(
                select(StrategyDefinitionModel)
                .where(StrategyDefinitionModel.strategy_name == strategy_name)
                .order_by(StrategyDefinitionModel.version.desc())
            )
            rows = result.scalars().all()
        return [
            {
                "id": r.id, "version": r.version, "is_active": r.is_active,
                "description": r.description, "definition": r.definition,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ]

    async def get_active(self, strategy_name: str) -> Optional[Dict[str, Any]]:
        async with async_session() as session:
            result = await session.execute(
                select(StrategyDefinitionModel).where(
                    StrategyDefinitionModel.strategy_name == strategy_name,
                    StrategyDefinitionModel.is_active == True,  # noqa: E712
                )
            )
            row = result.scalars().first()
        if not row:
            return None
        return {"version": row.version, "definition": row.definition, "description": row.description}

    async def save_new_version(self, strategy_name: str, definition: Dict[str, Any], activate: bool = True) -> Dict[str, Any]:
        errors = validate_definition(definition)
        if errors:
            raise StrategyValidationError(errors)

        async with async_session() as session:
            result = await session.execute(
                select(func.max(StrategyDefinitionModel.version)).where(
                    StrategyDefinitionModel.strategy_name == strategy_name
                )
            )
            max_version = result.scalar()
            next_version = (max_version or 0) + 1

            if activate:
                await session.execute(
                    update(StrategyDefinitionModel)
                    .where(StrategyDefinitionModel.strategy_name == strategy_name)
                    .values(is_active=False)
                )

            row = StrategyDefinitionModel(
                strategy_name=strategy_name,
                version=next_version,
                is_active=activate,
                description=definition.get("description"),
                definition=definition,
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)

        logger.info(f"Saved strategy '{strategy_name}' v{next_version} (active={activate})")
        if activate:
            await self._reload_engine()
        return {"strategy_name": strategy_name, "version": next_version, "is_active": activate}

    async def clone_strategy(self, source_name: str, new_name: str) -> Dict[str, Any]:
        source = await self.get_active(source_name)
        if not source:
            raise ValueError(f"No active version found for strategy '{source_name}'")
        existing = await self.get_active(new_name)
        if existing:
            raise ValueError(f"A strategy named '{new_name}' already exists")

        cloned_def = dict(source["definition"])
        cloned_def["name"] = new_name
        return await self.save_new_version(new_name, cloned_def, activate=True)

    async def activate_version(self, strategy_name: str, version: int) -> Dict[str, Any]:
        async with async_session() as session:
            result = await session.execute(
                select(StrategyDefinitionModel).where(
                    StrategyDefinitionModel.strategy_name == strategy_name,
                    StrategyDefinitionModel.version == version,
                )
            )
            target = result.scalars().first()
            if not target:
                raise ValueError(f"Strategy '{strategy_name}' version {version} not found")

            await session.execute(
                update(StrategyDefinitionModel)
                .where(StrategyDefinitionModel.strategy_name == strategy_name)
                .values(is_active=False)
            )
            target.is_active = True
            await session.commit()

        logger.info(f"Activated strategy '{strategy_name}' v{version}")
        await self._reload_engine()
        return {"strategy_name": strategy_name, "version": version, "is_active": True}

    async def delete_strategy(self, strategy_name: str) -> None:
        from sqlalchemy import delete
        async with async_session() as session:
            await session.execute(
                delete(StrategyDefinitionModel).where(StrategyDefinitionModel.strategy_name == strategy_name)
            )
            await session.commit()
        logger.info(f"Deleted strategy '{strategy_name}' (all versions)")
        await self._reload_engine()

    @staticmethod
    async def _reload_engine() -> None:
        """Hot-reloads the live StrategyEngine registry so a save/activate/
        delete takes effect immediately — no server restart needed."""
        from strategies.engine import strategy_engine
        await strategy_engine.load_from_db()

    async def get_ui_schema(self) -> Dict[str, Any]:
        """Everything the form-based builder needs for its dropdowns —
        keeps the frontend from hardcoding values that already live
        server-side (indicator registry, valid operators/sources)."""
        return {
            "sources": sorted(VALID_SOURCES),
            "operators": sorted(VALID_OPERATORS),
            "indicators": [
                {"key": k, "family": v.get("family"), "description": v.get("description")}
                for k, v in INDICATOR_REGISTRY.items()
            ],
            "actions": ["SIGNAL_BUY", "SIGNAL_SELL", "SIGNAL_NONE"],
            "group_operators": ["AND", "OR", "NOT"],
        }


# Singleton
strategy_studio = StrategyStudio()
