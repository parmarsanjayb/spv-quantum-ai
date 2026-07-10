import pytest
from sqlalchemy import delete
from database.connection import async_session
from database.models import IPOIssueModel, IPORecommendationModel, IPOPerformanceModel
from ipo.performance import ipo_performance_tracker, _judge


async def _clean(*symbols: str) -> None:
    async with async_session() as session:
        for model in (IPOIssueModel, IPORecommendationModel, IPOPerformanceModel):
            col = model.symbol if model is IPOIssueModel else model.ipo_symbol
            await session.execute(delete(model).where(col.in_(symbols)))
        await session.commit()


def test_judge_apply_correct_when_gain_positive():
    assert _judge("APPLY", 15.0) is True

def test_judge_apply_incorrect_when_gain_negative():
    assert _judge("APPLY", -5.0) is False

def test_judge_avoid_correct_when_gain_negative_or_flat():
    assert _judge("AVOID", -5.0) is True
    assert _judge("AVOID", 0.0) is True

def test_judge_avoid_incorrect_when_gain_positive():
    assert _judge("AVOID", 5.0) is False

def test_judge_wait_is_never_judged():
    assert _judge("WAIT", 20.0) is None
    assert _judge("WAIT", -20.0) is None

def test_judge_none_gain_returns_none():
    assert _judge("APPLY", None) is None


@pytest.mark.asyncio
async def test_evaluate_symbol_returns_none_if_not_listed():
    await _clean("PERFNOTLIST")
    async with async_session() as session:
        session.add(IPOIssueModel(symbol="PERFNOTLIST", company_name="X", status="OPEN"))
        session.add(IPORecommendationModel(
            ipo_symbol="PERFNOTLIST", recommendation="APPLY", confidence=80.0, reasoning="test",
        ))
        await session.commit()
    result = await ipo_performance_tracker.evaluate_symbol("PERFNOTLIST")
    assert result is None
    await _clean("PERFNOTLIST")


@pytest.mark.asyncio
async def test_evaluate_symbol_computes_real_gain_and_correctness():
    await _clean("PERFLISTED")
    async with async_session() as session:
        session.add(IPOIssueModel(
            symbol="PERFLISTED", company_name="X", status="LISTED",
            price_band_high=100.0, listing_price=130.0,
        ))
        session.add(IPORecommendationModel(
            ipo_symbol="PERFLISTED", recommendation="APPLY", confidence=80.0, reasoning="test",
        ))
        await session.commit()

    result = await ipo_performance_tracker.evaluate_symbol("PERFLISTED")
    assert result["listing_gain_pct"] == 30.0
    assert result["was_correct"] is True

    async with async_session() as session:
        from sqlalchemy import select
        perf = (await session.execute(
            select(IPOPerformanceModel).where(IPOPerformanceModel.ipo_symbol == "PERFLISTED")
        )).scalars().first()
        assert perf is not None
        assert perf.listing_gain_pct == 30.0
    await _clean("PERFLISTED")


@pytest.mark.asyncio
async def test_accuracy_summary_aggregates_correctly():
    await _clean("ACC1", "ACC2", "ACC3")
    async with async_session() as session:
        session.add(IPOIssueModel(symbol="ACC1", company_name="A1", status="LISTED", price_band_high=100.0, listing_price=120.0))
        session.add(IPORecommendationModel(ipo_symbol="ACC1", recommendation="APPLY", confidence=80.0, reasoning="t"))
        session.add(IPOIssueModel(symbol="ACC2", company_name="A2", status="LISTED", price_band_high=100.0, listing_price=90.0))
        session.add(IPORecommendationModel(ipo_symbol="ACC2", recommendation="AVOID", confidence=80.0, reasoning="t"))
        session.add(IPOIssueModel(symbol="ACC3", company_name="A3", status="LISTED", price_band_high=100.0, listing_price=80.0))
        session.add(IPORecommendationModel(ipo_symbol="ACC3", recommendation="APPLY", confidence=80.0, reasoning="t"))
        await session.commit()

    for sym in ("ACC1", "ACC2", "ACC3"):
        await ipo_performance_tracker.evaluate_symbol(sym)

    summary = await ipo_performance_tracker.get_accuracy_summary()
    assert summary["total_judged"] >= 3
    assert summary["by_recommendation"]["APPLY"]["total"] >= 2
    await _clean("ACC1", "ACC2", "ACC3")
