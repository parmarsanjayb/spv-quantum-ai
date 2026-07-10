import pytest
from sqlalchemy import delete, select
from database.connection import async_session
from database.models import IPOIssueModel, IPOSubscriptionSnapshotModel, IPOAnalystReportModel, IPORecommendationModel
from ipo.analysts import SubscriptionAnalyst, ValuationAnalyst, IssueSizeAnalyst
from ipo.ceo import ipo_ceo


async def _clean(symbol: str) -> None:
    async with async_session() as session:
        await session.execute(delete(IPOIssueModel).where(IPOIssueModel.symbol == symbol))
        await session.execute(delete(IPOSubscriptionSnapshotModel).where(IPOSubscriptionSnapshotModel.ipo_symbol == symbol))
        await session.execute(delete(IPOAnalystReportModel).where(IPOAnalystReportModel.ipo_symbol == symbol))
        await session.execute(delete(IPORecommendationModel).where(IPORecommendationModel.ipo_symbol == symbol))
        await session.commit()


async def _seed_issue(symbol: str, **kwargs) -> IPOIssueModel:
    kwargs.setdefault("status", "OPEN")
    async with async_session() as session:
        issue = IPOIssueModel(symbol=symbol, company_name=f"{symbol} Co", **kwargs)
        session.add(issue)
        await session.commit()
        await session.refresh(issue)
        return issue


# ── SubscriptionAnalyst ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_subscription_analyst_returns_none_without_real_data():
    await _clean("SUBNONE")
    issue = await _seed_issue("SUBNONE")
    report = await SubscriptionAnalyst().analyze(issue)
    assert report is None
    await _clean("SUBNONE")


@pytest.mark.asyncio
async def test_subscription_analyst_scores_oversubscription_high():
    await _clean("SUBHIGH")
    issue = await _seed_issue("SUBHIGH")
    async with async_session() as session:
        session.add(IPOSubscriptionSnapshotModel(ipo_symbol="SUBHIGH", category="Total", subscription_times=15.0))
        await session.commit()
    report = await SubscriptionAnalyst().analyze(issue)
    assert report["score"] >= 85.0
    await _clean("SUBHIGH")


@pytest.mark.asyncio
async def test_subscription_analyst_scores_undersubscription_low():
    await _clean("SUBLOW")
    issue = await _seed_issue("SUBLOW")
    async with async_session() as session:
        session.add(IPOSubscriptionSnapshotModel(ipo_symbol="SUBLOW", category="Total", subscription_times=0.5))
        await session.commit()
    report = await SubscriptionAnalyst().analyze(issue)
    assert report["score"] <= 25.0
    assert len(report["risks"]) >= 1
    await _clean("SUBLOW")


# ── ValuationAnalyst ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_valuation_analyst_returns_none_without_price_band():
    issue = IPOIssueModel(symbol="X", company_name="X", status="OPEN")
    report = await ValuationAnalyst().analyze(issue)
    assert report is None


@pytest.mark.asyncio
async def test_valuation_analyst_scores_tight_band_higher():
    tight = IPOIssueModel(symbol="TIGHT", company_name="T", status="OPEN", price_band_low=100.0, price_band_high=102.0)
    wide = IPOIssueModel(symbol="WIDE", company_name="W", status="OPEN", price_band_low=100.0, price_band_high=120.0)
    tight_report = await ValuationAnalyst().analyze(tight)
    wide_report = await ValuationAnalyst().analyze(wide)
    assert tight_report["score"] > wide_report["score"]


# ── IssueSizeAnalyst ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_issue_size_analyst_flags_sme_with_liquidity_risk():
    sme = IPOIssueModel(symbol="SME1", company_name="S", status="OPEN", security_type="SME",
                          issue_size=100000.0, price_band_high=100.0)
    report = await IssueSizeAnalyst().analyze(sme)
    assert report is not None
    assert any("liquid" in r.lower() for r in report["risks"])


@pytest.mark.asyncio
async def test_issue_size_analyst_returns_none_without_real_size_data():
    issue = IPOIssueModel(symbol="NOSIZE", company_name="N", status="OPEN")
    report = await IssueSizeAnalyst().analyze(issue)
    assert report is None


# ── IPO CEO ──────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_ceo_returns_wait_with_no_analyst_data():
    await _clean("CEOWAIT")
    await _seed_issue("CEOWAIT", status="UPCOMING")
    result = await ipo_ceo.analyze("CEOWAIT")
    assert result["recommendation"] == "WAIT"
    assert result["data_completeness_pct"] == 0.0
    await _clean("CEOWAIT")


@pytest.mark.asyncio
async def test_ceo_avoids_on_undersubscription_regardless_of_other_scores():
    await _clean("CEOAVOID")
    await _seed_issue("CEOAVOID", price_band_low=100.0, price_band_high=102.0, issue_size=1_000_000.0)
    async with async_session() as session:
        session.add(IPOSubscriptionSnapshotModel(ipo_symbol="CEOAVOID", category="Total", subscription_times=0.3))
        await session.commit()
    result = await ipo_ceo.analyze("CEOAVOID")
    assert result["recommendation"] == "AVOID"
    await _clean("CEOAVOID")


@pytest.mark.asyncio
async def test_ceo_never_recommends_long_term_investment_in_phase_1():
    """Architectural guarantee: without real fundamental/financial data,
    the CEO must never claim a long-term investment view."""
    await _clean("CEOSTRONG")
    await _seed_issue("CEOSTRONG", price_band_low=100.0, price_band_high=101.0, issue_size=10_000_000.0)
    async with async_session() as session:
        session.add(IPOSubscriptionSnapshotModel(ipo_symbol="CEOSTRONG", category="Total", subscription_times=50.0))
        await session.commit()
    result = await ipo_ceo.analyze("CEOSTRONG")
    assert result["recommendation"] != "LONG_TERM_INVESTMENT"
    assert "Phase 2" in result["reasoning"]
    await _clean("CEOSTRONG")


@pytest.mark.asyncio
async def test_ceo_persists_reports_and_recommendation_to_db():
    await _clean("CEOPERSIST")
    await _seed_issue("CEOPERSIST", price_band_low=100.0, price_band_high=105.0, issue_size=5_000_000.0)
    async with async_session() as session:
        session.add(IPOSubscriptionSnapshotModel(ipo_symbol="CEOPERSIST", category="Total", subscription_times=5.0))
        await session.commit()

    await ipo_ceo.analyze("CEOPERSIST")

    async with async_session() as session:
        reports = (await session.execute(
            select(IPOAnalystReportModel).where(IPOAnalystReportModel.ipo_symbol == "CEOPERSIST")
        )).scalars().all()
        rec = (await session.execute(
            select(IPORecommendationModel).where(IPORecommendationModel.ipo_symbol == "CEOPERSIST")
        )).scalars().first()

    assert len(reports) == 3
    assert rec is not None
    assert rec.recommendation in ("APPLY", "AVOID", "LISTING_GAIN_ONLY", "WAIT")
    await _clean("CEOPERSIST")


@pytest.mark.asyncio
async def test_ceo_raises_for_unknown_symbol():
    with pytest.raises(ValueError):
        await ipo_ceo.analyze("DEFINITELY_NOT_A_REAL_SYMBOL_XYZ")
