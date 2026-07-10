import asyncio
import math
from datetime import datetime, timezone
from statistics import pstdev
from typing import Any, Dict, List, Optional
import uuid

from core.bus import event_bus, EventModel
from core.logging import get_logger
from brokers.manager import broker_manager

from backtest.models import BacktestConfig, BacktestProgress
from backtest.loader import HistoricalDataLoader
from backtest.publisher import BacktestPublisher

logger = get_logger("backtest_engine")

class BacktestingEngine:
    """
    Enterprise Backtesting Engine.
    Simulates the complete trading system exactly as Live Trading works by replaying
    historical candles sequentially on the Event Bus.
    """
    def __init__(self) -> None:
        self.loader = HistoricalDataLoader()
        self.publisher = BacktestPublisher()
        
        self.progress = BacktestProgress(backtest_id="", status="PENDING")
        self.last_result: Dict[str, Any] = {}
        self._running = False
        self._current_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._running = True
        logger.info("BacktestingEngine ready.")

    async def stop(self) -> None:
        self._running = False
        if self._current_task:
            self._current_task.cancel()
            try:
                await self._current_task
            except asyncio.CancelledError:
                pass
            self._current_task = None
        logger.info("BacktestingEngine stopped.")

    async def run_backtest(self, config: BacktestConfig) -> str:
        """
        Starts a background task executing the sequential replay.
        """
        backtest_id = f"BKT-{uuid.uuid4().hex[:8]}"
        self.progress = BacktestProgress(
            backtest_id=backtest_id,
            status="RUNNING",
            progress_pct=0.0
        )
        
        # Stop any active run
        if self._current_task:
            self._current_task.cancel()
            
        self._current_task = asyncio.create_task(self._execute_simulation(backtest_id, config))
        return backtest_id

    async def _execute_simulation(self, backtest_id: str, config: BacktestConfig) -> None:
        # SAFETY: broker_manager.load("paper_broker") only connects paper_broker
        # into the pool — it does NOT make it the active broker. If the user has
        # kotak_neo (real money) selected as active, get_active() right after
        # would still return the REAL broker, and every simulated trade below
        # would place a genuine live order. A backtest must be physically
        # incapable of touching a real broker regardless of what's selected
        # elsewhere in the app, so we force-pin paper_broker for the duration
        # of this run and always restore whatever was active before, even on
        # cancellation/failure.
        original_broker_name = broker_manager._active_broker_name
        original_enabled_states: Dict[str, bool] = {}
        from strategies.engine import strategy_engine
        try:
            # The Backtest engine never knows or cares how a strategy was
            # authored (YAML file or Strategy Studio) — it only asks the
            # registry for it by name and isolates the run to that one
            # strategy, so results reflect that strategy alone rather than
            # whatever else happens to be enabled globally. Done before
            # publish_started so "started" always reflects the true isolated
            # state the run will actually execute under.
            if config.strategy_name:
                for s in strategy_engine.registry.get_all():
                    original_enabled_states[s.name] = s.enabled
                    s.enabled = (s.name == config.strategy_name)

            logger.info(f"Starting backtest {backtest_id}...")
            await self.publisher.publish_started(backtest_id, config)

            # 1. Reset states to prevent pollution
            # Reset PaperBroker
            await broker_manager.load("paper_broker")
            broker_manager._active_broker_name = "paper_broker"
            broker = broker_manager.get_active()
            if hasattr(broker, "_positions"):
                broker._positions.clear()
                broker._orders.clear()
                broker._trades.clear()
                broker._balance = config.initial_capital
                broker._used_margin = 0.0
                broker._partial_fill_rate = 0.0
                broker._rejection_rate = 0.0

            # Reset PortfolioEngine positions
            from portfolio.engine import portfolio_engine
            async with portfolio_engine.positions._lock:
                portfolio_engine.positions._positions.clear()
            portfolio_engine.summary = portfolio_engine.summary.__class__()

            # Reset TradeJournalEngine lists
            from journal.engine import trade_journal_engine
            async with trade_journal_engine._lock:
                trade_journal_engine._active_trades.clear()
            from journal.repository import TradeHistoryRepository
            TradeHistoryRepository._in_memory_journal.clear()

            # Ensure execution engine is active
            from execution.engine import execution_engine
            await execution_engine.start()

            # 2. Fetch all historical candles
            all_candles = []
            for symbol in config.symbols:
                candles = await self.loader.load_candles(symbol, config.timeframe, config.start_date, config.end_date)
                all_candles.extend(candles)
                
            # Sort candles chronologically so they are replayed in order
            all_candles.sort(key=lambda c: c.timestamp)
            total_count = len(all_candles)
            
            logger.info(f"Loaded {total_count} historical candles for simulation.")

            if total_count == 0:
                self.progress.status = "COMPLETED"
                self.progress.progress_pct = 100.0
                no_data_metrics = {"message": "No data found"}
                self.last_result = {
                    "backtest_id": backtest_id,
                    "strategy_name": config.strategy_name,
                    "metrics": no_data_metrics,
                    "verdict": {
                        "label": "NO_DATA",
                        "headline": "No real historical data available",
                        "detail": "No recorded candles exist yet for the selected symbol/timeframe/date range — "
                                  "the platform only backtests against real data it has actually collected.",
                    },
                    "equity_curve": [],
                    "trade_log": [],
                }
                await self.publisher.publish_completed(backtest_id, self.progress, no_data_metrics)
                return

            # 3. Sequential replay loop
            from market.manager import market_data_manager
            
            for index, candle in enumerate(all_candles):
                if not self._running:
                    break
                    
                # Update progress status
                self.progress.current_symbol = candle.symbol
                self.progress.current_date = candle.timestamp
                self.progress.progress_pct = round((index + 1) / total_count * 100.0, 2)
                
                # Mock tick price update for the portfolio & scanners
                from market.models import MarketData
                tick = MarketData(
                    symbol=candle.symbol,
                    ltp=candle.close,
                    prev_close=candle.open,
                    volume=candle.volume,
                    timestamp=candle.timestamp
                )
                await market_data_manager.cache.update_tick(tick)
                
                # Publish tick event (unrealized PNL update)
                await event_bus.publish(EventModel(
                    event_type="tick",
                    source_agent="market_data_engine",
                    payload=tick.model_dump(mode="json")
                ))

                # Publish completed candle event
                await event_bus.publish(EventModel(
                    event_type="candle",
                    source_agent="market_data_engine",
                    payload={"candle": candle.model_dump(mode="json")}
                ))

                # Yield control to let async event handlers validate and execute trades
                await asyncio.sleep(0.01)

                # Update live stats
                stats = await trade_journal_engine.get_performance_stats()
                self.progress.trades_executed = stats.get("total_trades", 0)
                self.progress.total_pnl = stats.get("total_realized_pnl", 0.0)
                
                # Periodically publish progress event
                if index % 10 == 0:
                    await self.publisher.publish_progress(self.progress)

            # 4. Finalize
            self.progress.status = "COMPLETED"
            self.progress.progress_pct = 100.0
            
            stats = await trade_journal_engine.get_performance_stats()
            trades = await trade_journal_engine.repo.get_all_trades()
            risk_metrics = self._compute_risk_metrics(trades, config.initial_capital)
            loss_trades = [t for t in trades if t.realized_pnl < 0]
            win_trades = [t for t in trades if t.realized_pnl > 0]
            metrics = {
                "total_trades": stats.get("total_trades", 0),
                "winning_trades": len(win_trades),
                "losing_trades": len(loss_trades),
                "win_rate_pct": stats.get("win_rate", 0.0),
                "loss_rate_pct": round(100.0 - stats.get("win_rate", 0.0), 2) if trades else 0.0,
                "net_profit_loss": stats.get("total_realized_pnl", 0.0),
                "sharpe_ratio": risk_metrics["sharpe_ratio"],
                "drawdown_pct": risk_metrics["drawdown_pct"],
                "profit_factor": risk_metrics["profit_factor"],
            }

            self.last_result = {
                "backtest_id": backtest_id,
                "strategy_name": config.strategy_name,
                "metrics": metrics,
                "verdict": self._compute_verdict(metrics),
                "equity_curve": risk_metrics["equity_curve"],
                "trade_log": self._build_trade_log(trades),
            }

            await self.publisher.publish_completed(backtest_id, self.progress, metrics)
            logger.info(f"Backtest {backtest_id} complete. Trades: {metrics['total_trades']}, PNL: {metrics['net_profit_loss']}")

        except asyncio.CancelledError:
            self.progress.status = "FAILED"
            logger.warning(f"Backtest {backtest_id} cancelled.")
        except Exception as e:
            self.progress.status = "FAILED"
            logger.error(f"Backtest {backtest_id} failed: {e}")
            raise e
        finally:
            broker_manager._active_broker_name = original_broker_name
            if original_enabled_states:
                for s in strategy_engine.registry.get_all():
                    if s.name in original_enabled_states:
                        s.enabled = original_enabled_states[s.name]

    @staticmethod
    def _compute_risk_metrics(trades: List[Any], initial_capital: float) -> Dict[str, Any]:
        """
        Computes Sharpe ratio, max drawdown, profit factor, and the equity
        curve from the actual sequence of closed trades produced by this
        backtest run — no placeholder values. Sharpe uses day-over-day
        equity returns annualized over 252 trading days, assuming a 0%
        risk-free rate (standard simplification, not a fabricated result).
        """
        if not trades:
            return {"sharpe_ratio": 0.0, "drawdown_pct": 0.0, "profit_factor": 0.0, "equity_curve": []}

        sorted_trades = sorted(trades, key=lambda t: t.timestamp)

        equity = initial_capital
        peak = initial_capital
        max_dd_pct = 0.0
        daily_equity: Dict[Any, float] = {}
        equity_curve: List[Dict[str, Any]] = [
            {"timestamp": sorted_trades[0].timestamp.isoformat(), "equity": round(initial_capital, 2)}
        ]
        gross_profit = 0.0
        gross_loss = 0.0
        for t in sorted_trades:
            equity += t.realized_pnl
            peak = max(peak, equity)
            if peak > 0:
                max_dd_pct = max(max_dd_pct, (peak - equity) / peak * 100.0)
            daily_equity[t.timestamp.date()] = equity
            equity_curve.append({"timestamp": t.timestamp.isoformat(), "equity": round(equity, 2)})
            if t.realized_pnl > 0:
                gross_profit += t.realized_pnl
            elif t.realized_pnl < 0:
                gross_loss += abs(t.realized_pnl)

        daily_values = [initial_capital] + [v for _, v in sorted(daily_equity.items())]
        daily_returns = [
            (daily_values[i] - daily_values[i - 1]) / daily_values[i - 1]
            for i in range(1, len(daily_values))
            if daily_values[i - 1] != 0
        ]

        sharpe = 0.0
        if len(daily_returns) >= 2:
            mean_r = sum(daily_returns) / len(daily_returns)
            std_r = pstdev(daily_returns)
            if std_r > 0:
                sharpe = (mean_r / std_r) * math.sqrt(252)

        # Conventionally undefined (infinite) with zero losses — reported as
        # None rather than a fabricated large number; the UI shows "N/A".
        profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else None

        return {
            "sharpe_ratio": round(sharpe, 2),
            "drawdown_pct": round(max_dd_pct, 2),
            "profit_factor": profit_factor,
            "equity_curve": equity_curve,
        }

    @staticmethod
    def _build_trade_log(trades: List[Any]) -> List[Dict[str, Any]]:
        """Complete per-trade log for the UI/API — entry/exit price, qty,
        pnl, and holding duration for every closed trade in this run."""
        sorted_trades = sorted(trades, key=lambda t: t.timestamp)
        return [
            {
                "symbol": t.symbol,
                "side": t.side,
                "entry_price": t.entry_price,
                "exit_price": t.exit_price,
                "quantity": t.quantity,
                "realized_pnl": round(t.realized_pnl, 2),
                "net_pnl": round(t.net_pnl, 2),
                "holding_duration_sec": t.holding_duration,
                "timestamp": t.timestamp.isoformat(),
            }
            for t in sorted_trades
        ]

    @staticmethod
    def _compute_verdict(metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Plain-language profitable/not-profitable verdict for a non-technical
        user, derived from the real metrics above — never a guess.
        """
        total_trades = metrics["total_trades"]
        net_pnl = metrics["net_profit_loss"]
        win_rate = metrics["win_rate_pct"]
        drawdown = metrics["drawdown_pct"]

        if total_trades == 0:
            return {
                "label": "NO_DATA",
                "headline": "Not enough trades to judge",
                "detail": "This backtest produced zero completed trades in the selected range — "
                          "there isn't enough history yet to say whether the strategy is profitable.",
            }

        low_sample = total_trades < 10
        sample_note = (
            f" (only {total_trades} trades — treat this as a rough signal, not a reliable verdict)"
            if low_sample else f" (based on {total_trades} trades)"
        )

        if net_pnl > 0:
            label = "PROFITABLE"
            headline = f"Profitable{sample_note}"
        elif net_pnl == 0:
            label = "BREAK_EVEN"
            headline = f"Broke even{sample_note}"
        else:
            label = "NOT_PROFITABLE"
            headline = f"Not profitable{sample_note}"

        detail = (
            f"Net P&L: {net_pnl:.2f} | Win rate: {win_rate:.1f}% | "
            f"Max drawdown: {drawdown:.1f}% | Sharpe: {metrics['sharpe_ratio']:.2f}"
        )
        return {"label": label, "headline": headline, "detail": detail}

    async def get_dashboard_status(self) -> Dict[str, Any]:
        """Returns backtest progress and status metrics."""
        return {
            "backtest_id": self.progress.backtest_id,
            "status": self.progress.status,
            "progress_pct": self.progress.progress_pct,
            "current_symbol": self.progress.current_symbol,
            "current_date": self.progress.current_date.isoformat() if self.progress.current_date else None,
            "trades_executed": self.progress.trades_executed,
            "total_pnl": self.progress.total_pnl
        }

# Singleton
backtesting_engine = BacktestingEngine()
