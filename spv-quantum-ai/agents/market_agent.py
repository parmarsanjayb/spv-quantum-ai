from typing import List, Optional
from core.agent import BaseAgent, AgentResultModel
from core.bus import EventModel
from market.manager import market_data_manager
from market.models import MarketSession
from market.persistence import market_data_persistence

class MarketAgent(BaseAgent):
    """
    Thin agent wrapper that starts and stops the Market Data Engine.
    Is the exclusive publisher of all market data to the Event Bus.
    No other agent, broker, or strategy connects to any feed directly.
    """

    def __init__(self) -> None:
        super().__init__(
            name="market_agent",
            description="Market Data Engine controller – sole source of market data for the OS"
        )

    @property
    def input_event_types(self) -> List[str]:
        return ["market_control", "symbol_changed"]

    @property
    def output_event_types(self) -> List[str]:
        return ["tick", "candle", "option_chain_updated",
                "market_open", "market_close", "market_status_changed",
                "feed_connected", "feed_disconnected"]

    async def initialize(self) -> None:
        await market_data_manager.start()
        await market_data_persistence.start()
        self.log_info("Market Data Engine started by MarketAgent.")

    async def shutdown(self) -> None:
        await market_data_persistence.stop()
        await market_data_manager.stop()
        self.log_info("Market Data Engine stopped by MarketAgent.")

    async def analyze(self, event: EventModel) -> Optional[AgentResultModel]:
        if event.event_type == "symbol_changed":
            sym    = event.payload.get("symbol")
            action = event.payload.get("action", "").upper()
            if sym and action == "ADD":
                market_data_manager.registry.register(sym)
            elif sym and action == "REMOVE":
                market_data_manager.registry.unregister(sym)
            self.log_info(f"Symbol registry: {action} {sym}")

        return AgentResultModel(
            agent_name=self.agent_name,
            signal="NONE", confidence=100.0,
            reason="Market control event processed.",
            processing_time=0.0,
        )
