import asyncio
import os
import psutil
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.agent import BaseAgent, AgentResultModel
from core.bus import event_bus, EventModel
from core.logging import get_logger
from database.connection import async_session, engine
from database.models import AgentReportModel

logger = get_logger("chief_technology_officer_agent")

class ChiefTechnologyOfficerAgent(BaseAgent):
    """
    Chief Technology Officer (CTO) Agent.
    Monitors host system health, database latency, API states, and agent execution threads.
    Auto-recovers failed background tasks and worker agents.
    """
    def __init__(self) -> None:
        super().__init__(
            name="chief_technology_officer_agent",
            description="Monitors system resources, CPU, RAM, thread health, and recovers failed workers"
        )
        self._monitor_task: Optional[asyncio.Task] = None
        self._running = False
        self.recovery_count = 0

    @property
    def input_event_types(self) -> List[str]:
        return []

    @property
    def output_event_types(self) -> List[str]:
        return ["cto_telemetry"]

    async def initialize(self) -> None:
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitoring_loop())
        self.log_info("ChiefTechnologyOfficerAgent initialized and monitoring loop started.")

    async def shutdown(self) -> None:
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            self._monitor_task = None
        self.log_info("ChiefTechnologyOfficerAgent stopped.")

    async def analyze(self, event: EventModel) -> Optional[AgentResultModel]:
        return None

    async def _monitoring_loop(self) -> None:
        await asyncio.sleep(5)  # Wait for startup
        while self._running:
            try:
                await self._perform_telemetry_check()
            except Exception as e:
                self.log_error(f"Error in CTO monitoring loop: {e}")
            await asyncio.sleep(5)

    async def _perform_telemetry_check(self) -> None:
        # 1. System stats
        cpu_usage = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory()
        mem_usage = mem.percent
        
        # Disk usage of current drive
        disk = psutil.disk_usage(os.getcwd())
        disk_usage = disk.percent
        
        # 2. Database latency check
        db_start = time.perf_counter()
        db_status = "HEALTHY"
        db_latency = 0.0
        try:
            async with engine.connect() as conn:
                await conn.execute("SELECT 1")
            db_latency = (time.perf_counter() - db_start) * 1000.0
        except Exception as e:
            db_status = "FAILED"
            db_latency = 999.0
            
        # 3. Supervise active agents
        from agents.manager import AgentManager
        # Access agents via parent/global scope if instantiated
        from dashboard.main import agent_manager
        
        agents_health = {}
        for name, agent in list(agent_manager.active_agents.items()):
            if agent.enabled:
                health = await agent.health_check()
                agents_health[name] = {
                    "status": agent.status,
                    "health": health
                }
                
                # Auto-recover if status failed
                if agent.status == "FAILED" or health == "UNHEALTHY":
                    self.log_warn(f"CTO: Detected unhealthy agent '{name}'. Triggering auto-recovery...")
                    self.recovery_count += 1
                    # Recover via agent_manager.audit_health_and_supervise()
                    asyncio.create_task(agent_manager.audit_health_and_supervise())

        telemetry_payload = {
            "cpu_usage_pct": cpu_usage,
            "memory_usage_pct": mem_usage,
            "disk_usage_pct": disk_usage,
            "database_status": db_status,
            "database_latency_ms": db_latency,
            "agents_status": agents_health,
            "recovery_count": self.recovery_count,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        # Broadcast CTO Telemetry
        await event_bus.publish(EventModel(
            event_type="cto_telemetry",
            source_agent=self.agent_name,
            payload=telemetry_payload
        ))

        # Save to database
        await self._save_telemetry_to_db(telemetry_payload)

    async def _save_telemetry_to_db(self, record: Dict[str, Any]) -> None:
        try:
            async with async_session() as session:
                db_entry = AgentReportModel(
                    agent_name=self.agent_name,
                    report_type="cto_health",
                    data=record
                )
                session.add(db_entry)
                await session.commit()
        except Exception as e:
            self.log_error(f"Failed to save CTO telemetry report: {e}")
