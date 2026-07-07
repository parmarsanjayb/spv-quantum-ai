// SPV Quantum AI Operating System - Dashboard Controller v2
document.addEventListener("DOMContentLoaded", () => {
    const apiBase = window.location.origin;
    const wsUri = `${window.location.protocol === "https:" ? "wss:" : "ws:"}//${window.location.host}/ws`;

    // Active state caches
    let activePrices = {};
    let allLogs = [];
    let activeLogFilter = "all";
    let activeEmployeeCode = null;
    let decisionLogs = [];
    let pnlChart = null;
    let accuracyChart = null;
    let ws;

    window.switchTab = function(tabId) {
        document.querySelectorAll(".nav-tabs .tab-btn").forEach(btn => {
            if (btn.id === `btn-${tabId}`) {
                btn.classList.add("active");
            } else {
                btn.classList.remove("active");
            }
        });
        document.querySelectorAll(".tab-panel").forEach(panel => {
            if (panel.id === tabId) {
                panel.classList.add("active");
            } else {
                panel.classList.remove("active");
            }
        });
        if (tabId === "tab-monitoring") {
            fetchEmployeeData();
        }
        if (tabId === "tab-performance") {
            fetchEmployeeData();
            fetchSystemAnalytics();
        }
        if (tabId === "tab-decisions") {
            fetchDecisionHistory();
        }
        if (tabId === "tab-settings") {
            fetchSystemSettings();
        }
    };

    // REST Interval loops
    let metricsInterval;
    let portfolioInterval;
    let telemetryInterval;

    // Initialize clock
    setInterval(() => {
        const now = new Date();
        document.getElementById("top-time").textContent = now.toLocaleTimeString();
    }, 1000);

    // Initial setup
    fetchInitialData();
    connectWs();
    setupEventListeners();

    // Setup WebSockets
    function connectWs() {
        console.log("Connecting WebSocket...");
        ws = new WebSocket(wsUri);

        ws.onopen = () => {
            document.getElementById("connection-status").textContent = "CONNECTED";
            document.getElementById("connection-status").className = "value text-green";
            document.getElementById("status-indicator").classList.remove("disconnected");
            appendTerminalLog("system", "websocket", "WebSocket stream established successfully.");
        };

        ws.onmessage = (event) => {
            const payload = JSON.parse(event.data);
            handleBusEvent(payload);
        };

        ws.onclose = () => {
            document.getElementById("connection-status").textContent = "DISCONNECTED";
            document.getElementById("connection-status").className = "value text-red";
            document.getElementById("status-indicator").classList.add("disconnected");
            appendTerminalLog("system", "websocket", "WebSocket disconnected. Reconnecting in 3s...", "color: var(--accent-red)");
            setTimeout(connectWs, 3000);
        };
    }

    // Event router
    function handleBusEvent(event) {
        const { topic, sender, timestamp, data } = event;
        const formattedTime = new Date(timestamp).toLocaleTimeString();

        // Save log in memory
        allLogs.push({ sender, topic, dataStr: JSON.stringify(data), timestamp });
        if (allLogs.length > 200) allLogs.shift();
        
        // Re-render logs if current filter matches
        if (activeLogFilter === "all" || matchesFilter(sender, topic, activeLogFilter)) {
            appendTerminalLog(sender, topic, JSON.stringify(data), getTopicStyle(topic));
        }

        // Live prices
        if (topic === "market_data" || topic === "tick") {
            updatePriceWidget(data.tick || data);
        } else if (topic === "order_filled" || topic === "paper_order_filled") {
            refreshTables();
            fetchEmployeeData();
            fetchSystemAnalytics();
        } else if (topic === "volume_intelligence_update") {
            updateVolumeIntelligenceUI(data);
        } else if (topic === "option_flow_updated") {
            updateOptionFlowUI(data);
        } else if (topic === "trend_updated") {
            updateTrendIntelUI(data);
        } else if (topic === "employee_decision" || topic === "trade_approved" || topic === "trade_rejected" || topic === "trade_blocked") {
            handleEmployeeDecision(data);
        } else if (topic === "employee_profile_updated" || topic === "employee_status_updated") {
            fetchEmployeeData();
            fetchSystemAnalytics();
        }
    }

    // Setup event handlers
    function setupEventListeners() {
        // Log filters
        const filterContainer = document.getElementById("log-filters");
        filterContainer.addEventListener("click", (e) => {
            if (e.target.tagName === "BUTTON") {
                filterContainer.querySelectorAll("button").forEach(b => b.classList.remove("active"));
                e.target.classList.add("active");
                activeLogFilter = e.target.getAttribute("data-filter");
                renderLogsFromMemory();
            }
        });

        // AI Employee switches
        const empSelect = document.getElementById("emp-select");
        empSelect.addEventListener("change", async () => {
            const code = empSelect.value;
            if (code) {
                try {
                    const res = await fetch(`${apiBase}/api/employees/activate?code=${code}`, { method: "POST" });
                    const r = await res.json();
                    if (r.status === "success") {
                        appendTerminalLog("employee_engine", "activate", `AI Employee ${code} activated successfully.`);
                        await fetchEmployeeData();
                        await fetchSafetyData();
                    }
                } catch (e) {
                    console.error("Failed to activate employee", e);
                }
            }
        });

        // Emergency controls
        document.getElementById("btn-emerg-kill").addEventListener("click", () => triggerEmergencyAction("kill"));
        document.getElementById("btn-emerg-reset").addEventListener("click", () => triggerEmergencyAction("reset"));
        document.getElementById("btn-emerg-pause").addEventListener("click", () => triggerEmergencyAction("pause"));
        document.getElementById("btn-emerg-resume").addEventListener("click", () => triggerEmergencyAction("resume"));

        // CSV export
        document.getElementById("btn-export-csv").addEventListener("click", exportTradesCSV);

        // Watchlist searches
        document.getElementById("market-search").addEventListener("input", (e) => {
            const query = e.target.value.toLowerCase();
            document.querySelectorAll(".price-card").forEach(card => {
                const sym = card.id.replace("price-card-", "").toLowerCase();
                if (sym.includes(query)) {
                    card.style.display = "flex";
                } else {
                    card.style.display = "none";
                }
            });
        });
    }

    async function triggerEmergencyAction(action) {
        try {
            const res = await fetch(`${apiBase}/api/safety/emergency/${action}`, { method: "POST" });
            const r = await res.json();
            appendTerminalLog("dashboard_client", `emerg_${action}`, r.message || `Action ${action} triggered successfully.`);
            await fetchSafetyData();
        } catch (e) {
            console.error(`Emergency action ${action} failed`, e);
        }
    }

    // Refresh core tables
    async function refreshTables() {
        await Promise.all([
            fetchPortfolioSummary(),
            fetchClosedTrades(),
            fetchActivePositions()
        ]);
    }

    // Initial load
    async function fetchInitialData() {
        await Promise.all([
            fetchEmployeeData(),
            fetchBrokerStatus(),
            fetchSafetyData(),
            fetchTelemetryData(),
            fetchVolumeIntelligenceData(),
            fetchOptionFlowData(),
            fetchTrendIntelData(),
            refreshTables(),
            fetchSystemSettings(),
            fetchDecisionHistory(),
            fetchSystemAnalytics()
        ]);

        // Start loops
        portfolioInterval = setInterval(() => {
            refreshTables();
            fetchSystemAnalytics();
        }, 3000);
        telemetryInterval = setInterval(() => {
            fetchTelemetryData();
            fetchVolumeIntelligenceData();
            fetchOptionFlowData();
            fetchTrendIntelData();
        }, 3000);
    }

    // Fetch employee registry
    async function fetchEmployeeData() {
        try {
            const res = await fetch(`${apiBase}/api/employees`);
            const data = await res.json();
            
            activeEmployeeCode = data.active_employee_code;
            
            const onlineCount = data.employees.filter(e => e.is_active && e.health_status === "HEALTHY").length;
            const totalCount = data.employees.length;
            document.getElementById("top-employee").textContent = `Employees Online: ${onlineCount}/${totalCount}`;

            const empSelect = document.getElementById("emp-select");
            empSelect.innerHTML = "<option value=''>-- Select Employee --</option>";
            
            data.employees.forEach(e => {
                const opt = document.createElement("option");
                opt.value = e.employee_code;
                opt.textContent = `${e.name} (${e.employee_code})`;
                if (e.employee_code === activeEmployeeCode) {
                    opt.selected = true;
                    renderActiveEmployeeDetails(e);
                }
                empSelect.appendChild(opt);
            });
            renderMonitoringPage(data.employees);
            renderPerformancePage(data.employees);
        } catch (e) {
            console.error("Failed to fetch employee registry", e);
        }
    }

    function renderActiveEmployeeDetails(emp) {
        document.getElementById("emp-name").textContent = emp.name;
        document.getElementById("emp-type").textContent = emp.employee_type;
        document.getElementById("emp-desc").textContent = emp.description;
        document.getElementById("emp-status-val").textContent = emp.state;
        document.getElementById("emp-loss-limit").textContent = `₹${emp.risk_stats.max_daily_loss}`;
        document.getElementById("emp-exposure-limit").textContent = `₹${emp.risk_stats.max_exposure}`;
        document.getElementById("emp-trade-count").textContent = emp.trade_count;
        document.getElementById("emp-win-rate").textContent = `${typeof emp.win_rate === 'number' ? emp.win_rate.toFixed(1) : '0.0'}%`;
        
        const pnlEl = document.getElementById("emp-pnl-val");
        pnlEl.textContent = `₹${typeof emp.pnl === 'number' ? emp.pnl.toFixed(2) : '0.00'}`;
        pnlEl.className = emp.pnl >= 0 ? "text-green" : "text-red";
        
        document.getElementById("emp-details").style.display = "block";
    }

    // Fetch broker settings
    async function fetchBrokerStatus() {
        try {
            const res = await fetch(`${apiBase}/api/broker/status`);
            const status = await res.json();
            
            document.getElementById("top-broker").textContent = status.broker.toUpperCase();
            
            const isLive = status.broker !== "paper_broker";
            document.getElementById("top-mode").textContent = isLive ? "LIVE_MODE" : "PAPER_MODE";
            document.getElementById("top-mode").className = isLive ? "value text-red" : "value text-green";
        } catch (e) {
            console.error("Failed to fetch broker status", e);
        }
    }

    // Fetch safety status
    async function fetchSafetyData() {
        try {
            const res = await fetch(`${apiBase}/api/safety/status`);
            const data = await res.json();

            const safetyEl = document.getElementById("risk-safety-status");
            safetyEl.textContent = data.safety_status;
            safetyEl.className = `badge ${data.safety_status === "OPERATIONAL" ? "text-green" : "text-red"}`;

            const activeKill = data.emergency_status.kill_switch_active;
            const paused = data.emergency_status.trading_paused;
            let emergVal = "NORMAL";
            if (activeKill) emergVal = "KILL SWITCH ACTIVE";
            else if (paused) emergVal = "TRADING PAUSED";
            
            document.getElementById("risk-emergency-switch").textContent = emergVal;
            document.getElementById("risk-emergency-switch").className = emergVal === "NORMAL" ? "text-green" : "text-red";

            document.getElementById("risk-current-exposure").textContent = `₹${data.current_exposure.toLocaleString()}`;
            document.getElementById("risk-profit-lock").textContent = `₹${data.daily_limits.daily_profit_lock_usd}`;
        } catch (e) {
            console.error("Failed to fetch safety status", e);
        }
    }

    // Fetch system telemetry
    async function fetchTelemetryData() {
        try {
            const res = await fetch(`${apiBase}/api/health/status`);
            const health = await res.json();

            // Status bar health
            const healthEl = document.getElementById("top-health");
            healthEl.textContent = health.overall_system_health;
            if (health.overall_system_health === "HEALTHY") {
                healthEl.className = "value text-green";
            } else if (health.overall_system_health === "DEGRADED") {
                healthEl.className = "value text-warning";
            } else {
                healthEl.className = "value text-red";
            }

            // CPU
            document.getElementById("sys-cpu-val").textContent = `${health.cpu_usage_pct}%`;
            document.getElementById("sys-cpu-fill").style.width = `${health.cpu_usage_pct}%`;

            // Memory
            document.getElementById("sys-mem-val").textContent = `${health.memory_usage_pct}%`;
            document.getElementById("sys-mem-fill").style.width = `${health.memory_usage_pct}%`;

            // Latencies
            document.getElementById("sys-net-latency").textContent = `${health.latency.internet_latency_ms}ms`;
            document.getElementById("sys-broker-latency").textContent = `${health.latency.broker_latency_ms}ms`;
            document.getElementById("sys-db-latency").textContent = `${health.latency.database_latency_ms}ms`;
            
            // Queue size
            document.getElementById("sys-queue-size").textContent = health.event_queue_health.event_bus_queue_size;
        } catch (e) {
            console.error("Failed to fetch telemetry data", e);
        }
    }

    // Fetch portfolio summary
    async function fetchPortfolioSummary() {
        try {
            const res = await fetch(`${apiBase}/api/portfolio/summary`);
            const sum = await res.json();

            document.getElementById("card-capital").textContent = `₹${sum.available_capital.toLocaleString(undefined, {minimumFractionDigits: 2})}`;
            
            const portfolioVal = sum.available_capital + sum.mtm;
            document.getElementById("card-portfolio-val").textContent = `₹${portfolioVal.toLocaleString(undefined, {minimumFractionDigits: 2})}`;
            
            const todayPnlEl = document.getElementById("card-today-pnl");
            todayPnlEl.textContent = `${sum.realized_pnl >= 0 ? '+' : ''}₹${sum.realized_pnl.toLocaleString(undefined, {minimumFractionDigits: 2})}`;
            todayPnlEl.className = sum.realized_pnl >= 0 ? "value text-green" : "value text-red";

            const totalPnlEl = document.getElementById("card-total-pnl");
            totalPnlEl.textContent = `${sum.mtm >= 0 ? '+' : ''}₹${sum.mtm.toLocaleString(undefined, {minimumFractionDigits: 2})}`;
            totalPnlEl.className = sum.mtm >= 0 ? "value text-green" : "value text-red";

            document.getElementById("card-margin").textContent = `₹${sum.utilized_margin.toLocaleString(undefined, {minimumFractionDigits: 2})}`;
        } catch (e) {
            console.error("Failed to fetch portfolio summary", e);
        }
    }

    // Fetch active positions
    async function fetchActivePositions() {
        try {
            const res = await fetch(`${apiBase}/api/portfolio/positions`);
            const data = await res.json();
            
            document.getElementById("card-open-pos").textContent = data.open_positions.length;
            document.getElementById("card-closed-trades").textContent = data.closed_positions.length;

            const posBody = document.getElementById("positions-body");
            posBody.innerHTML = "";
            
            if (data.open_positions.length === 0) {
                posBody.innerHTML = `<tr><td colspan="8" class="text-center text-muted">No open positions.</td></tr>`;
                return;
            }

            data.open_positions.forEach(p => {
                const tr = document.createElement("tr");
                const pnlClass = p.unrealized_pnl >= 0 ? "text-green" : "text-red";
                tr.innerHTML = `
                    <td><b>${p.symbol}</b></td>
                    <td><span class="${p.side === 'BUY' ? 'text-green' : 'text-red'}">${p.side}</span></td>
                    <td>${p.quantity}</td>
                    <td>₹${p.avg_price.toFixed(2)}</td>
                    <td>₹${p.ltp.toFixed(2)}</td>
                    <td class="${pnlClass}">₹${p.unrealized_pnl.toFixed(2)}</td>
                    <td>-</td>
                    <td><button class="btn-danger btn-xs" onclick="exitPosition('${p.symbol}', '${p.side}', ${p.quantity})">Exit</button></td>
                `;
                posBody.appendChild(tr);
            });
        } catch (e) {
            console.error("Failed to fetch open positions", e);
        }
    }

    // Exit Position handler
    window.exitPosition = async function(symbol, side, qty) {
        const exitSide = side === "BUY" ? "SELL" : "BUY";
        const payload = {
            symbol: symbol,
            side: exitSide,
            quantity: qty,
            type: "MARKET"
        };
        try {
            const res = await fetch(`${apiBase}/api/execution/submit`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload)
            });
            const r = await res.json();
            appendTerminalLog("execution_engine", "exit_position", `Closed position in ${symbol}.`);
            await refreshTables();
        } catch (e) {
            console.error("Failed to exit position", e);
        }
    };

    // Fetch closed trades
    async function fetchClosedTrades() {
        try {
            const res = await fetch(`${apiBase}/api/trades?limit=15`);
            const trades = await res.json();
            
            const tradesBody = document.getElementById("trades-body");
            tradesBody.innerHTML = "";

            if (trades.length === 0) {
                tradesBody.innerHTML = `<tr><td colspan="7" class="text-center text-muted">No closed trades.</td></tr>`;
                return;
            }

            trades.forEach(t => {
                const tr = document.createElement("tr");
                const pnlClass = t.realized_pnl >= 0 ? "text-green" : "text-red";
                tr.innerHTML = `
                    <td><b>${t.symbol}</b></td>
                    <td><span class="${t.side === 'BUY' ? 'text-green' : 'text-red'}">${t.side}</span></td>
                    <td>${t.quantity}</td>
                    <td>₹${t.price.toFixed(2)}</td>
                    <td>₹${t.commission.toFixed(4)}</td>
                    <td class="${pnlClass}">₹${t.realized_pnl.toFixed(2)}</td>
                    <td>${new Date(t.executed_at).toLocaleTimeString()}</td>
                `;
                tradesBody.appendChild(tr);
            });
        } catch (e) {
            console.error("Failed to fetch closed trades", e);
        }
    }

    // Export closed trades as CSV
    async function exportTradesCSV() {
        try {
            const res = await fetch(`${apiBase}/api/trades?limit=1000`);
            const trades = await res.json();
            
            if (trades.length === 0) {
                alert("No trades found to export.");
                return;
            }

            let csvContent = "Symbol,Side,Quantity,Price,Commission,Realized_PnL,Time,Broker\n";
            trades.forEach(t => {
                csvContent += `${t.symbol},${t.side},${t.quantity},${t.price},${t.commission},${t.realized_pnl},${t.executed_at},${t.broker}\n`;
            });

            const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
            const url = URL.createObjectURL(blob);
            const link = document.createElement("a");
            link.setAttribute("href", url);
            link.setAttribute("download", `trades_export_${Date.now()}.csv`);
            link.style.visibility = "hidden";
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
        } catch (e) {
            console.error("Failed to export trades CSV", e);
        }
    }

    // Live price widget
    function updatePriceWidget(tick) {
        const { symbol, close } = tick;
        const prevPrice = activePrices[symbol];
        activePrices[symbol] = close;

        let card = document.getElementById(`price-card-${symbol}`);
        if (!card) {
            card = document.createElement("div");
            card.id = `price-card-${symbol}`;
            card.className = "price-card";
            card.innerHTML = `
                <div class="symbol">${symbol}</div>
                <div class="price" id="price-val-${symbol}">₹${close.toFixed(2)}</div>
                <div class="change text-green" id="price-change-${symbol}">+0.00%</div>
            `;
            document.getElementById("prices-row").appendChild(card);
        } else {
            const priceVal = document.getElementById(`price-val-${symbol}`);
            const priceChange = document.getElementById(`price-change-${symbol}`);
            priceVal.textContent = `₹${close.toFixed(2)}`;

            if (prevPrice) {
                const diff = close - prevPrice;
                const percent = (diff / prevPrice) * 100;
                priceChange.textContent = `${percent >= 0 ? '+' : ''}${percent.toFixed(2)}%`;
                if (diff >= 0) {
                    priceChange.className = "change text-green";
                    priceVal.style.color = "var(--accent-green)";
                } else {
                    priceChange.className = "change text-red";
                    priceVal.style.color = "var(--accent-red)";
                }
                setTimeout(() => { priceVal.style.color = ""; }, 300);
            }
        }

        recalculateGainersLosers();
    }

    function recalculateGainersLosers() {
        // Automatically rank and display gainers and losers
        const tickers = Object.keys(activePrices).map(sym => {
            const priceEl = document.getElementById(`price-change-${sym}`);
            const pct = priceEl ? parseFloat(priceEl.textContent) : 0.0;
            return { symbol: sym, pct };
        });

        tickers.sort((a, b) => b.pct - a.pct);
        
        const gainers = tickers.filter(t => t.pct > 0).slice(0, 3);
        const losers = [...tickers].reverse().filter(t => t.pct < 0).slice(0, 3);

        const gContainer = document.getElementById("top-gainers");
        gContainer.innerHTML = gainers.length > 0 ? "" : "-";
        gainers.forEach(g => {
            gContainer.innerHTML += `<div style="display:flex; justify-content:space-between"><span>${g.symbol}</span><span class="text-green">+${g.pct.toFixed(2)}%</span></div>`;
        });

        const lContainer = document.getElementById("top-losers");
        lContainer.innerHTML = losers.length > 0 ? "" : "-";
        losers.forEach(l => {
            lContainer.innerHTML += `<div style="display:flex; justify-content:space-between"><span>${l.symbol}</span><span class="text-red">${l.pct.toFixed(2)}%</span></div>`;
        });
    }

    // Logs rendering & filtering
    function matchesFilter(sender, topic, filter) {
        const send = sender.toLowerCase();
        const top = topic.toLowerCase();
        
        if (filter === "system") return send.includes("system") || send.includes("health");
        if (filter === "strategy") return send.includes("strategy") || top.includes("signal");
        if (filter === "execution") return send.includes("execution") || top.includes("order");
        if (filter === "risk") return send.includes("risk") || top.includes("safety");
        if (filter === "broker") return send.includes("broker") || send.includes("kotak");
        if (filter === "paper") return send.includes("paper");
        return false;
    }

    function renderLogsFromMemory() {
        const terminal = document.getElementById("terminal-logs");
        terminal.innerHTML = "";
        
        const filtered = activeLogFilter === "all" 
            ? allLogs 
            : allLogs.filter(l => matchesFilter(l.sender, l.topic, activeLogFilter));

        filtered.slice(-100).forEach(l => {
            const entry = document.createElement("div");
            entry.className = "log-entry";
            entry.innerHTML = `
                <span class="log-time">[${new Date(l.timestamp).toLocaleTimeString()}]</span>
                <span class="log-sender">&lt;${l.sender}&gt;</span>
                <span class="log-topic" style="${getTopicStyle(l.topic)}">[${l.topic.toUpperCase()}]</span>
                <span class="log-data">${l.dataStr}</span>
            `;
            terminal.appendChild(entry);
        });
        terminal.scrollTop = terminal.scrollHeight;
    }

    function appendTerminalLog(sender, topic, dataStr, customStyle = "") {
        const terminal = document.getElementById("terminal-logs");
        const entry = document.createElement("div");
        entry.className = "log-entry";
        const time = new Date().toLocaleTimeString();
        entry.innerHTML = `
            <span class="log-time">[${time}]</span>
            <span class="log-sender">&lt;${sender}&gt;</span>
            <span class="log-topic" style="${customStyle}">[${topic.toUpperCase()}]</span>
            <span class="log-data">${dataStr}</span>
        `;
        terminal.appendChild(entry);
        terminal.scrollTop = terminal.scrollHeight;

        if (terminal.children.length > 100) {
            terminal.removeChild(terminal.firstChild);
        }
    }

    async function fetchVolumeIntelligenceData() {
        try {
            const symbol = "BTCUSD"; // Default symbol or select active symbol
            const res = await fetch(`${apiBase}/api/volume_intelligence/status?symbol=${symbol}`);
            const data = await res.json();
            updateVolumeIntelligenceUI(data);
        } catch (e) {
            console.error("Failed to fetch volume intelligence data", e);
        }
    }

    function updateVolumeIntelligenceUI(data) {
        if (!data) return;
        const statusEl = document.getElementById("vol-status");
        if (statusEl) {
            statusEl.textContent = data.confirmation_status;
            statusEl.className = `badge ${
                data.confirmation_status === "CONFIRM" ? "text-green" : 
                data.confirmation_status === "REJECT" ? "text-red" : "text-warning"
            }`;
        }
        const scoreEl = document.getElementById("vol-score");
        if (scoreEl) scoreEl.textContent = typeof data.volume_score === 'number' ? data.volume_score.toFixed(2) : data.volume_score;
        const rvolEl = document.getElementById("vol-rvol");
        if (rvolEl) rvolEl.textContent = typeof data.rvol === 'number' ? data.rvol.toFixed(4) : data.rvol;
        const trendEl = document.getElementById("vol-trend");
        if (trendEl) trendEl.textContent = data.volume_trend;
        const confEl = document.getElementById("vol-confidence");
        if (confEl) confEl.textContent = typeof data.confidence === 'number' ? `${data.confidence.toFixed(0)}%` : `${data.confidence}%`;
    }

    async function fetchOptionFlowData() {
        try {
            const symbol = "NIFTY50";
            const res = await fetch(`${apiBase}/api/option_flow/status?symbol=${symbol}`);
            const data = await res.json();
            updateOptionFlowUI(data);
        } catch (e) {
            console.error("Failed to fetch option flow data", e);
        }
    }

    function updateOptionFlowUI(data) {
        if (!data) return;
        const atmEl = document.getElementById("of-atm");
        if (atmEl) atmEl.textContent = data.atm_strike;
        
        const ceEl = document.getElementById("of-ce-strength");
        if (ceEl) ceEl.textContent = typeof data.ce_strength === 'number' ? data.ce_strength.toFixed(2) : data.ce_strength;
        
        const peEl = document.getElementById("of-pe-strength");
        if (peEl) peEl.textContent = typeof data.pe_strength === 'number' ? data.pe_strength.toFixed(2) : data.pe_strength;
        
        const pcrEl = document.getElementById("of-pcr");
        if (pcrEl) pcrEl.textContent = typeof data.pcr === 'number' ? data.pcr.toFixed(4) : data.pcr;
        
        const scoreEl = document.getElementById("of-score");
        if (scoreEl) scoreEl.textContent = typeof data.option_flow_score === 'number' ? data.option_flow_score.toFixed(2) : data.option_flow_score;
        
        const bullishEl = document.getElementById("of-bullish-pct");
        if (bullishEl) {
            const val = typeof data.option_flow_score === 'number' ? data.option_flow_score : 50;
            bullishEl.textContent = `${val.toFixed(0)}%`;
        }
        
        const bearishEl = document.getElementById("of-bearish-pct");
        if (bearishEl) {
            const val = typeof data.option_flow_score === 'number' ? (100 - data.option_flow_score) : 50;
            bearishEl.textContent = `${val.toFixed(0)}%`;
        }
        
        const volRatioEl = document.getElementById("of-vol-ratio");
        if (volRatioEl) {
            const ratio = data.ce_strength > 0 ? (data.pe_strength / data.ce_strength) : 1.0;
            volRatioEl.textContent = ratio.toFixed(2);
        }
        
        const oiRatioEl = document.getElementById("of-oi-ratio");
        if (oiRatioEl) oiRatioEl.textContent = typeof data.pcr === 'number' ? data.pcr.toFixed(2) : data.pcr;
        
        const confEl = document.getElementById("of-confidence");
        if (confEl) confEl.textContent = typeof data.confidence === 'number' ? `${data.confidence.toFixed(0)}%` : `${data.confidence}%`;
        
        const recEl = document.getElementById("of-recommendation");
        if (recEl) {
            recEl.textContent = data.recommendation;
            recEl.className = `badge ${
                data.recommendation === "BUY CE" ? "text-green" :
                data.recommendation === "BUY PE" ? "text-red" : "text-warning"
            }`;
        }
    }

    async function fetchTrendIntelData() {
        try {
            const symbol = "NIFTY50";
            const res = await fetch(`${apiBase}/api/trend_intelligence/status?symbol=${symbol}`);
            const data = await res.json();
            updateTrendIntelUI(data);
        } catch (e) {
            console.error("Failed to fetch trend intelligence data", e);
        }
    }

    function updateTrendIntelUI(data) {
        if (!data) return;
        const trendEl = document.getElementById("trend-classification");
        if (trendEl) {
            trendEl.textContent = data.trend;
            trendEl.className = `badge ${
                data.trend.includes("BULLISH") ? "badge active" :
                data.trend.includes("BEARISH") ? "badge failed" : "badge inactive"
            }`;
        }
        
        const emaEl = document.getElementById("trend-ema-alignment");
        if (emaEl) {
            emaEl.textContent = data.ema_alignment;
            emaEl.className = data.ema_alignment === "BULLISH" ? "text-green" : (data.ema_alignment === "BEARISH" ? "text-red" : "");
        }
        
        const vwapEl = document.getElementById("trend-vwap-status");
        if (vwapEl) {
            vwapEl.textContent = data.vwap_status;
            vwapEl.className = data.vwap_status === "ABOVE" ? "text-green" : (data.vwap_status === "BELOW" ? "text-red" : "");
        }
        
        const adxEl = document.getElementById("trend-adx");
        if (adxEl) adxEl.textContent = typeof data.adx === 'number' ? data.adx.toFixed(2) : data.adx;
        
        const rsiEl = document.getElementById("trend-rsi");
        if (rsiEl) rsiEl.textContent = typeof data.rsi === 'number' ? data.rsi.toFixed(2) : data.rsi;
        
        const macdEl = document.getElementById("trend-macd-hist");
        if (macdEl) {
            const hist = data.macd ? data.macd.histogram : 0.0;
            macdEl.textContent = typeof hist === 'number' ? hist.toFixed(4) : hist;
            macdEl.className = hist >= 0 ? "text-green" : "text-red";
        }
        
        const confEl = document.getElementById("trend-confidence");
        if (confEl) confEl.textContent = typeof data.confidence === 'number' ? `${data.confidence.toFixed(0)}%` : `${data.confidence}%`;
        
        const recEl = document.getElementById("trend-recommendation");
        if (recEl) {
            recEl.textContent = data.recommendation;
            recEl.className = `badge ${
                data.recommendation === "BUY" ? "badge active" :
                data.recommendation === "SELL" ? "badge failed" : "badge inactive"
            }`;
        }
    }

    function getTopicStyle(topic) {
        switch (topic) {
            case "trend_updated": return "color: var(--text-muted); font-size: 0.75rem;";
            case "trend_signal": return "color: var(--accent-green); font-weight: bold; text-shadow: 0 0 5px rgba(0,245,160,0.3);";
            case "trend_warning": return "color: var(--accent-orange); font-weight: bold;";
            case "market_data": return "color: var(--text-muted); font-size: 0.75rem;";
            case "order_request": return "color: var(--accent-blue);";
            case "order_approved": return "color: var(--accent-green); font-weight: bold;";
            case "order_rejected": return "color: var(--accent-red); font-weight: bold;";
            case "order_filled": return "color: var(--accent-green); font-weight: bold; text-shadow: 0 0 5px rgba(0,245,160,0.3)";
            case "risk_alert": return "color: var(--accent-orange); font-weight: bold;";
            case "execution_failed": return "color: var(--accent-red); font-weight: bold;";
            case "employee_activated": return "color: var(--accent-blue); font-weight: bold;";
            case "volume_intelligence_update": return "color: var(--accent-blue); font-weight: bold; text-shadow: 0 0 5px rgba(0,200,255,0.3)";
            case "option_flow_updated": return "color: var(--text-muted); font-size: 0.75rem;";
            case "option_flow_signal": return "color: var(--accent-green); font-weight: bold;";
            case "option_flow_warning": return "color: var(--accent-orange); font-weight: bold;";
            case "option_flow_trap": return "color: var(--accent-red); font-weight: bold; text-shadow: 0 0 5px rgba(255,0,0,0.3)";
            case "employee_decision": return "color: var(--accent-blue); font-size: 0.8rem;";
            case "critical_employee_failure_alert": return "color: var(--accent-red); font-weight: bold; text-shadow: 0 0 10px rgba(255,0,0,0.4);";
            default: return "";
        }
    }

    function renderMonitoringPage(employees) {
        const container = document.getElementById("monitor-cards-container");
        if (!container) return;
        
        container.innerHTML = "";
        
        const departments = {
            "Market Intelligence Department": ["EMP-TRD", "EMP-VOL", "EMP-MOM", "EMP-VWP", "EMP-RGM", "EMP-EQI", "EMP-EQS", "EMP-COM", "EMP-CUR"],
            "Options Intelligence Department": ["EMP-OFT", "EMP-OIE", "EMP-PCR", "EMP-GRK", "EMP-MPN", "EMP-OPT"],
            "Institutional Department": ["EMP-SME", "EMP-LQD", "EMP-OFL", "EMP-DEL"],
            "Risk Department": ["EMP-RSK", "EMP-PZS", "EMP-CPT", "EMP-EXP"],
            "News Department": ["EMP-NWS", "EMP-CAL", "EMP-EVR"],
            "Execution Department": ["EMP-EXE", "EMP-PTF", "EMP-PPR", "EMP-PM"]
        };
        
        Object.entries(departments).forEach(([deptName, codes]) => {
            const deptEmployees = employees.filter(e => codes.includes(e.employee_code));
            if (deptEmployees.length === 0) return;
            
            const section = document.createElement("div");
            section.className = "dept-section";
            section.style.gridColumn = "1 / -1";
            section.style.marginTop = "1.5rem";
            section.style.marginBottom = "0.5rem";
            section.style.borderBottom = "1px solid var(--border-glass)";
            section.style.paddingBottom = "0.5rem";
            section.innerHTML = `<h3 style="color: var(--accent-blue); font-size: 1.1rem; font-weight: 600;">${deptName}</h3>`;
            container.appendChild(section);
            
            deptEmployees.forEach(e => {
                const isFailed = e.health_status === "FAILED" || !e.is_active;
                const card = document.createElement("div");
                card.className = `employee-monitor-card ${isFailed ? "failed" : ""}`;
                
                const formattedTime = e.heartbeat_timestamp ? new Date(e.heartbeat_timestamp).toLocaleTimeString() : "-";
                
                card.innerHTML = `
                    <div class="monitor-header">
                        <div class="monitor-title">
                            <div class="monitor-avatar-placeholder">🤖</div>
                            <div>
                                <h3 style="font-size: 0.95rem; font-weight:600;">${e.name}</h3>
                                <span style="font-size: 0.75rem; color: var(--text-secondary);">${e.employee_code}</span>
                            </div>
                        </div>
                        <span class="status-badge ${e.is_active ? (e.health_status === 'FAILED' ? 'failed' : 'active') : 'inactive'}">
                            ${e.is_active ? e.health_status : 'OFFLINE'}
                        </span>
                    </div>
                    
                    <div class="monitor-metrics-grid">
                        <div class="monitor-metric">
                            <span class="lbl">Last Decision</span>
                            <span class="val accent-blue" style="font-size: 0.8rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${e.last_decision}">${e.last_decision}</span>
                        </div>
                        <div class="monitor-metric">
                            <span class="lbl">Confidence</span>
                            <span class="val">${typeof e.last_decision_confidence === 'number' ? e.last_decision_confidence.toFixed(1) : '0.0'}%</span>
                        </div>
                        <div class="monitor-metric">
                            <span class="lbl">Signals</span>
                            <span class="val">${e.total_signals || 0}</span>
                        </div>
                        <div class="monitor-metric">
                            <span class="lbl">Accuracy</span>
                            <span class="val text-green">${typeof e.accuracy_pct === 'number' ? e.accuracy_pct.toFixed(1) : '0.0'}%</span>
                        </div>
                        <div class="monitor-metric">
                            <span class="lbl">Last Exec Latency</span>
                            <span class="val">${typeof e.last_execution_time_ms === 'number' ? e.last_execution_time_ms.toFixed(1) : '0.0'} ms</span>
                        </div>
                        <div class="monitor-metric">
                            <span class="lbl">Avg Latency</span>
                            <span class="val">${typeof e.avg_execution_time_ms === 'number' ? e.avg_execution_time_ms.toFixed(1) : '0.0'} ms</span>
                        </div>
                    </div>
                    
                    <div class="monitor-footer">
                        <div><strong>Errors:</strong> <span class="${e.error_count > 0 ? 'text-red' : ''}">${e.error_count}</span> ${e.last_error ? `<span style="font-size: 0.7rem; color: var(--accent-red); block">(${e.last_error})</span>` : ''}</div>
                        <div><strong>Heartbeat:</strong> <span>${formattedTime}</span></div>
                    </div>
                `;
                container.appendChild(card);
            });
        });
    }

    function renderPerformancePage(employees) {
        const body = document.getElementById("performance-table-body");
        if (!body) return;
        
        body.innerHTML = "";
        employees.forEach(e => {
            const row = document.createElement("tr");
            const formattedTime = e.heartbeat_timestamp ? new Date(e.heartbeat_timestamp).toLocaleTimeString() : "-";
            
            row.innerHTML = `
                <td><strong>${e.name}</strong><br><small style="color: var(--text-muted);">${e.employee_code}</small></td>
                <td><span class="badge">${e.employee_type}</span></td>
                <td>${e.total_signals}</td>
                <td class="text-green">${e.correct_signals}</td>
                <td class="text-red">${e.incorrect_signals}</td>
                <td><strong>${typeof e.accuracy_pct === 'number' ? e.accuracy_pct.toFixed(2) : '0.00'}%</strong></td>
                <td>${typeof e.avg_execution_time_ms === 'number' ? e.avg_execution_time_ms.toFixed(1) : '0.0'} ms</td>
                <td>${formattedTime}</td>
                <td><span class="status-badge ${e.health_status === 'HEALTHY' ? 'active' : 'failed'}">${e.health_status}</span></td>
            `;
            body.appendChild(row);
        });
        
        updatePerformanceCharts(employees);
    }

    function updatePerformanceCharts(employees) {
        const pnlCtx = document.getElementById("chart-employee-pnl");
        const accCtx = document.getElementById("chart-employee-accuracy");
        if (!pnlCtx || !accCtx) return;
        
        if (typeof Chart === 'undefined') {
            console.warn("Chart.js is not loaded. Skipping performance charts rendering.");
            return;
        }
        
        const labels = employees.map(e => e.name.replace("Default ", ""));
        const pnlData = employees.map(e => e.pnl);
        const colors = pnlData.map(val => val >= 0 ? 'rgba(0, 245, 160, 0.45)' : 'rgba(255, 59, 48, 0.45)');
        const borderColors = pnlData.map(val => val >= 0 ? '#00f5a0' : '#ff3b30');
        
        if (!pnlChart) {
            pnlChart = new Chart(pnlCtx, {
                type: 'bar',
                data: {
                    labels: labels,
                    datasets: [{
                        label: 'Net PnL (₹)',
                        data: pnlData,
                        backgroundColor: colors,
                        borderColor: borderColors,
                        borderWidth: 1.5
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: { mode: 'index', intersect: false }
                    },
                    scales: {
                        y: {
                            grid: { color: 'rgba(255, 255, 255, 0.05)' },
                            ticks: { color: '#9ca3af' }
                        },
                        x: {
                            grid: { display: false },
                            ticks: { color: '#9ca3af' }
                        }
                    }
                }
            });
        } else {
            pnlChart.data.labels = labels;
            pnlChart.data.datasets[0].data = pnlData;
            pnlChart.data.datasets[0].backgroundColor = colors;
            pnlChart.data.datasets[0].borderColor = borderColors;
            pnlChart.update();
        }
        
        let selectedEmp = employees.find(e => e.employee_code === activeEmployeeCode) || employees[0];
        let accHistory = selectedEmp ? selectedEmp.accuracy_history : [];
        
        let historyLabels = [];
        let historyData = [];
        
        if (accHistory && accHistory.length > 0) {
            historyLabels = accHistory.map((h, idx) => `Sig ${idx + 1}`);
            historyData = accHistory.map(h => h.accuracy_pct);
        } else {
            // Default baseline values
            historyLabels = ['Sig 1', 'Sig 2', 'Sig 3', 'Sig 4', 'Sig 5'];
            historyData = [100, 100, 100, 100, 100];
        }
        
        if (!accuracyChart) {
            accuracyChart = new Chart(accCtx, {
                type: 'line',
                data: {
                    labels: historyLabels,
                    datasets: [{
                        label: `${selectedEmp ? selectedEmp.name.replace("Default ", "") : 'Employee'} Accuracy Over Time (%)`,
                        data: historyData,
                        borderColor: '#00f2fe',
                        backgroundColor: 'rgba(0, 242, 254, 0.1)',
                        fill: true,
                        tension: 0.3,
                        borderWidth: 2
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: true, labels: { color: '#f3f4f6' } },
                        tooltip: { mode: 'index', intersect: false }
                    },
                    scales: {
                        y: {
                            min: 0,
                            max: 100,
                            grid: { color: 'rgba(255, 255, 255, 0.05)' },
                            ticks: { color: '#9ca3af' }
                        },
                        x: {
                            grid: { display: false },
                            ticks: { color: '#9ca3af' }
                        }
                    }
                }
            });
        } else {
            accuracyChart.data.labels = historyLabels;
            accuracyChart.data.datasets[0].label = `${selectedEmp ? selectedEmp.name.replace("Default ", "") : 'Employee'} Accuracy Over Time (%)`;
            accuracyChart.data.datasets[0].data = historyData;
            accuracyChart.update();
        }
    }

    function handleEmployeeDecision(data) {
        decisionLogs.unshift(data);
        if (decisionLogs.length > 100) {
            decisionLogs.pop();
        }
        renderDecisionLog();
    }

    function renderDecisionLog() {
        const body = document.getElementById("decisions-log-body");
        if (!body) return;
        
        const filterVal = document.getElementById("decisions-search")?.value.toLowerCase() || "";
        const filtered = decisionLogs.filter(d => {
            const empCode = (d.employee_code || "EMP-CHIEF").toLowerCase();
            const empName = (d.name || "Chief Decision Agent").toLowerCase();
            const decisionVal = (d.decision || d.status || "UNKNOWN").toLowerCase();
            const symbolVal = (d.symbol || "").toLowerCase();
            const explanationVal = (d.explanation || d.error || "").toLowerCase();
            
            return empCode.includes(filterVal) ||
                   empName.includes(filterVal) ||
                   decisionVal.includes(filterVal) ||
                   symbolVal.includes(filterVal) ||
                   explanationVal.includes(filterVal);
        });
        
        if (filtered.length === 0) {
            body.innerHTML = `<tr><td colspan="7" class="text-center text-muted">No matching decisions.</td></tr>`;
            return;
        }
        
        body.innerHTML = "";
        filtered.forEach(d => {
            const empCode = d.employee_code || "EMP-CHIEF";
            const empName = d.name || "Chief Decision Agent";
            const decisionText = d.decision || d.status || "UNKNOWN";
            const symbolText = d.symbol || "-";
            const confidenceVal = typeof d.confidence === 'number' ? d.confidence.toFixed(1) : '0.0';
            const latencyText = typeof d.execution_time_ms === 'number' ? `${d.execution_time_ms.toFixed(2)} ms` : '-';
            const detailsText = d.explanation || d.error || "Check passed.";
            
            const time = new Date(d.timestamp).toLocaleTimeString();
            const row = document.createElement("tr");
            
            const isApproved = decisionText.includes("APPROVED") || decisionText.includes("COMPLETE") || decisionText.includes("FILLED");
            const decisionClass = isApproved ? "text-green" : ((decisionText.includes("REJECTED") || decisionText.includes("BLOCKED") || decisionText.includes("FAILED")) ? "text-red" : "text-warning");
            
            row.innerHTML = `
                <td>${time}</td>
                <td><strong>${empName}</strong><br><small style="color: var(--text-muted);">${empCode}</small></td>
                <td><b>${symbolText}</b></td>
                <td><span class="${decisionClass}">${decisionText}</span></td>
                <td>${confidenceVal}%</td>
                <td>${latencyText}</td>
                <td title="${detailsText}">${detailsText}</td>
            `;
            body.appendChild(row);
        });
    }

    async function fetchDecisionHistory() {
        try {
            const res = await fetch(`${apiBase}/api/chief/history`);
            const data = await res.json();
            decisionLogs = data;
            renderDecisionLog();
        } catch (e) {
            console.error("Failed to fetch decision history", e);
        }
    }
    window.fetchDecisionHistory = fetchDecisionHistory;

    let equityChart = null;

    async function fetchSystemAnalytics() {
        try {
            const summaryRes = await fetch(`${apiBase}/api/analytics/summary`);
            const metrics = await summaryRes.json();
            
            document.getElementById("card-win-rate").textContent = `${typeof metrics.win_rate === 'number' ? metrics.win_rate.toFixed(1) : '0.0'}%`;
            document.getElementById("card-rr").textContent = typeof metrics.risk_reward_ratio === 'number' ? metrics.risk_reward_ratio.toFixed(2) : '0.00';
            document.getElementById("card-drawdown").textContent = `₹${typeof metrics.max_drawdown === 'number' ? metrics.max_drawdown.toLocaleString(undefined, {minimumFractionDigits: 2}) : '0.00'}`;
            
            const reportRes = await fetch(`${apiBase}/api/analytics/report`);
            const report = await reportRes.json();
            
            updateSystemCharts(report);
        } catch (e) {
            console.error("Failed to fetch system analytics", e);
        }
    }
    window.fetchSystemAnalytics = fetchSystemAnalytics;

    function updateSystemCharts(report) {
        const equityCtx = document.getElementById("chart-system-equity");
        if (!equityCtx) return;
        
        if (typeof Chart === 'undefined') {
            console.warn("Chart.js is not loaded. Skipping system equity chart rendering.");
            return;
        }

        const dataPoints = report.equity_curve || [0.0];
        const labels = dataPoints.map((_, idx) => `Trade ${idx}`);

        if (!equityChart) {
            equityChart = new Chart(equityCtx, {
                type: 'line',
                data: {
                    labels: labels,
                    datasets: [{
                        label: 'Net Equity Balance (₹)',
                        data: dataPoints,
                        borderColor: '#00f5a0',
                        backgroundColor: 'rgba(0, 245, 160, 0.1)',
                        fill: true,
                        tension: 0.2,
                        borderWidth: 2
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: true, labels: { color: '#f3f4f6' } },
                        tooltip: { mode: 'index', intersect: false }
                    },
                    scales: {
                        y: {
                            grid: { color: 'rgba(255, 255, 255, 0.05)' },
                            ticks: { color: '#9ca3af' }
                        },
                        x: {
                            grid: { display: false },
                            ticks: { color: '#9ca3af' }
                        }
                    }
                }
            });
        } else {
            equityChart.data.labels = labels;
            equityChart.data.datasets[0].data = dataPoints;
            equityChart.update();
        }
    }

    // Set up live filter search input
    document.getElementById("decisions-search")?.addEventListener("input", renderDecisionLog);

    window.clearDecisionLog = function() {
        decisionLogs = [];
        renderDecisionLog();
    };

    // --- SYSTEM & BROKER SETTINGS ---
    async function fetchSystemSettings() {
        try {
            const res = await fetch(`${apiBase}/api/settings`);
            const data = await res.json();
            
            document.getElementById("settings-active-broker").value = data.active_broker;
            document.getElementById("settings-client-id").value = data.client_id;
            document.getElementById("settings-api-key").value = data.api_key;
            document.getElementById("settings-max-daily-loss").value = Math.round(data.max_daily_loss);
            document.getElementById("settings-max-exposure").value = Math.round(data.max_exposure);
            
            toggleCredentialsVisibility();
        } catch (e) {
            console.error("Failed to fetch system settings", e);
        }
    }
    window.fetchSystemSettings = fetchSystemSettings;

    function toggleCredentialsVisibility() {
        const activeBroker = document.getElementById("settings-active-broker").value;
        const credsGroup = document.getElementById("kotak-creds-group");
        if (credsGroup) {
            if (activeBroker === "kotak_neo") {
                credsGroup.style.display = "flex";
            } else {
                credsGroup.style.display = "none";
            }
        }
    }
    window.toggleCredentialsVisibility = toggleCredentialsVisibility;

    window.saveSystemSettings = async function(event) {
        if (event) event.preventDefault();
        
        const statusMsg = document.getElementById("settings-status-msg");
        if (statusMsg) {
            statusMsg.textContent = "Saving settings...";
            statusMsg.style.color = "var(--text-secondary)";
        }
        
        const payload = {
            active_broker: document.getElementById("settings-active-broker").value,
            client_id: document.getElementById("settings-client-id").value,
            api_key: document.getElementById("settings-api-key").value,
            max_daily_loss: parseFloat(document.getElementById("settings-max-daily-loss").value || 0),
            max_exposure: parseFloat(document.getElementById("settings-max-exposure").value || 0)
        };
        
        try {
            const res = await fetch(`${apiBase}/api/settings`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload)
            });
            const data = await res.json();
            
            if (res.ok && data.success) {
                if (statusMsg) {
                    statusMsg.textContent = "⚙️ Settings saved & reloaded!";
                    statusMsg.style.color = "var(--accent-green)";
                }
                appendTerminalLog("system", "config_update", `Active broker switched to ${payload.active_broker}. Risk metrics updated.`);
                
                // Refresh top metrics & safety status
                fetchBrokerStatus();
                fetchSafetyData();
                fetchEmployeeData();
            } else {
                if (statusMsg) {
                    statusMsg.textContent = `Error: ${data.detail || 'Save failed'}`;
                    statusMsg.style.color = "var(--accent-red)";
                }
            }
        } catch (e) {
            console.error("Failed to save settings", e);
            if (statusMsg) {
                statusMsg.textContent = "Failed to communicate with server.";
                statusMsg.style.color = "var(--accent-red)";
            }
        }
    };
});
