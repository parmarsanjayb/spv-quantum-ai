// SPV Quantum AI Operating System - Dashboard Controller v2
document.addEventListener("DOMContentLoaded", () => {
    const apiBase = window.location.origin;
    const wsUri = `${window.location.protocol === "https:" ? "wss:" : "ws:"}//${window.location.host}/ws`;

    // Active state caches
    let symbolSegments = {};   // symbol -> raw segment (INDEX/EQUITY/COMMODITY/CURRENCY/SPOT)
    // Central market data store — the single source of truth every Live Market
    // Monitor widget (indices, gainers/losers, commodities) reads from. There is
    // exactly one live feed subscription (the Kotak Neo WebSocket on the backend);
    // this store is how its ticks fan out to multiple widgets without each of
    // them subscribing separately.
    let marketDataStore = {};  // symbol -> { ltp, change, changePct, volume }

    // Fixed panel memberships — Market Indices and Commodity Watch always show
    // exactly these symbols (in this order); Top Gainers/Losers is computed
    // dynamically from whichever symbols are tagged EQUITY in symbolSegments.
    const INDEX_SYMBOLS = ["NIFTY50", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"];
    const INDEX_LABELS = {
        NIFTY50: "NIFTY 50", BANKNIFTY: "NIFTY BANK", FINNIFTY: "FINNIFTY",
        MIDCPNIFTY: "NIFTY MIDCAP SELECT", SENSEX: "SENSEX",
    };
    const COMMODITY_SYMBOLS = ["CRUDEOIL", "NATURALGAS", "GOLD", "SILVER", "COPPER", "ZINC", "ALUMINIUM", "LEAD", "NICKEL"];

    let allLogs = [];
    let activeLogFilter = "all";
    let activeEmployeeCode = null;
    let decisionLogs = [];
    let pnlChart = null;
    let accuracyChart = null;
    let ws;

    // Simplified trading controls state
    let lastEmployeeList = [];
    let activeSymbol = null;
    let latestOpenPositions = [];
    let pendingConfirmationsInterval;
    let backtestPollInterval;

    // Strategy Studio state
    let studioSchema = null;
    let studioStrategies = [];
    let studioCurrentName = null;   // null = creating a new strategy
    let studioConditionRowSeq = 0;
    let backtestEquityChart = null;

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
        if (tabId === "tab-strategy") {
            loadStrategySetup();
        }
        if (tabId === "tab-backtest") {
            populateBacktestStrategyPicker();
            loadBacktestResult();
        }
        if (tabId === "tab-studio") {
            loadStudioStrategyList();
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
        } else if (topic === "feed_connected") {
            setFeedStatus(true);
        } else if (topic === "feed_disconnected") {
            setFeedStatus(false);
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

        // Show/hide the full multi-panel market overview (defaults collapsed
        // so a new user just sees their selected symbol, not everything).
        document.getElementById("btn-toggle-market-panel").addEventListener("click", (e) => {
            const panel = document.getElementById("market-groups");
            const collapsed = panel.style.display === "none";
            panel.style.display = collapsed ? "block" : "none";
            e.target.textContent = collapsed ? "Hide Full Market" : "Show Full Market";
        });

        // Strategy Setup: symbol picker
        document.getElementById("strategy-symbol-select").addEventListener("change", (e) => {
            if (e.target.value) setActiveSymbol(e.target.value);
        });

        // Strategy Setup: manual/auto mode toggle
        document.getElementById("btn-mode-auto").addEventListener("click", () => setTradingMode("AUTO"));
        document.getElementById("btn-mode-manual").addEventListener("click", () => setTradingMode("MANUAL"));

        // Strategy Setup: broker toggle
        document.getElementById("btn-broker-paper").addEventListener("click", () => setActiveBroker("paper_broker"));
        document.getElementById("btn-broker-real").addEventListener("click", () => setActiveBroker("kotak_neo"));

        // Backtest form
        document.getElementById("backtest-form").addEventListener("submit", runBacktestFromForm);

        // Pending confirmation buttons (delegated — rows are re-rendered often)
        document.getElementById("pending-confirmations-list").addEventListener("click", (e) => {
            const confirmId = e.target.getAttribute("data-confirm");
            const rejectId = e.target.getAttribute("data-reject");
            if (confirmId) respondToPendingTrade(confirmId, "confirm");
            if (rejectId) respondToPendingTrade(rejectId, "reject");
        });

        // Manual Buy/Sell on the active symbol
        document.getElementById("btn-manual-buy").addEventListener("click", () => submitManualOrder("BUY"));
        document.getElementById("btn-manual-sell").addEventListener("click", () => submitManualOrder("SELL"));

        // Employee Monitor: relevant-only filter
        document.getElementById("chk-relevant-only").addEventListener("change", applyEmployeeRelevanceFilter);

        // Strategy Studio
        document.getElementById("btn-studio-new").addEventListener("click", openStudioNew);
        document.getElementById("btn-studio-validate").addEventListener("click", validateStudioForm);
        document.getElementById("btn-studio-save").addEventListener("click", saveStudioForm);
        document.getElementById("btn-studio-add-entry-condition").addEventListener("click", () => createConditionRow("studio-entry-conditions"));
        document.getElementById("btn-studio-add-exit-condition").addEventListener("click", () => createConditionRow("studio-exit-conditions"));
        document.getElementById("studio-strategy-list").addEventListener("click", (e) => {
            const openName = e.target.getAttribute("data-open");
            const cloneName = e.target.getAttribute("data-clone");
            const deleteName = e.target.getAttribute("data-delete");
            if (openName) openStudioEdit(openName);
            if (cloneName) cloneStudioStrategy(cloneName);
            if (deleteName) deleteStudioStrategy(deleteName);
        });
    }

    async function submitManualOrder(side) {
        if (!activeSymbol) {
            alert("Select a symbol in Strategy Setup first.");
            return;
        }
        const qtyStr = prompt(`Quantity to ${side} for ${activeSymbol}?`, "1");
        if (!qtyStr) return;
        const quantity = parseFloat(qtyStr);
        if (!quantity || quantity <= 0) {
            alert("Enter a valid quantity.");
            return;
        }
        try {
            const res = await fetch(`${apiBase}/api/execution/submit`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ symbol: activeSymbol, side, quantity, type: "MARKET" })
            });
            await res.json();
            appendTerminalLog("dashboard_client", "manual_order", `Manual ${side} submitted for ${activeSymbol} x${quantity}.`);
            await refreshTables();
        } catch (e) {
            console.error("Failed to submit manual order", e);
        }
    }

    async function applyEmployeeRelevanceFilter() {
        const relevantOnly = document.getElementById("chk-relevant-only").checked;
        if (!relevantOnly) {
            renderMonitoringPage(lastEmployeeList);
            return;
        }
        try {
            const res = await fetch(`${apiBase}/api/employees/relevant`);
            const data = await res.json();
            renderMonitoringPage(data.employees && data.employees.length ? data.employees : lastEmployeeList);
        } catch (e) {
            console.error("Failed to fetch relevant employees", e);
            renderMonitoringPage(lastEmployeeList);
        }
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
    async function fetchSymbolGroups() {
        try {
            const res = await fetch(`${apiBase}/api/market/segments`);
            symbolSegments = await res.json();
        } catch (e) {
            console.error("Failed to fetch market segments", e);
        }
    }

    // Bulk-load whatever prices the server already has cached, so the panels
    // aren't empty until the next live tick happens to arrive for each symbol.
    async function fetchMarketSnapshot() {
        try {
            const res = await fetch(`${apiBase}/api/market/snapshot`);
            const snapshot = await res.json();
            Object.keys(snapshot).forEach(sym => {
                const s = snapshot[sym];
                marketDataStore[sym] = {
                    ltp: s.ltp, change: s.change, changePct: s.change_pct, volume: s.volume,
                };
            });
            renderMarketIndices();
            renderCommodityWatch();
            renderTopGainersLosers();
        } catch (e) {
            console.error("Failed to fetch market snapshot", e);
        }
    }

    async function fetchInitialData() {
        await fetchSymbolGroups();
        await Promise.all([
            fetchMarketSnapshot(),
            fetchFeedStatus(),
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
            fetchSystemAnalytics(),
            populateSymbolPickers(),
            loadActiveSymbol(),
            loadTradingMode()
        ]);

        // Start loops
        portfolioInterval = setInterval(() => {
            refreshTables();
            fetchSystemAnalytics();
            updateMySymbolCard();
        }, 3000);
        telemetryInterval = setInterval(() => {
            fetchTelemetryData();
            fetchVolumeIntelligenceData();
            fetchOptionFlowData();
            fetchTrendIntelData();
        }, 3000);
        setInterval(fetchFeedStatus, 5000);
        pendingConfirmationsInterval = setInterval(fetchPendingConfirmations, 4000);
        fetchPendingConfirmations();
    }

    // ── Simplified Trading Controls: symbol picker, mode, broker ──────────────

    async function populateSymbolPickers() {
        try {
            const res = await fetch(`${apiBase}/api/market/symbols`);
            const data = await res.json();
            const symbols = (data.symbols || []).slice().sort();
            ["strategy-symbol-select", "backtest-symbol"].forEach(id => {
                const sel = document.getElementById(id);
                if (!sel) return;
                const current = sel.value;
                sel.innerHTML = `<option value="">-- Select Symbol --</option>`;
                symbols.forEach(sym => {
                    const opt = document.createElement("option");
                    opt.value = sym;
                    opt.textContent = sym;
                    sel.appendChild(opt);
                });
                if (current) sel.value = current;
            });
        } catch (e) {
            console.error("Failed to populate symbol pickers", e);
        }
    }

    async function loadActiveSymbol() {
        try {
            const res = await fetch(`${apiBase}/api/trading/active-symbol`);
            const data = await res.json();
            activeSymbol = data.symbol;
            const sel = document.getElementById("strategy-symbol-select");
            if (sel && activeSymbol) sel.value = activeSymbol;
            updateMySymbolCard();
        } catch (e) {
            console.error("Failed to load active symbol", e);
        }
    }

    async function setActiveSymbol(symbol) {
        try {
            await fetch(`${apiBase}/api/trading/active-symbol`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ symbol })
            });
            activeSymbol = symbol.toUpperCase();
            updateMySymbolCard();
            appendTerminalLog("dashboard_client", "active_symbol", `Now working on ${activeSymbol}.`);
        } catch (e) {
            console.error("Failed to set active symbol", e);
        }
    }

    function updateMySymbolCard() {
        const emptyEl = document.getElementById("my-symbol-empty");
        const contentEl = document.getElementById("my-symbol-content");
        if (!activeSymbol) {
            emptyEl.style.display = "block";
            contentEl.style.display = "none";
            return;
        }
        emptyEl.style.display = "none";
        contentEl.style.display = "block";
        document.getElementById("my-symbol-name").textContent = activeSymbol;

        const tick = marketDataStore[activeSymbol];
        if (tick) {
            document.getElementById("my-symbol-ltp").textContent = `₹${Number(tick.ltp).toFixed(2)}`;
            const chg = Number(tick.changePct || 0);
            const chgEl = document.getElementById("my-symbol-change");
            chgEl.textContent = `${chg >= 0 ? "+" : ""}${chg.toFixed(2)}%`;
            chgEl.className = `value ${chg >= 0 ? "text-green" : "text-red"}`;
        } else {
            document.getElementById("my-symbol-ltp").textContent = "-";
            document.getElementById("my-symbol-change").textContent = "-";
        }

        const posEl = document.getElementById("my-symbol-position");
        const pos = latestOpenPositions.find(p => p.symbol === activeSymbol);
        if (pos) {
            const pnlClass = pos.unrealized_pnl >= 0 ? "text-green" : "text-red";
            posEl.innerHTML = `${pos.side} ${pos.quantity} @ ₹${pos.avg_price.toFixed(2)} &nbsp; <span class="${pnlClass}">₹${pos.unrealized_pnl.toFixed(2)}</span>`;
        } else {
            posEl.textContent = "No open position";
        }
    }

    async function loadTradingMode() {
        try {
            const res = await fetch(`${apiBase}/api/trading/mode`);
            const data = await res.json();
            setModeButtonsActive(data.mode);
        } catch (e) {
            console.error("Failed to load trading mode", e);
        }
        try {
            const res2 = await fetch(`${apiBase}/api/broker/status`);
            const status = await res2.json();
            setBrokerButtonsActive(status.broker);
        } catch (e) {
            console.error("Failed to load broker status", e);
        }
    }

    function setModeButtonsActive(mode) {
        document.querySelectorAll(".mode-toggle-btn").forEach(btn => {
            btn.classList.toggle("active-toggle", btn.getAttribute("data-mode") === mode);
        });
        const manualActionsEl = document.getElementById("manual-trade-actions");
        if (manualActionsEl) manualActionsEl.style.display = mode === "MANUAL" ? "flex" : "none";
        const badgeEl = document.getElementById("my-symbol-mode-badge");
        if (badgeEl) {
            badgeEl.textContent = mode;
            badgeEl.className = `badge ${mode === "MANUAL" ? "text-orange" : "text-green"}`;
        }
        const pendingCard = document.getElementById("pending-confirmations-card");
        if (pendingCard && mode === "AUTO") pendingCard.style.display = "none";
    }

    function setBrokerButtonsActive(broker) {
        document.querySelectorAll(".broker-toggle-btn").forEach(btn => {
            btn.classList.toggle("active-toggle", btn.getAttribute("data-broker") === broker);
        });
    }

    async function setTradingMode(mode) {
        try {
            const res = await fetch(`${apiBase}/api/trading/mode`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ mode })
            });
            const r = await res.json();
            setModeButtonsActive(r.mode);
            document.getElementById("strategy-setup-status").textContent = `Trade mode set to ${r.mode}.`;
        } catch (e) {
            console.error("Failed to set trading mode", e);
        }
    }

    async function setActiveBroker(broker) {
        try {
            const res = await fetch(`${apiBase}/api/trading/broker`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ broker })
            });
            const r = await res.json();
            setBrokerButtonsActive(r.broker);
            document.getElementById("strategy-setup-status").textContent =
                `Broker switched to ${r.broker === "paper_broker" ? "Paper Trading" : "Real Money (Kotak Neo)"}.`;
            await fetchBrokerStatus();
        } catch (e) {
            console.error("Failed to switch broker", e);
            document.getElementById("strategy-setup-status").textContent = "Failed to switch broker.";
        }
    }

    async function loadStrategySetup() {
        await populateSymbolPickers();
        await loadActiveSymbol();
        await loadTradingMode();
        try {
            const res = await fetch(`${apiBase}/api/employees/relevant`);
            const data = await res.json();
            const el = document.getElementById("active-strategy-details");
            if (!data.strategy_name) {
                el.innerHTML = `<div>No active strategy configured.</div>`;
                return;
            }
            el.innerHTML = `
                <div><strong>Strategy:</strong> ${data.strategy_name}</div>
                <div><strong>Instrument segment:</strong> ${data.segment}</div>
                <div><strong>Relevant AI Employees:</strong> ${data.relevant_codes.length} of 30</div>
            `;
        } catch (e) {
            console.error("Failed to load active strategy", e);
        }
    }

    // ── Pending Confirmations (Manual mode) ────────────────────────────────────

    async function fetchPendingConfirmations() {
        try {
            const res = await fetch(`${apiBase}/api/trading/pending`);
            const pending = await res.json();
            const card = document.getElementById("pending-confirmations-card");
            const list = document.getElementById("pending-confirmations-list");
            if (!pending || pending.length === 0) {
                card.style.display = "none";
                return;
            }
            card.style.display = "block";
            list.innerHTML = "";
            pending.forEach(p => {
                const row = document.createElement("div");
                row.className = "pending-confirmation-row";
                row.style.cssText = "display:flex; align-items:center; justify-content:space-between; padding:0.6rem 0; border-bottom:1px solid var(--border-glass);";
                row.innerHTML = `
                    <div>
                        <strong>${p.side} ${p.symbol}</strong>
                        <span style="color: var(--text-secondary); font-size: 0.85rem;">qty ${p.quantity} @ ₹${Number(p.price).toFixed(2)}</span>
                    </div>
                    <div style="display:flex; gap:0.5rem;">
                        <button class="btn-success btn-xs" data-confirm="${p.decision_id}">Confirm</button>
                        <button class="btn-danger btn-xs" data-reject="${p.decision_id}">Reject</button>
                    </div>
                `;
                list.appendChild(row);
            });
        } catch (e) {
            console.error("Failed to fetch pending confirmations", e);
        }
    }

    async function respondToPendingTrade(decisionId, action) {
        try {
            await fetch(`${apiBase}/api/trading/${action}/${decisionId}`, { method: "POST" });
            appendTerminalLog("dashboard_client", `trade_${action}`, `Trade ${action}ed: ${decisionId}`);
            await fetchPendingConfirmations();
            await refreshTables();
        } catch (e) {
            console.error(`Failed to ${action} trade`, e);
        }
    }

    // ── Backtest ────────────────────────────────────────────────────────────────

    async function populateBacktestStrategyPicker() {
        try {
            const res = await fetch(`${apiBase}/api/strategy-studio/strategies`);
            const studioList = await res.json();
            const res2 = await fetch(`${apiBase}/api/strategies/active`);
            const yamlList = await res2.json();

            const studioNames = new Set(studioList.map(s => s.strategy_name));
            const allNames = [...studioNames, ...yamlList.map(s => s.name).filter(n => !studioNames.has(n))];

            const sel = document.getElementById("backtest-strategy");
            const current = sel.value;
            sel.innerHTML = `<option value="">-- Select Strategy --</option>`;
            allNames.forEach(name => {
                const opt = document.createElement("option");
                opt.value = name;
                opt.textContent = name;
                sel.appendChild(opt);
            });
            if (current) sel.value = current;
        } catch (e) {
            console.error("Failed to populate backtest strategy picker", e);
        }
    }

    async function runBacktestFromForm(e) {
        e.preventDefault();
        const strategyName = document.getElementById("backtest-strategy").value;
        const symbol = document.getElementById("backtest-symbol").value;
        const start = document.getElementById("backtest-start-date").value;
        const end = document.getElementById("backtest-end-date").value;
        if (!strategyName || !symbol || !start || !end) {
            alert("Please select a strategy, symbol, and date range.");
            return;
        }
        const config = {
            symbols: [symbol],
            timeframe: "1m",
            start_date: new Date(start + "T00:00:00Z").toISOString(),
            end_date: new Date(end + "T23:59:59Z").toISOString(),
            initial_capital: 100000.0,
            strategy_name: strategyName
        };
        try {
            const res = await fetch(`${apiBase}/api/backtest/run`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(config)
            });
            await res.json();
            document.getElementById("backtest-progress").style.display = "block";
            document.getElementById("backtest-result-card").style.display = "none";
            if (backtestPollInterval) clearInterval(backtestPollInterval);
            backtestPollInterval = setInterval(pollBacktestStatus, 500);
        } catch (e) {
            console.error("Failed to start backtest", e);
        }
    }

    async function pollBacktestStatus() {
        try {
            const res = await fetch(`${apiBase}/api/backtest/status`);
            const status = await res.json();
            const pct = status.progress_pct || 0;
            document.getElementById("backtest-progress-fill").style.width = `${pct}%`;
            document.getElementById("backtest-progress-text").textContent =
                `${status.status} — ${pct.toFixed(0)}% (${status.trades_executed || 0} trades so far)`;
            if (status.status === "COMPLETED" || status.status === "FAILED") {
                clearInterval(backtestPollInterval);
                await loadBacktestResult();
            }
        } catch (e) {
            console.error("Failed to poll backtest status", e);
        }
    }

    async function loadBacktestResult() {
        try {
            const res = await fetch(`${apiBase}/api/backtest/result`);
            const result = await res.json();
            if (!result.verdict) return;
            document.getElementById("backtest-progress").style.display = "none";
            const card = document.getElementById("backtest-result-card");
            card.style.display = "block";
            const badge = document.getElementById("backtest-verdict-badge");
            badge.textContent = result.verdict.headline;
            const colorClass = result.verdict.label === "PROFITABLE" ? "text-green"
                : result.verdict.label === "NOT_PROFITABLE" ? "text-red" : "text-orange";
            badge.className = `badge ${colorClass}`;
            document.getElementById("backtest-verdict-detail").textContent = result.verdict.detail || "";

            renderBacktestMetricsGrid(result.metrics || {});
            renderBacktestEquityChart(result.equity_curve || []);
            renderBacktestTradeLog(result.trade_log || []);
        } catch (e) {
            console.error("Failed to load backtest result", e);
        }
    }

    function renderBacktestMetricsGrid(metrics) {
        const grid = document.getElementById("backtest-metrics-grid");
        if (!metrics.total_trades && metrics.total_trades !== 0) {
            grid.innerHTML = "";
            return;
        }
        const pnlClass = (metrics.net_profit_loss || 0) >= 0 ? "text-green" : "text-red";
        const pf = metrics.profit_factor;
        const tiles = [
            ["Total Trades", metrics.total_trades ?? 0],
            ["Winning Trades", metrics.winning_trades ?? 0],
            ["Losing Trades", metrics.losing_trades ?? 0],
            ["Win Rate", `${(metrics.win_rate_pct ?? 0).toFixed(1)}%`],
            ["Loss Rate", `${(metrics.loss_rate_pct ?? 0).toFixed(1)}%`],
            ["Net P&L", `₹${(metrics.net_profit_loss ?? 0).toFixed(2)}`, pnlClass],
            ["Profit Factor", pf === null || pf === undefined ? "N/A" : pf.toFixed(2)],
            ["Max Drawdown", `${(metrics.drawdown_pct ?? 0).toFixed(1)}%`],
            ["Sharpe Ratio", (metrics.sharpe_ratio ?? 0).toFixed(2)],
        ];
        grid.innerHTML = tiles.map(([label, value, cls]) => `
            <div class="metric-card">
                <span class="label">${label}</span>
                <span class="value ${cls || ''}">${value}</span>
            </div>
        `).join("");
    }

    function renderBacktestEquityChart(equityCurve) {
        const ctx = document.getElementById("backtest-equity-chart");
        if (!ctx || typeof Chart === "undefined") return;

        const labels = equityCurve.map((p, i) => i === 0 ? "Start" : `Trade ${i}`);
        const data = equityCurve.map(p => p.equity);

        if (backtestEquityChart) {
            backtestEquityChart.data.labels = labels;
            backtestEquityChart.data.datasets[0].data = data;
            backtestEquityChart.update();
            return;
        }
        backtestEquityChart = new Chart(ctx, {
            type: "line",
            data: {
                labels: labels,
                datasets: [{
                    label: "Equity (₹)",
                    data: data,
                    borderColor: "#00f5a0",
                    backgroundColor: "rgba(0, 245, 160, 0.1)",
                    fill: true,
                    tension: 0.2,
                    borderWidth: 2,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: true, labels: { color: "#f3f4f6" } },
                    tooltip: { mode: "index", intersect: false },
                },
                scales: {
                    y: { grid: { color: "rgba(255,255,255,0.05)" }, ticks: { color: "#9ca3af" } },
                    x: { grid: { display: false }, ticks: { color: "#9ca3af" } },
                },
            },
        });
    }

    function renderBacktestTradeLog(tradeLog) {
        const body = document.getElementById("backtest-trade-log-body");
        if (!tradeLog || tradeLog.length === 0) {
            body.innerHTML = `<tr><td colspan="7" class="text-center text-muted">No trades in this run.</td></tr>`;
            return;
        }
        body.innerHTML = tradeLog.map(t => {
            const pnlClass = t.realized_pnl >= 0 ? "text-green" : "text-red";
            return `
                <tr>
                    <td>${new Date(t.timestamp).toLocaleString()}</td>
                    <td><b>${t.symbol}</b></td>
                    <td><span class="${t.side === 'BUY' ? 'text-green' : 'text-red'}">${t.side}</span></td>
                    <td>₹${Number(t.entry_price).toFixed(2)}</td>
                    <td>${t.exit_price !== null && t.exit_price !== undefined ? "₹" + Number(t.exit_price).toFixed(2) : "-"}</td>
                    <td>${t.quantity}</td>
                    <td class="${pnlClass}">₹${Number(t.realized_pnl).toFixed(2)}</td>
                </tr>
            `;
        }).join("");
    }

    // ── Strategy Studio ──────────────────────────────────────────────────────

    async function ensureStudioSchema() {
        if (studioSchema) return studioSchema;
        const res = await fetch(`${apiBase}/api/strategy-studio/schema`);
        studioSchema = await res.json();
        return studioSchema;
    }

    async function loadStudioStrategyList() {
        await ensureStudioSchema();
        try {
            const res = await fetch(`${apiBase}/api/strategy-studio/strategies`);
            studioStrategies = await res.json();
        } catch (e) {
            console.error("Failed to load Strategy Studio list", e);
            studioStrategies = [];
        }
        const container = document.getElementById("studio-strategy-list");
        container.innerHTML = "";
        if (studioStrategies.length === 0) {
            container.innerHTML = `<p style="color: var(--text-secondary); font-size: 0.85rem; padding: 0.5rem 0;">No strategies yet — click "+ New" to build one.</p>`;
        }
        studioStrategies.forEach(s => {
            const row = document.createElement("div");
            row.className = "studio-strategy-row";
            row.style.cssText = "padding: 0.6rem 0; border-bottom: 1px solid var(--border-glass);";
            row.innerHTML = `
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <strong style="cursor:pointer;" data-open="${s.strategy_name}">${s.strategy_name}</strong>
                    <span class="badge ${s.enabled ? 'text-green' : ''}" style="font-size: 0.7rem;">v${s.active_version}</span>
                </div>
                <div style="font-size: 0.78rem; color: var(--text-secondary); margin: 0.2rem 0;">${s.description || ''}</div>
                <div style="display:flex; gap: 0.4rem; margin-top: 0.3rem;">
                    <button class="btn-action btn-xs" data-open="${s.strategy_name}">Edit</button>
                    <button class="btn-action btn-xs" data-clone="${s.strategy_name}">Clone</button>
                    <button class="btn-danger btn-xs" data-delete="${s.strategy_name}">Delete</button>
                </div>
            `;
            container.appendChild(row);
        });
    }

    function studioSourceOptions(selected) {
        return studioSchema.sources.map(s => `<option value="${s}" ${s === selected ? "selected" : ""}>${s}</option>`).join("");
    }

    function studioOperatorOptions(selected) {
        return studioSchema.operators.map(o => `<option value="${o}" ${o === selected ? "selected" : ""}>${o}</option>`).join("");
    }

    function studioIndicatorOptions(selected) {
        let opts = `<option value="">-- text/number --</option>`;
        opts += studioSchema.indicators.map(i => `<option value="${i.key}" ${i.key === selected ? "selected" : ""}>${i.key}</option>`).join("");
        return opts;
    }

    function createConditionRow(containerId, condition) {
        condition = condition || {};
        const rowId = `cond-row-${studioConditionRowSeq++}`;
        const container = document.getElementById(containerId);
        const row = document.createElement("div");
        row.className = "studio-condition-row";
        row.id = rowId;
        row.style.cssText = "display: grid; grid-template-columns: 1.2fr 1.2fr 1fr 1fr 1.2fr auto; gap: 0.4rem; align-items: center; margin-bottom: 0.5rem;";

        const isIndicator = (condition.source || "indicator") === "indicator";
        row.innerHTML = `
            <select class="cond-source" style="padding: 0.35rem; border-radius: 6px; background: rgba(0,0,0,0.3); color: var(--text-primary); border: 1px solid var(--border-glass);">${studioSourceOptions(condition.source || "indicator")}</select>
            <select class="cond-key" style="padding: 0.35rem; border-radius: 6px; background: rgba(0,0,0,0.3); color: var(--text-primary); border: 1px solid var(--border-glass); display:${isIndicator ? "block" : "none"};">${studioIndicatorOptions(condition.key)}</select>
            <input class="cond-key-text" type="text" placeholder="key" value="${condition.source && !isIndicator ? (condition.key || "") : ""}" style="padding: 0.35rem; border-radius: 6px; background: rgba(0,0,0,0.2); color: var(--text-primary); border: 1px solid var(--border-glass); display:${isIndicator ? "none" : "block"};">
            <select class="cond-operator" style="padding: 0.35rem; border-radius: 6px; background: rgba(0,0,0,0.3); color: var(--text-primary); border: 1px solid var(--border-glass);">${studioOperatorOptions(condition.operator)}</select>
            <input class="cond-value" type="text" placeholder="value (e.g. 50 or TRENDING_BULLISH)" value="${condition.value !== undefined && condition.value !== null ? condition.value : ""}" style="padding: 0.35rem; border-radius: 6px; background: rgba(0,0,0,0.2); color: var(--text-primary); border: 1px solid var(--border-glass);">
            <div style="display:flex; gap:0.3rem;">
                <select class="cond-target" style="flex:1; padding: 0.35rem; border-radius: 6px; background: rgba(0,0,0,0.3); color: var(--text-primary); border: 1px solid var(--border-glass);" title="Target indicator (for comparing two indicators)">${studioIndicatorOptions(condition.target)}</select>
                <button class="btn-danger btn-xs" type="button" data-remove-row="${rowId}">✕</button>
            </div>
        `;
        container.appendChild(row);

        row.querySelector(".cond-source").addEventListener("change", (e) => {
            const indicatorMode = e.target.value === "indicator";
            row.querySelector(".cond-key").style.display = indicatorMode ? "block" : "none";
            row.querySelector(".cond-key-text").style.display = indicatorMode ? "none" : "block";
        });
        row.querySelector("[data-remove-row]").addEventListener("click", () => row.remove());
    }

    function collectConditionsFromContainer(containerId) {
        const container = document.getElementById(containerId);
        const conditions = [];
        container.querySelectorAll(".studio-condition-row").forEach(row => {
            const source = row.querySelector(".cond-source").value;
            const isIndicator = source === "indicator";
            const key = isIndicator ? row.querySelector(".cond-key").value : row.querySelector(".cond-key-text").value;
            const operator = row.querySelector(".cond-operator").value;
            const rawValue = row.querySelector(".cond-value").value;
            const target = row.querySelector(".cond-target").value;

            const cond = { source, operator: operator };
            if (key) cond.key = key;
            if (target) cond.target = target;
            if (rawValue !== "") {
                const num = Number(rawValue);
                cond.value = (rawValue.trim() !== "" && !isNaN(num)) ? num : rawValue;
            }
            conditions.push(cond);
        });
        return conditions;
    }

    function resetStudioForm() {
        document.getElementById("studio-name").value = "";
        document.getElementById("studio-name").disabled = false;
        document.getElementById("studio-description").value = "";
        document.getElementById("studio-enabled").checked = true;
        document.getElementById("studio-entry-operator").value = "AND";
        document.getElementById("studio-exit-operator").value = "AND";
        document.getElementById("studio-entry-confidence").value = 85;
        document.getElementById("studio-entry-reason").value = "";
        document.getElementById("studio-exit-confidence").value = 80;
        document.getElementById("studio-exit-reason").value = "";
        document.getElementById("studio-entry-conditions").innerHTML = "";
        document.getElementById("studio-exit-conditions").innerHTML = "";
        document.getElementById("studio-validation-errors").style.display = "none";
        document.getElementById("studio-version-history").innerHTML = "";
    }

    async function openStudioNew() {
        await ensureStudioSchema();
        studioCurrentName = null;
        resetStudioForm();
        document.getElementById("studio-editor-title").textContent = "New Strategy";
        document.getElementById("studio-editor-card").style.display = "block";
        document.getElementById("studio-empty-state").style.display = "none";
        createConditionRow("studio-entry-conditions");
        createConditionRow("studio-exit-conditions");
    }

    async function openStudioEdit(name) {
        await ensureStudioSchema();
        try {
            const res = await fetch(`${apiBase}/api/strategy-studio/strategies/${encodeURIComponent(name)}/versions`);
            const versions = await res.json();
            const active = versions.find(v => v.is_active) || versions[0];

            studioCurrentName = name;
            resetStudioForm();
            document.getElementById("studio-editor-title").textContent = `Edit: ${name}`;
            document.getElementById("studio-editor-card").style.display = "block";
            document.getElementById("studio-empty-state").style.display = "none";

            document.getElementById("studio-name").value = name;
            document.getElementById("studio-name").disabled = true; // renaming = clone instead
            const def = active.definition;
            document.getElementById("studio-description").value = def.description || "";
            document.getElementById("studio-enabled").checked = def.enabled !== false;

            document.getElementById("studio-entry-operator").value = def.rules.operator;
            (def.rules.conditions || []).forEach(c => createConditionRow("studio-entry-conditions", c));
            const matched = (def.actions || {}).matched || {};
            document.getElementById("studio-entry-confidence").value = matched.confidence || 85;
            document.getElementById("studio-entry-reason").value = matched.reason || "";

            if (def.exit_rules) {
                document.getElementById("studio-exit-operator").value = def.exit_rules.operator;
                (def.exit_rules.conditions || []).forEach(c => createConditionRow("studio-exit-conditions", c));
            }
            const exitAction = (def.actions || {}).exit || {};
            document.getElementById("studio-exit-confidence").value = exitAction.confidence || 80;
            document.getElementById("studio-exit-reason").value = exitAction.reason || "";

            renderVersionHistory(versions);
        } catch (e) {
            console.error("Failed to open strategy for editing", e);
        }
    }

    function renderVersionHistory(versions) {
        const container = document.getElementById("studio-version-history");
        container.innerHTML = "";
        versions.sort((a, b) => b.version - a.version).forEach(v => {
            const row = document.createElement("div");
            row.style.cssText = "display:flex; justify-content:space-between; align-items:center; padding: 0.4rem 0; border-bottom: 1px solid var(--border-glass); font-size: 0.85rem;";
            row.innerHTML = `
                <span>v${v.version} ${v.is_active ? '<span class="badge text-green" style="font-size:0.7rem;">ACTIVE</span>' : ''} — ${new Date(v.created_at).toLocaleString()}</span>
                ${v.is_active ? "" : `<button class="btn-action btn-xs" data-activate-version="${v.version}">Activate</button>`}
            `;
            container.appendChild(row);
        });
        container.querySelectorAll("[data-activate-version]").forEach(btn => {
            btn.addEventListener("click", async () => {
                await fetch(`${apiBase}/api/strategy-studio/strategies/${encodeURIComponent(studioCurrentName)}/activate/${btn.getAttribute("data-activate-version")}`, { method: "POST" });
                await loadStudioStrategyList();
                await openStudioEdit(studioCurrentName);
            });
        });
    }

    function buildDefinitionFromForm() {
        const name = document.getElementById("studio-name").value.trim();
        const entryConditions = collectConditionsFromContainer("studio-entry-conditions");
        const exitConditions = collectConditionsFromContainer("studio-exit-conditions");

        const definition = {
            name,
            version: "1.0.0",
            description: document.getElementById("studio-description").value,
            enabled: document.getElementById("studio-enabled").checked,
            rules: {
                operator: document.getElementById("studio-entry-operator").value,
                conditions: entryConditions,
            },
            actions: {
                matched: {
                    action: "SIGNAL_BUY",
                    confidence: Number(document.getElementById("studio-entry-confidence").value) || 0,
                    reason: document.getElementById("studio-entry-reason").value || "Entry rules matched.",
                },
            },
        };

        if (exitConditions.length > 0) {
            definition.exit_rules = {
                operator: document.getElementById("studio-exit-operator").value,
                conditions: exitConditions,
            };
            definition.actions.exit = {
                action: "SIGNAL_SELL",
                confidence: Number(document.getElementById("studio-exit-confidence").value) || 0,
                reason: document.getElementById("studio-exit-reason").value || "Exit rules matched.",
            };
        }

        return { name, definition };
    }

    function showStudioErrors(errors) {
        const el = document.getElementById("studio-validation-errors");
        if (!errors || errors.length === 0) {
            el.style.display = "none";
            el.innerHTML = "";
            return;
        }
        el.style.display = "block";
        el.innerHTML = `<strong>Fix these before saving:</strong><ul style="margin: 0.4rem 0 0 1.2rem;">${errors.map(e => `<li>${e}</li>`).join("")}</ul>`;
    }

    async function validateStudioForm() {
        const { name, definition } = buildDefinitionFromForm();
        if (!name) {
            showStudioErrors(["Strategy name is required."]);
            return false;
        }
        try {
            const res = await fetch(`${apiBase}/api/strategy-studio/validate`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ definition }),
            });
            const result = await res.json();
            showStudioErrors(result.errors);
            return result.valid;
        } catch (e) {
            console.error("Validation request failed", e);
            return false;
        }
    }

    async function saveStudioForm() {
        const valid = await validateStudioForm();
        if (!valid) return;
        const { name, definition } = buildDefinitionFromForm();
        try {
            const res = await fetch(`${apiBase}/api/strategy-studio/strategies/${encodeURIComponent(name)}`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ definition, activate: true }),
            });
            if (!res.ok) {
                const err = await res.json();
                showStudioErrors(Array.isArray(err.message) ? err.message : [err.message || "Save failed."]);
                return;
            }
            appendTerminalLog("dashboard_client", "strategy_studio", `Saved strategy '${name}'.`);
            await loadStudioStrategyList();
            await openStudioEdit(name);
        } catch (e) {
            console.error("Failed to save strategy", e);
        }
    }

    async function cloneStudioStrategy(name) {
        const newName = prompt(`Clone "${name}" as:`, `${name}_copy`);
        if (!newName) return;
        try {
            const res = await fetch(`${apiBase}/api/strategy-studio/strategies/${encodeURIComponent(name)}/clone`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ new_name: newName }),
            });
            if (!res.ok) {
                const err = await res.json();
                alert(err.detail || "Clone failed.");
                return;
            }
            await loadStudioStrategyList();
            await openStudioEdit(newName);
        } catch (e) {
            console.error("Failed to clone strategy", e);
        }
    }

    async function deleteStudioStrategy(name) {
        if (!confirm(`Delete strategy "${name}" and all its versions? This can't be undone.`)) return;
        try {
            await fetch(`${apiBase}/api/strategy-studio/strategies/${encodeURIComponent(name)}`, { method: "DELETE" });
            if (studioCurrentName === name) {
                document.getElementById("studio-editor-card").style.display = "none";
                document.getElementById("studio-empty-state").style.display = "block";
                studioCurrentName = null;
            }
            await loadStudioStrategyList();
        } catch (e) {
            console.error("Failed to delete strategy", e);
        }
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
            lastEmployeeList = data.employees;
            const relevantOnlyChk = document.getElementById("chk-relevant-only");
            if (relevantOnlyChk && relevantOnlyChk.checked) {
                applyEmployeeRelevanceFilter();
            } else {
                renderMonitoringPage(data.employees);
            }
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

            latestOpenPositions = data.open_positions;
            updateMySymbolCard();

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

    // Kotak Neo feed connection status — no simulated fallback, so when the
    // feed is down we show "Waiting for live market feed..." instead of stale/fake prices.
    // Distinct from "Market Closed", which means the feed is fine but there's
    // genuinely no session right now (checked via fetchFeedStatus's market_session).
    function setFeedStatus(connected) {
        const badge = document.getElementById("feed-status-badge");
        const groups = document.getElementById("market-groups");
        if (badge) {
            badge.textContent = connected ? "Feed Connected" : "Feed Disconnected";
            badge.classList.toggle("feed-connected", connected);
            badge.classList.toggle("feed-disconnected", !connected);
        }
        if (groups) {
            groups.classList.toggle("feed-is-disconnected", !connected);
            if (connected) groups.classList.remove("market-is-closed");
        }
        if (!connected) {
            marketDataStore = {};
            const commodityRow = document.getElementById("prices-row-commodity");
            if (commodityRow) commodityRow.innerHTML = "";
            renderMarketIndices();
            renderTopGainersLosers();
        }
    }

    function setMarketClosed(closed) {
        const groups = document.getElementById("market-groups");
        if (!groups) return;
        // Market Closed only makes sense to show when the feed itself is fine —
        // if the feed is actually down, that overlay already takes priority.
        if (closed && !groups.classList.contains("feed-is-disconnected")) {
            groups.classList.add("market-is-closed");
        } else {
            groups.classList.remove("market-is-closed");
        }
    }

    async function fetchFeedStatus() {
        try {
            const res = await fetch(`${apiBase}/api/market/feed`);
            const data = await res.json();
            setFeedStatus(!!data.stream_connected);
            setMarketClosed(data.market_session !== "OPEN");
        } catch (e) {
            console.error("Failed to fetch feed status", e);
            setFeedStatus(false);
        }
    }

    // Briefly flashes an element green/up or red/down when its value changes.
    function flashElement(el, direction) {
        if (!el || !direction) return;
        el.classList.remove("flash-up", "flash-down");
        // Force reflow so the animation restarts even if it fires again quickly.
        void el.offsetWidth;
        el.classList.add(direction === "up" ? "flash-up" : "flash-down");
    }

    function populateSearchAndFocus(symbol) {
        const input = document.getElementById("market-search");
        if (!input) return;
        input.value = symbol;
        input.dispatchEvent(new Event("input"));
        input.scrollIntoView({ behavior: "smooth", block: "center" });
        input.focus();
    }

    // Live price widget — the single entry point every tick flows through.
    // Updates the central store, then re-renders only the widget(s) that symbol
    // belongs to (Market Indices / Commodity Watch / Top Gainers-Losers).
    function updatePriceWidget(tick) {
        const symbol = tick.symbol;
        const ltp = typeof tick.ltp === "number" ? tick.ltp : parseFloat(tick.ltp);
        if (!symbol || Number.isNaN(ltp)) return;

        const prevEntry = marketDataStore[symbol];
        const prevLtp = prevEntry ? prevEntry.ltp : undefined;
        const prevClose = (prevEntry && prevEntry.prevClose) || tick.prev_close || ltp;
        const change = prevClose ? ltp - prevClose : 0;
        const changePct = prevClose ? (change / prevClose) * 100 : 0;

        marketDataStore[symbol] = {
            ltp, change, changePct, prevClose,
            volume: typeof tick.volume === "number" ? tick.volume : (prevEntry ? prevEntry.volume : 0),
        };

        const direction = prevLtp === undefined ? null : (ltp > prevLtp ? "up" : ltp < prevLtp ? "down" : null);

        if (INDEX_SYMBOLS.includes(symbol)) {
            renderMarketIndices();
            flashElement(document.getElementById(`index-card-${symbol}`), direction);
            return;
        }

        const segment = symbolSegments[symbol];
        if (segment === "COMMODITY") {
            renderCommodityWatch();
            flashElement(document.getElementById(`price-card-${symbol}`), direction);
            return;
        }

        if (segment === "EQUITY") {
            renderTopGainersLosers();
        }
    }

    // ── Market Indices panel ──────────────────────────────────────────────────
    function renderMarketIndices() {
        const row = document.getElementById("indices-row");
        if (!row) return;
        row.innerHTML = INDEX_SYMBOLS.map(sym => {
            const d = marketDataStore[sym];
            const label = INDEX_LABELS[sym] || sym;
            if (!d) {
                return `<div class="index-card" id="index-card-${sym}">
                    <div class="index-name">${label}</div>
                    <div class="index-ltp text-muted">—</div>
                    <div class="index-change text-muted">Waiting for tick...</div>
                </div>`;
            }
            const cls = d.change >= 0 ? "text-green" : "text-red";
            const sign = d.change >= 0 ? "+" : "";
            return `<div class="index-card" id="index-card-${sym}">
                <div class="index-name">${label}</div>
                <div class="index-ltp">${d.ltp.toFixed(2)}</div>
                <div class="index-change ${cls}">${sign}${d.change.toFixed(2)} (${sign}${d.changePct.toFixed(2)}%)</div>
            </div>`;
        }).join("");
    }

    // ── Commodity Watch panel ─────────────────────────────────────────────────
    function renderCommodityWatch() {
        const row = document.getElementById("prices-row-commodity");
        if (!row) return;
        row.innerHTML = COMMODITY_SYMBOLS.map(sym => {
            const d = marketDataStore[sym];
            if (!d) {
                return `<div class="price-card" id="price-card-${sym}">
                    <div class="symbol">${sym}</div>
                    <div class="price text-muted">—</div>
                    <div class="change text-muted">Waiting for tick...</div>
                </div>`;
            }
            const cls = d.change >= 0 ? "text-green" : "text-red";
            const sign = d.change >= 0 ? "+" : "";
            return `<div class="price-card" id="price-card-${sym}">
                <div class="symbol">${sym}</div>
                <div class="price">₹${d.ltp.toFixed(2)}</div>
                <div class="change ${cls}">${sign}${d.change.toFixed(2)} (${sign}${d.changePct.toFixed(2)}%)</div>
            </div>`;
        }).join("");
    }

    // ── Top Gainers / Top Losers panels ───────────────────────────────────────
    function renderTopGainersLosers() {
        const equitySymbols = Object.keys(symbolSegments).filter(sym => symbolSegments[sym] === "EQUITY");
        const ranked = equitySymbols
            .filter(sym => marketDataStore[sym])
            .map(sym => ({ symbol: sym, ...marketDataStore[sym] }));

        ranked.sort((a, b) => b.changePct - a.changePct);
        const gainers = ranked.filter(r => r.changePct > 0).slice(0, 10);
        const losers = ranked.filter(r => r.changePct < 0).slice(-10).reverse();

        renderRankedTable("top-gainers-body", gainers, "up");
        renderRankedTable("top-losers-body", losers, "down");
    }

    function renderRankedTable(tbodyId, rows, direction) {
        const tbody = document.getElementById(tbodyId);
        if (!tbody) return;
        if (rows.length === 0) {
            tbody.innerHTML = `<tr><td colspan="3" class="mini-table-empty">-</td></tr>`;
            return;
        }
        const icon = direction === "up" ? "▲" : "▼";
        const cls = direction === "up" ? "text-green" : "text-red";
        tbody.innerHTML = rows.map(r => `
            <tr onclick="window.selectMarketSymbol('${r.symbol}')">
                <td class="sym-cell">${r.symbol}</td>
                <td>${r.ltp.toFixed(2)}</td>
                <td class="${cls}">${icon} ${r.changePct.toFixed(2)}%</td>
            </tr>
        `).join("");
    }

    window.selectMarketSymbol = function(symbol) {
        populateSearchAndFocus(symbol);
    };

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
