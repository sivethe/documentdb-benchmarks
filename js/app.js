/**
 * Main application module - Router, data loading, and page coordination
 */

// Global state
const AppState = {
    indexData: null,
    benchmarkData: {}, // Cache: { benchmark_name: { engine_name: [...data] } }
    chartTitleOverrides: { benchmarks: {} },
    currentPage: 'graphs',
    charts: [], // Track active Chart.js instances for cleanup
};

// DOM elements
let loadingEl, errorEl, contentEl;

/**
 * Initialize the application
 */
async function init() {
    // Get DOM elements
    loadingEl = document.getElementById('loading');
    errorEl = document.getElementById('error');
    contentEl = document.getElementById('content');

    // Set up navigation
    setupNavigation();

    // Set up hash change listener for routing
    window.addEventListener('hashchange', handleRouteChange);

    try {
        // Load startup data
        showLoading();
        await Promise.all([
            loadIndexData(),
            loadChartTitleOverrides()
        ]);
        hideLoading();

        // Handle initial route
        handleRouteChange();
    } catch (error) {
        showError('Failed to load benchmark index: ' + error.message);
    }
}

/**
 * Load chart title overrides from the local JSON file.
 */
async function loadChartTitleOverrides() {
    try {
        const response = await fetch('data/chart_title_overrides.json');
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const rawData = await response.json();
        AppState.chartTitleOverrides = normalizeChartTitleOverrides(rawData);
        console.log('Loaded chart title overrides:', AppState.chartTitleOverrides);
    } catch (error) {
        console.warn('Failed to load chart title overrides, using defaults:', error);
        AppState.chartTitleOverrides = { benchmarks: {} };
    }
}

/**
 * Normalize chart title override data into the expected structure.
 * @param {Object|null} rawData - Raw JSON data
 * @returns {{benchmarks: Object}} - Normalized overrides
 */
function normalizeChartTitleOverrides(rawData) {
    if (!rawData || typeof rawData !== 'object') {
        return { benchmarks: {} };
    }

    const benchmarks = rawData.benchmarks;
    if (!benchmarks || typeof benchmarks !== 'object' || Array.isArray(benchmarks)) {
        return { benchmarks: {} };
    }

    return { benchmarks };
}

/**
 * Escape HTML special characters for safe inline markup.
 * @param {string} value - Raw text
 * @returns {string} - Escaped text
 */
function escapeHtml(value) {
    return String(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

/**
 * Set up navigation link highlighting
 */
function setupNavigation() {
    const navLinks = document.querySelectorAll('.nav-link');
    navLinks.forEach(link => {
        link.addEventListener('click', (e) => {
            navLinks.forEach(l => l.classList.remove('active'));
            e.target.classList.add('active');
        });
    });
}

/**
 * Handle hash route changes
 */
function handleRouteChange() {
    const hash = window.location.hash || '#/graphs';
    const [_, page, ...params] = hash.split('/');
    
    // Clean up previous charts
    cleanupCharts();

    // Update navigation
    const navLinks = document.querySelectorAll('.nav-link');
    navLinks.forEach(link => {
        const linkPage = link.getAttribute('data-page');
        if (linkPage === page) {
            link.classList.add('active');
        } else {
            link.classList.remove('active');
        }
    });

    // Route to appropriate page
    AppState.currentPage = page || 'graphs';
    
    switch (AppState.currentPage) {
        case 'graphs':
            renderGraphsPage();
            break;
        case 'compare':
            renderComparePage();
            break;
        case 'dashboard':
            renderDashboardPage();
            break;
        case 'status':
            renderStatusPage();
            break;
        case 'help':
            renderHelpPage();
            break;
        default:
            renderGraphsPage();
    }
}

/**
 * Load the index.json file
 */
async function loadIndexData() {
    try {
        const response = await fetch('data/index.json');
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        const rawData = await response.json();
        
        // Transform the nested structure to a simpler format for the app
        AppState.indexData = {
            generated_at: rawData.generated_at,
            benchmarks: Object.keys(rawData.benchmarks || {}),
            engines: extractEnginesList(rawData.benchmarks || {}),
            benchmarkDetails: rawData.benchmarks || {}
        };
        
        console.log('Loaded index data:', AppState.indexData);
    } catch (error) {
        console.error('Failed to load index data:', error);
        throw error;
    }
}

/**
 * Extract unique list of engines from benchmark data
 * @param {Object} benchmarks - Benchmarks object from index.json
 * @returns {Array<string>} - Array of unique engine names
 */
function extractEnginesList(benchmarks) {
    const engineSet = new Set();
    for (const benchmark of Object.values(benchmarks)) {
        if (benchmark.engines) {
            Object.keys(benchmark.engines).forEach(engine => engineSet.add(engine));
        }
    }
    return Array.from(engineSet).sort();
}

/**
 * Load benchmark data for a specific benchmark and engine
 * @param {string} benchmarkName - Name of the benchmark
 * @param {string} engineName - Name of the engine
 * @returns {Promise<Array>} - Array of result entries (operations)
 */
async function loadBenchmarkData(benchmarkName, engineName) {
    // Check cache
    const cacheKey = `${benchmarkName}/${engineName}`;
    if (AppState.benchmarkData[cacheKey]) {
        return AppState.benchmarkData[cacheKey];
    }

    try {
        const url = `data/${benchmarkName}/${engineName}.json`;
        const response = await fetch(url);
        if (!response.ok) {
            console.warn(`Failed to load ${url}: ${response.status}`);
            return [];
        }
        const rawData = await response.json();
        
        // Transform data format:
        // If it's an object with operations array, use the operations
        // If it's already an array, use it as-is (old format)
        let data;
        if (Array.isArray(rawData)) {
            data = rawData; // Old format: array of runs
        } else if (rawData.operations && Array.isArray(rawData.operations)) {
            data = rawData.operations; // New format: object with operations array
        } else {
            console.warn(`Unexpected data format for ${benchmarkName}/${engineName}`);
            return [];
        }
        
        AppState.benchmarkData[cacheKey] = data;
        return data;
    } catch (error) {
        console.error(`Failed to load data for ${benchmarkName}/${engineName}:`, error);
        return [];
    }
}

/**
 * Load all data for a benchmark across all engines
 * @param {string} benchmarkName - Name of the benchmark
 * @returns {Promise<Object>} - Object with engine names as keys
 */
async function loadBenchmarkDataAllEngines(benchmarkName) {
    if (!AppState.indexData) {
        throw new Error('Index data not loaded');
    }

    const engines = AppState.indexData.engines || [];
    const results = {};

    await Promise.all(
        engines.map(async (engineName) => {
            results[engineName] = await loadBenchmarkData(benchmarkName, engineName);
        })
    );

    return results;
}

/**
 * Render the Graphs page
 */
async function renderGraphsPage() {
    if (!AppState.indexData) {
        showError('Index data not loaded');
        return;
    }

    // Get query parameters
    const urlParams = new URLSearchParams(window.location.hash.split('?')[1] || '');
    const selectedBenchmarks = urlParams.getAll('benchmark');
    const selectedMetric = urlParams.get('metric') || 'avg_response_time_ms';

    // Get date range from URL parameters or use defaults
    const { startDate: defaultStartDate, endDate: defaultEndDate } = getDefaultDateRange();
    const urlStartDate = urlParams.get('startDate');
    const urlEndDate = urlParams.get('endDate');
    
    const startDate = parseDateInputValue(urlStartDate) || defaultStartDate;
    const endDate = parseDateInputValue(urlEndDate, true) || defaultEndDate;
    const startDateStr = urlStartDate || formatDateForInput(startDate);
    const endDateStr = urlEndDate || formatDateForInput(endDate);

    // Build controls HTML
    const benchmarks = AppState.indexData.benchmarks || [];
    const benchmarkOptions = benchmarks.map(b => 
        `<option value="${b}" ${selectedBenchmarks.includes(b) ? 'selected' : ''}>${b}</option>`
    ).join('');

    const metricOptions = [
        { value: 'avg_response_time_ms', label: 'Avg Response Time' },
        { value: 'p50', label: 'P50 Latency' },
        { value: 'p95', label: 'P95 Latency' },
        { value: 'p99', label: 'P99 Latency' },
        { value: 'requests_per_sec', label: 'Requests per Second' },
    ].map(m => 
        `<option value="${m.value}" ${m.value === selectedMetric ? 'selected' : ''}>${m.label}</option>`
    ).join('');

    const html = `
        <div class="controls">
            <div class="control-group">
                <label for="date-start">Start Date:</label>
                <input type="date" id="date-start" value="${startDateStr}">
            </div>
            <div class="control-group">
                <label for="date-end">End Date:</label>
                <input type="date" id="date-end" value="${endDateStr}">
            </div>
            <div class="control-group">
                <label for="metric-select">Metric:</label>
                <select id="metric-select">${metricOptions}</select>
            </div>
            <div class="control-group">
                <label for="benchmark-select">Benchmarks:</label>
                <select id="benchmark-select" multiple size="5">${benchmarkOptions}</select>
                <small style="display: block; margin-top: 4px; color: #666;">Hold Ctrl/Cmd to select multiple</small>
            </div>
            <div class="control-group">
                <button id="refresh-btn">Refresh</button>
            </div>
        </div>
        <div class="chart-grid" id="chart-grid">
            <p class="text-center">Loading charts...</p>
        </div>
    `;

    contentEl.innerHTML = html;

    // Add event listeners
    document.getElementById('refresh-btn').addEventListener('click', () => {
        // Get current date values from inputs
        const startDateInput = document.getElementById('date-start').value;
        const endDateInput = document.getElementById('date-end').value;
        
        // Get currently selected benchmarks
        const benchmarkSelect = document.getElementById('benchmark-select');
        const currentBenchmarks = Array.from(benchmarkSelect.selectedOptions).map(opt => opt.value);
        
        // Re-render charts with current date range
        renderGraphCharts(
            currentBenchmarks,
            selectedMetric,
            startDateInput || formatDateForInput(startDate),
            endDateInput || formatDateForInput(endDate)
        );
    });

    document.getElementById('benchmark-select').addEventListener('change', (e) => {
        const benchmarkSelect = e.target;
        const selectedBenchmarks = Array.from(benchmarkSelect.selectedOptions).map(opt => opt.value);
        const currentStartDate = document.getElementById('date-start').value;
        const currentEndDate = document.getElementById('date-end').value;
        
        let params = new URLSearchParams();
        params.set('metric', selectedMetric);
        selectedBenchmarks.forEach(b => params.append('benchmark', b));
        if (currentStartDate) params.set('startDate', currentStartDate);
        if (currentEndDate) params.set('endDate', currentEndDate);
        
        window.location.hash = `#/graphs?${params.toString()}`;
    });

    document.getElementById('metric-select').addEventListener('change', (e) => {
        const newMetric = e.target.value;
        const currentStartDate = document.getElementById('date-start').value;
        const currentEndDate = document.getElementById('date-end').value;
        const benchmarkSelect = document.getElementById('benchmark-select');
        const currentBenchmarks = Array.from(benchmarkSelect.selectedOptions).map(opt => opt.value);
        
        let params = new URLSearchParams();
        params.set('metric', newMetric);
        currentBenchmarks.forEach(b => params.append('benchmark', b));
        if (currentStartDate) params.set('startDate', currentStartDate);
        if (currentEndDate) params.set('endDate', currentEndDate);
        
        window.location.hash = `#/graphs?${params.toString()}`;
    });

    // Add date input listeners to update URL
    document.getElementById('date-start').addEventListener('change', (e) => {
        const newStartDate = e.target.value;
        const currentEndDate = document.getElementById('date-end').value;
        const benchmarkSelect = document.getElementById('benchmark-select');
        const currentBenchmarks = Array.from(benchmarkSelect.selectedOptions).map(opt => opt.value);
        
        let params = new URLSearchParams();
        params.set('metric', selectedMetric);
        currentBenchmarks.forEach(b => params.append('benchmark', b));
        if (newStartDate) params.set('startDate', newStartDate);
        if (currentEndDate) params.set('endDate', currentEndDate);
        
        window.location.hash = `#/graphs?${params.toString()}`;
    });

    document.getElementById('date-end').addEventListener('change', (e) => {
        const newEndDate = e.target.value;
        const currentStartDate = document.getElementById('date-start').value;
        const benchmarkSelect = document.getElementById('benchmark-select');
        const currentBenchmarks = Array.from(benchmarkSelect.selectedOptions).map(opt => opt.value);
        
        let params = new URLSearchParams();
        params.set('metric', selectedMetric);
        currentBenchmarks.forEach(b => params.append('benchmark', b));
        if (currentStartDate) params.set('startDate', currentStartDate);
        if (newEndDate) params.set('endDate', newEndDate);
        
        window.location.hash = `#/graphs?${params.toString()}`;
    });

    // Load and render charts
    await renderGraphCharts(
        selectedBenchmarks,
        selectedMetric,
        urlStartDate || startDateStr,
        urlEndDate || endDateStr
    );
}

/**
 * Render charts for the Graphs page
 */
async function renderGraphCharts(selectedBenchmarks, metric, startDate, endDate) {
    const chartGrid = document.getElementById('chart-grid');
    chartGrid.innerHTML = '<p class="text-center">Loading charts...</p>';

    const benchmarks = (selectedBenchmarks && selectedBenchmarks.length > 0)
        ? selectedBenchmarks
        : AppState.indexData.benchmarks || [];

    if (benchmarks.length === 0) {
        chartGrid.innerHTML = '<p class="text-center">No benchmarks found</p>';
        return;
    }

    chartGrid.innerHTML = '';

    // Load data for all benchmarks
    for (const benchmarkName of benchmarks) {
        // Load data for all engines
        const allData = await loadBenchmarkDataAllEngines(benchmarkName);

        const seriesNames = Array.from(
            new Set(
                Object.values(allData)
                    .flat()
                    .map(entry => entry.name)
                    .filter(Boolean)
            )
        );

        const benchmarkSeries = seriesNames.length > 0 ? seriesNames : [null];

        for (const seriesName of benchmarkSeries) {
            const chartData = {};

            for (const [engineName, entries] of Object.entries(allData)) {
                if (!entries || entries.length === 0) continue;

                const filtered = filterByDateRange(entries, startDate, endDate)
                    .filter(entry => !seriesName || entry.name === seriesName);

                chartData[engineName] = filtered.map(entry => ({
                    timestamp: entry.timestamp,
                    value: extractMetricValue(entry, metric)
                })).filter(point => point.value !== null);
            }

            if (Object.values(chartData).every(points => points.length === 0)) {
                continue;
            }

            const chartTitle = resolveChartTitle(benchmarkName, seriesName);
            const chartId = `chart-${toSafeChartId(benchmarkName)}${seriesName ? `-${toSafeChartId(seriesName)}` : ''}`;

            const chartContainer = document.createElement('div');
            chartContainer.className = 'chart-container';
            chartContainer.innerHTML = `
                <h3>${escapeHtml(chartTitle)}</h3>
                <canvas id="${chartId}"></canvas>
            `;
            chartGrid.appendChild(chartContainer);

            const canvas = document.getElementById(chartId);
            const chart = createTimeSeriesChart(
                canvas,
                chartData,
                formatMetricName(metric),
                chartTitle
            );
            AppState.charts.push(chart);
        }
    }
}

/**
 * Convert a benchmark or series name into a DOM-safe chart id fragment.
 * @param {string} value - Source name
 * @returns {string} - Safe id fragment
 */
function toSafeChartId(value) {
    return value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
}

/**
 * Resolve the display title for a benchmark chart.
 * @param {string} benchmarkName - Benchmark identifier
 * @param {string|null} seriesName - Operation name, if present
 * @returns {string} - Chart title
 */
function resolveChartTitle(benchmarkName, seriesName) {
    const benchmarkOverrides = AppState.chartTitleOverrides.benchmarks[benchmarkName];
    const operationOverride = seriesName ? benchmarkOverrides?.operations?.[seriesName] : null;

    if (operationOverride) {
        if (typeof operationOverride === 'string') {
            return operationOverride;
        }

        return operationOverride.title || `${benchmarkName} - ${formatSeriesName(seriesName)}`;
    }

    if (!seriesName && benchmarkOverrides?.title) {
        return benchmarkOverrides.title;
    }

    return seriesName
        ? `${benchmarkName} - ${formatSeriesName(seriesName)}`
        : benchmarkName;
}

/**
 * Render the Compare page
 */
function renderComparePage() {
    contentEl.innerHTML = '<div id="compare-content"></div>';
    renderComparePageContent();
}

/**
 * Render the Dashboard page
 */
function renderDashboardPage() {
    contentEl.innerHTML = '<div id="dashboard-content"></div>';
    renderDashboardPageContent();
}

/**
 * Render the Status page
 */
function renderStatusPage() {
    if (!AppState.indexData) {
        contentEl.innerHTML = '<p>Index data not loaded</p>';
        return;
    }

    const benchmarks = AppState.indexData.benchmarks || [];
    const engines = AppState.indexData.engines || [];

    const statusCards = benchmarks.map(benchmark => {
        return `
            <div class="status-card">
                <h3>${benchmark}</h3>
                <p class="timestamp">Engines: ${engines.join(', ')}</p>
            </div>
        `;
    }).join('');

    const html = `
        <h2>Benchmark Status</h2>
        <p>Available benchmarks and engines in the index:</p>
        <div class="status-grid">
            ${statusCards}
        </div>
    `;

    contentEl.innerHTML = html;
}

/**
 * Render the Help page
 */
function renderHelpPage() {
    const html = `
        <div class="help-section">
            <h2>About DocumentDB Benchmarks</h2>
            <p>
                This site displays performance benchmark results for MongoDB-compatible databases,
                comparing Atlas, Azure DocumentDB, AWS DocumentDB, and MongoDB across various workloads.
            </p>
        </div>

        <div class="help-section">
            <h2>Database Engines</h2>
            <ul>
                <li><strong>Atlas</strong> — MongoDB Atlas (fully managed MongoDB)</li>
                <li><strong>Azure DocumentDB</strong> — Azure Cosmos DB with MongoDB API</li>
                <li><strong>AWS DocumentDB</strong> — Amazon DocumentDB (MongoDB-compatible)</li>
                <li><strong>MongoDB</strong> — Self-hosted MongoDB Community Server</li>
            </ul>
        </div>

        <div class="help-section">
            <h2>Metrics Explained</h2>
            <ul>
                <li><strong>Avg Response Time</strong> — Mean latency for all requests</li>
                <li><strong>P50 Latency</strong> — 50th percentile (median) response time</li>
                <li><strong>P95 Latency</strong> — 95th percentile response time (95% of requests are faster)</li>
                <li><strong>P99 Latency</strong> — 99th percentile response time (99% of requests are faster)</li>
                <li><strong>Requests per Second (RPS)</strong> — Throughput (operations per second)</li>
            </ul>
        </div>

        <div class="help-section">
            <h2>How Benchmarks are Run</h2>
            <p>
                Benchmarks use <a href="https://locust.io" target="_blank">Locust</a>, a Python load testing framework.
                Each benchmark runs with a fixed number of concurrent users for a set duration (typically 60 seconds).
                Benchmarks include insert operations, aggregation queries, and count queries with various indexing strategies.
            </p>
        </div>

        <div class="help-section">
            <h2>Source Code</h2>
            <p>
                All benchmarks are open source. View the code, submit issues, or contribute on 
                <a href="https://github.com/your-org/documentdb-benchmarks" target="_blank">GitHub</a>.
            </p>
        </div>
    `;

    contentEl.innerHTML = html;
}

/**
 * Show loading indicator
 */
function showLoading() {
    loadingEl.style.display = 'block';
    errorEl.style.display = 'none';
    contentEl.style.display = 'none';
}

/**
 * Hide loading indicator
 */
function hideLoading() {
    loadingEl.style.display = 'none';
    contentEl.style.display = 'block';
}

/**
 * Show error message
 */
function showError(message) {
    errorEl.textContent = message;
    errorEl.style.display = 'block';
    loadingEl.style.display = 'none';
}

/**
 * Clean up all active charts
 */
function cleanupCharts() {
    AppState.charts.forEach(chart => destroyChart(chart));
    AppState.charts = [];
}

// Initialize app when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}
