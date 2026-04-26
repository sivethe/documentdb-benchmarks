/**
 * Main application module - Router, data loading, and page coordination
 */

// Global state
const AppState = {
    indexData: null,
    benchmarkData: {}, // Cache: { benchmark_name: { engine_name: [...data] } }
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
        // Load index data
        showLoading();
        await loadIndexData();
        hideLoading();

        // Handle initial route
        handleRouteChange();
    } catch (error) {
        showError('Failed to load benchmark index: ' + error.message);
    }
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
    const selectedBenchmark = urlParams.get('benchmark');
    const selectedMetric = urlParams.get('metric') || 'avg_response_time_ms';

    // Get date range
    const { startDate, endDate } = getDefaultDateRange();
    const startDateStr = formatDateForInput(startDate);
    const endDateStr = formatDateForInput(endDate);

    // Build controls HTML
    const benchmarks = AppState.indexData.benchmarks || [];
    const benchmarkOptions = ['<option value="">All Benchmarks</option>']
        .concat(benchmarks.map(b => 
            `<option value="${b}" ${b === selectedBenchmark ? 'selected' : ''}>${b}</option>`
        ))
        .join('');

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
                <label for="benchmark-select">Benchmark:</label>
                <select id="benchmark-select">${benchmarkOptions}</select>
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
        renderGraphsPage();
    });

    document.getElementById('benchmark-select').addEventListener('change', (e) => {
        const newBenchmark = e.target.value;
        const newHash = newBenchmark 
            ? `#/graphs?benchmark=${newBenchmark}&metric=${selectedMetric}`
            : `#/graphs?metric=${selectedMetric}`;
        window.location.hash = newHash;
    });

    document.getElementById('metric-select').addEventListener('change', (e) => {
        const newMetric = e.target.value;
        const newHash = selectedBenchmark
            ? `#/graphs?benchmark=${selectedBenchmark}&metric=${newMetric}`
            : `#/graphs?metric=${newMetric}`;
        window.location.hash = newHash;
    });

    // Load and render charts
    await renderGraphCharts(selectedBenchmark, selectedMetric, startDate, endDate);
}

/**
 * Render charts for the Graphs page
 */
async function renderGraphCharts(selectedBenchmark, metric, startDate, endDate) {
    const chartGrid = document.getElementById('chart-grid');
    chartGrid.innerHTML = '<p class="text-center">Loading charts...</p>';

    const benchmarks = selectedBenchmark 
        ? [selectedBenchmark]
        : AppState.indexData.benchmarks || [];

    if (benchmarks.length === 0) {
        chartGrid.innerHTML = '<p class="text-center">No benchmarks found</p>';
        return;
    }

    chartGrid.innerHTML = '';

    // Load data for all benchmarks
    for (const benchmarkName of benchmarks) {
        const chartContainer = document.createElement('div');
        chartContainer.className = 'chart-container';
        chartContainer.innerHTML = `
            <h3>${benchmarkName}</h3>
            <canvas id="chart-${benchmarkName}"></canvas>
        `;
        chartGrid.appendChild(chartContainer);

        // Load data for all engines
        const allData = await loadBenchmarkDataAllEngines(benchmarkName);

        // Prepare chart data
        const chartData = {};
        for (const [engineName, entries] of Object.entries(allData)) {
            if (!entries || entries.length === 0) continue;

            const filtered = filterByDateRange(entries, startDate, endDate);
            chartData[engineName] = filtered.map(entry => ({
                timestamp: entry.timestamp,
                value: extractMetricValue(entry, metric)
            })).filter(point => point.value !== null);
        }

        // Render chart
        const canvas = document.getElementById(`chart-${benchmarkName}`);
        const chart = createTimeSeriesChart(
            canvas,
            chartData,
            formatMetricName(metric),
            benchmarkName
        );
        AppState.charts.push(chart);
    }
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
