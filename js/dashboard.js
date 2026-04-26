/**
 * Dashboard module - Summary charts across all benchmarks
 */

/**
 * Render the Dashboard page content
 */
async function renderDashboardPageContent() {
    const container = document.getElementById('dashboard-content');
    
    if (!AppState.indexData) {
        container.innerHTML = '<p>Index data not loaded</p>';
        return;
    }

    const html = `
        <h2>Performance Dashboard</h2>
        <p class="mb-2">High-level performance trends across all benchmarks</p>
        <div class="dashboard-charts" id="dashboard-charts">
            <p class="text-center">Loading dashboard...</p>
        </div>
    `;

    container.innerHTML = html;

    // Load and render dashboard charts
    await renderDashboardCharts();
}

/**
 * Render dashboard summary charts
 */
async function renderDashboardCharts() {
    const chartsContainer = document.getElementById('dashboard-charts');
    chartsContainer.innerHTML = '<p class="text-center">Loading dashboard...</p>';

    const benchmarks = AppState.indexData.benchmarks || [];
    const engines = AppState.indexData.engines || [];

    if (benchmarks.length === 0 || engines.length === 0) {
        chartsContainer.innerHTML = '<p class="text-center">No data available</p>';
        return;
    }

    // Get date range (last 15 days)
    const { startDate, endDate } = getDefaultDateRange();

    // Load all benchmark data for all engines
    const allBenchmarkData = {};
    for (const benchmark of benchmarks) {
        allBenchmarkData[benchmark] = await loadBenchmarkDataAllEngines(benchmark);
    }

    // Create summary charts
    chartsContainer.innerHTML = '';

    // Chart 1: Average Latency (all insert benchmarks)
    await renderCategorySummaryChart(
        chartsContainer,
        allBenchmarkData,
        'insert',
        'avg_response_time_ms',
        'Average Insert Latency (ms)',
        startDate,
        endDate
    );

    // Chart 2: Average Latency (all count benchmarks)
    await renderCategorySummaryChart(
        chartsContainer,
        allBenchmarkData,
        'count',
        'avg_response_time_ms',
        'Average Count Latency (ms)',
        startDate,
        endDate
    );

    // Chart 3: Average RPS (all benchmarks)
    await renderOverallSummaryChart(
        chartsContainer,
        allBenchmarkData,
        'requests_per_sec',
        'Average Requests per Second (All Benchmarks)',
        startDate,
        endDate
    );

    // Chart 4: P99 Latency Trend (all benchmarks)
    await renderOverallSummaryChart(
        chartsContainer,
        allBenchmarkData,
        'p99',
        'P99 Latency Trend (All Benchmarks, ms)',
        startDate,
        endDate
    );
}

/**
 * Render a summary chart for a specific benchmark category
 */
async function renderCategorySummaryChart(
    container,
    allBenchmarkData,
    category,
    metric,
    title,
    startDate,
    endDate
) {
    // Filter benchmarks by category
    const categoryBenchmarks = Object.keys(allBenchmarkData).filter(b => 
        b.toLowerCase().startsWith(category.toLowerCase())
    );

    if (categoryBenchmarks.length === 0) {
        return; // Skip if no benchmarks in this category
    }

    // Aggregate data by engine
    const engineData = {};
    
    for (const [benchmark, benchmarkEngineData] of Object.entries(allBenchmarkData)) {
        if (!categoryBenchmarks.includes(benchmark)) continue;

        for (const [engine, entries] of Object.entries(benchmarkEngineData)) {
            if (!engineData[engine]) {
                engineData[engine] = {};
            }

            // Filter by date range
            const filtered = filterByDateRange(entries, startDate, endDate);

            // Group by timestamp and collect values
            for (const entry of filtered) {
                const timestamp = entry.timestamp;
                if (!engineData[engine][timestamp]) {
                    engineData[engine][timestamp] = [];
                }

                const value = extractMetricValue(entry, metric);
                if (value !== null) {
                    engineData[engine][timestamp].push(value);
                }
            }
        }
    }

    // Calculate averages for each timestamp
    const chartData = {};
    for (const [engine, timestampData] of Object.entries(engineData)) {
        chartData[engine] = Object.entries(timestampData).map(([timestamp, values]) => {
            const avg = values.reduce((sum, v) => sum + v, 0) / values.length;
            return { timestamp, value: avg };
        }).sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    }

    // Create chart container
    const chartContainer = document.createElement('div');
    chartContainer.className = 'chart-container large';
    chartContainer.innerHTML = `
        <h3>${title}</h3>
        <canvas id="dashboard-chart-${category}-${metric}"></canvas>
    `;
    container.appendChild(chartContainer);

    // Render chart
    const canvas = document.getElementById(`dashboard-chart-${category}-${metric}`);
    const chart = createSummaryChart(
        canvas,
        chartData,
        formatMetricName(metric),
        title
    );
    AppState.charts.push(chart);
}

/**
 * Render a summary chart across all benchmarks
 */
async function renderOverallSummaryChart(
    container,
    allBenchmarkData,
    metric,
    title,
    startDate,
    endDate
) {
    // Aggregate data by engine across all benchmarks
    const engineData = {};
    
    for (const [benchmark, benchmarkEngineData] of Object.entries(allBenchmarkData)) {
        for (const [engine, entries] of Object.entries(benchmarkEngineData)) {
            if (!engineData[engine]) {
                engineData[engine] = {};
            }

            // Filter by date range
            const filtered = filterByDateRange(entries, startDate, endDate);

            // Group by timestamp and collect values
            for (const entry of filtered) {
                const timestamp = entry.timestamp;
                if (!engineData[engine][timestamp]) {
                    engineData[engine][timestamp] = [];
                }

                const value = extractMetricValue(entry, metric);
                if (value !== null) {
                    engineData[engine][timestamp].push(value);
                }
            }
        }
    }

    // Calculate averages for each timestamp
    const chartData = {};
    for (const [engine, timestampData] of Object.entries(engineData)) {
        chartData[engine] = Object.entries(timestampData).map(([timestamp, values]) => {
            const avg = values.reduce((sum, v) => sum + v, 0) / values.length;
            return { timestamp, value: avg };
        }).sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    }

    // Create chart container
    const chartContainer = document.createElement('div');
    chartContainer.className = 'chart-container large';
    const chartId = `dashboard-chart-overall-${metric}`;
    chartContainer.innerHTML = `
        <h3>${title}</h3>
        <canvas id="${chartId}"></canvas>
    `;
    container.appendChild(chartContainer);

    // Render chart
    const canvas = document.getElementById(chartId);
    const chart = createSummaryChart(
        canvas,
        chartData,
        formatMetricName(metric),
        title
    );
    AppState.charts.push(chart);
}
