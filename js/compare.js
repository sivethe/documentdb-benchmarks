/**
 * Compare module - Engine comparison functionality
 */

/**
 * Render the Compare page content
 */
async function renderComparePageContent() {
    const container = document.getElementById('compare-content');
    
    if (!AppState.indexData) {
        container.innerHTML = '<p>Index data not loaded</p>';
        return;
    }

    const engines = AppState.indexData.engines || [];
    const benchmarks = AppState.indexData.benchmarks || [];

    if (engines.length < 2) {
        container.innerHTML = '<p>Need at least 2 engines to compare</p>';
        return;
    }

    // Get query parameters
    const urlParams = new URLSearchParams(window.location.hash.split('?')[1] || '');
    const engineA = urlParams.get('a') || engines[0];
    const engineB = urlParams.get('b') || engines[1];

    // Build engine selectors
    const engineOptionsA = engines.map(e => 
        `<option value="${e}" ${e === engineA ? 'selected' : ''}>${formatEngineName(e)}</option>`
    ).join('');
    const engineOptionsB = engines.map(e => 
        `<option value="${e}" ${e === engineB ? 'selected' : ''}>${formatEngineName(e)}</option>`
    ).join('');

    const html = `
        <h2>Compare Engines</h2>
        <div class="controls">
            <div class="control-group">
                <label for="engine-a-select">Engine A:</label>
                <select id="engine-a-select">${engineOptionsA}</select>
            </div>
            <div class="control-group">
                <label for="engine-b-select">Engine B:</label>
                <select id="engine-b-select">${engineOptionsB}</select>
            </div>
            <div class="control-group">
                <button id="compare-btn">Compare</button>
            </div>
        </div>
        <div id="comparison-results">
            <p class="text-center">Loading comparison...</p>
        </div>
    `;

    container.innerHTML = html;

    // Add event listeners
    document.getElementById('compare-btn').addEventListener('click', () => {
        const newEngineA = document.getElementById('engine-a-select').value;
        const newEngineB = document.getElementById('engine-b-select').value;
        window.location.hash = `#/compare?a=${newEngineA}&b=${newEngineB}`;
    });

    // Load and compare data
    await loadAndCompareEngines(engineA, engineB, benchmarks);
}

/**
 * Load data and generate comparison table
 */
async function loadAndCompareEngines(engineA, engineB, benchmarks) {
    const resultsContainer = document.getElementById('comparison-results');
    resultsContainer.innerHTML = '<p class="text-center">Loading comparison...</p>';

    const comparisons = [];

    // Load latest data for each benchmark
    for (const benchmarkName of benchmarks) {
        const dataA = await loadBenchmarkData(benchmarkName, engineA);
        const dataB = await loadBenchmarkData(benchmarkName, engineB);

        if (dataA.length === 0 || dataB.length === 0) {
            continue; // Skip if no data available
        }

        // Get most recent entry for each engine
        const latestA = dataA[dataA.length - 1];
        const latestB = dataB[dataB.length - 1];

        // Extract key metrics
        const metrics = ['avg_response_time_ms', 'p95', 'p99', 'requests_per_sec'];
        
        for (const metric of metrics) {
            const valueA = extractMetricValue(latestA, metric);
            const valueB = extractMetricValue(latestB, metric);

            if (valueA === null || valueB === null) continue;

            // Calculate difference
            const diff = valueB - valueA;
            const percentChange = valueA !== 0 ? (diff / valueA) * 100 : 0;

            // Determine if higher is better (only for RPS)
            const higherIsBetter = metric === 'requests_per_sec';
            const isBetter = higherIsBetter 
                ? percentChange > 0  // For RPS, higher is better
                : percentChange < 0; // For latency, lower is better

            comparisons.push({
                benchmark: benchmarkName,
                metric: formatMetricName(metric),
                valueA,
                valueB,
                diff,
                percentChange,
                isBetter,
                isWorse: !isBetter && percentChange !== 0
            });
        }
    }

    // Sort by absolute percent change (largest differences first)
    comparisons.sort((a, b) => Math.abs(b.percentChange) - Math.abs(a.percentChange));

    // Calculate summary stats
    const improvements = comparisons.filter(c => c.isBetter).length;
    const regressions = comparisons.filter(c => c.isWorse).length;
    const neutral = comparisons.filter(c => !c.isBetter && !c.isWorse).length;

    // Render summary
    const summaryHtml = `
        <div class="compare-summary">
            <div class="compare-summary-item">
                <span class="label">Total Comparisons</span>
                <span class="value">${comparisons.length}</span>
            </div>
            <div class="compare-summary-item">
                <span class="label">${formatEngineName(engineB)} Better</span>
                <span class="value positive">${improvements}</span>
            </div>
            <div class="compare-summary-item">
                <span class="label">${formatEngineName(engineA)} Better</span>
                <span class="value negative">${regressions}</span>
            </div>
            <div class="compare-summary-item">
                <span class="label">Similar</span>
                <span class="value neutral">${neutral}</span>
            </div>
        </div>
    `;

    // Render table
    const tableRows = comparisons.map(c => {
        const statusClass = c.isBetter ? 'positive' : (c.isWorse ? 'negative' : 'neutral');
        const statusSymbol = c.isBetter ? '✅' : (c.isWorse ? '❌' : '—');
        
        return `
            <tr>
                <td>${c.benchmark}</td>
                <td>${c.metric}</td>
                <td class="numeric">${formatNumber(c.valueA)}</td>
                <td class="numeric">${formatNumber(c.valueB)}</td>
                <td class="numeric ${statusClass}">${formatPercentChange(c.percentChange)}</td>
                <td class="text-center">${statusSymbol}</td>
            </tr>
        `;
    }).join('');

    const tableHtml = `
        <table class="compare-table">
            <thead>
                <tr>
                    <th>Benchmark</th>
                    <th>Metric</th>
                    <th class="numeric">${formatEngineName(engineA)}</th>
                    <th class="numeric">${formatEngineName(engineB)}</th>
                    <th class="numeric">Δ (%)</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                ${tableRows}
            </tbody>
        </table>
    `;

    resultsContainer.innerHTML = summaryHtml + tableHtml;
}

/**
 * Format a number for display
 * @param {number} value - Number to format
 * @returns {string} - Formatted number
 */
function formatNumber(value) {
    if (value === null || value === undefined) return 'N/A';
    if (value >= 1000) {
        return value.toFixed(0);
    } else if (value >= 10) {
        return value.toFixed(1);
    } else {
        return value.toFixed(2);
    }
}

/**
 * Format percent change for display
 * @param {number} percent - Percent change
 * @returns {string} - Formatted string with sign
 */
function formatPercentChange(percent) {
    const sign = percent > 0 ? '+' : '';
    return `${sign}${percent.toFixed(1)}%`;
}
