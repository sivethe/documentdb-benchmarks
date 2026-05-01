/**
 * Charts module - Chart.js wrapper functions for rendering benchmark visualizations
 */

// Engine colors for consistent visualization
const ENGINE_COLORS = {
    'atlas': '#4CAF50',
    'azure_documentdb': '#2196F3',
    'aws_documentdb': '#FF9800',
    'mongodb': '#00ED64'
};

// Default chart options
const DEFAULT_CHART_OPTIONS = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: {
        mode: 'index',
        intersect: false,
    },
    plugins: {
        legend: {
            position: 'top',
        },
        tooltip: {
            enabled: true,
        }
    },
    scales: {
        x: {
            type: 'time',
            time: {
                unit: 'day',
                displayFormats: {
                    day: 'MMM d'
                }
            },
            title: {
                display: true,
                text: 'Date'
            }
        },
        y: {
            beginAtZero: true,
        }
    }
};

/**
 * Create a time-series line chart comparing multiple engines
 * @param {HTMLCanvasElement} canvas - Canvas element to render chart on
 * @param {Object} data - Chart data with structure: { engine_name: [{timestamp, value}, ...] }
 * @param {string} metric - Metric name (for Y-axis label)
 * @param {string} title - Chart title
 */
function createTimeSeriesChart(canvas, data, metric, title) {
    const datasets = Object.keys(data).map(engineName => {
        const color = ENGINE_COLORS[engineName] || '#999';
        return {
            label: formatEngineName(engineName),
            data: data[engineName].map(point => ({
                x: new Date(point.timestamp),
                y: point.value
            })),
            borderColor: color,
            backgroundColor: color + '33', // Add transparency
            tension: 0.1,
            fill: false,
            pointRadius: 3,
            pointHoverRadius: 5
        };
    });

    const options = {
        ...DEFAULT_CHART_OPTIONS,
        plugins: {
            ...DEFAULT_CHART_OPTIONS.plugins,
            title: {
                display: true,
                text: title,
                font: {
                    size: 14,
                    weight: 'bold'
                }
            }
        },
        scales: {
            ...DEFAULT_CHART_OPTIONS.scales,
            y: {
                ...DEFAULT_CHART_OPTIONS.scales.y,
                title: {
                    display: true,
                    text: metric
                }
            }
        }
    };

    return new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: { datasets },
        options
    });
}

/**
 * Create a summary chart (aggregate across multiple benchmarks)
 * @param {HTMLCanvasElement} canvas - Canvas element to render chart on
 * @param {Object} data - Chart data with structure: { engine_name: [{timestamp, value}, ...] }
 * @param {string} metric - Metric name
 * @param {string} title - Chart title
 */
function createSummaryChart(canvas, data, metric, title) {
    return createTimeSeriesChart(canvas, data, metric, title);
}

/**
 * Format engine name for display
 * @param {string} engineName - Raw engine name (e.g., 'azure_documentdb')
 * @returns {string} - Formatted name (e.g., 'Azure DocumentDB')
 */
function formatEngineName(engineName) {
    const names = {
        'atlas': 'Atlas',
        'azure_documentdb': 'Azure DocumentDB',
        'aws_documentdb': 'AWS DocumentDB',
        'mongodb': 'MongoDB'
    };
    return names[engineName] || engineName;
}

/**
 * Format metric name for display
 * @param {string} metric - Raw metric name (e.g., 'avg_response_time_ms')
 * @returns {string} - Formatted name (e.g., 'Avg Response Time (ms)')
 */
function formatMetricName(metric) {
    const names = {
        'avg_response_time_ms': 'Avg Response Time (ms)',
        'min_response_time_ms': 'Min Response Time (ms)',
        'max_response_time_ms': 'Max Response Time (ms)',
        'p50': 'P50 Latency (ms)',
        'p75': 'P75 Latency (ms)',
        'p90': 'P90 Latency (ms)',
        'p95': 'P95 Latency (ms)',
        'p99': 'P99 Latency (ms)',
        'requests_per_sec': 'Requests per Second',
        'total_requests': 'Total Requests',
        'total_failures': 'Total Failures',
        'failure_rate': 'Failure Rate (%)'
    };
    return names[metric] || metric;
}

/**
 * Extract metric value from a result entry
 * @param {Object} entry - Result entry from JSON data (either old format with summary or operation object)
 * @param {string} metric - Metric name to extract
 * @returns {number|null} - Metric value or null if not found
 */
function extractMetricValue(entry, metric) {
    if (!entry) return null;

    // New format: operation object with response_time_ms
    if (entry.response_time_ms) {
        const rtms = entry.response_time_ms;
        
        // Direct metric mappings for new format
        if (metric === 'avg_response_time_ms' && rtms.avg !== undefined) {
            return rtms.avg;
        }
        if (metric === 'min_response_time_ms' && rtms.min !== undefined) {
            return rtms.min;
        }
        if (metric === 'max_response_time_ms' && rtms.max !== undefined) {
            return rtms.max;
        }
        if (metric === 'requests_per_sec' && entry.requests_per_sec !== undefined) {
            return entry.requests_per_sec;
        }
        
        // Percentiles
        if (rtms[metric] !== undefined) {
            return rtms[metric];
        }
        
        // Calculate failure rate
        if (metric === 'failure_rate' && entry.num_requests > 0) {
            return (entry.num_failures / entry.num_requests) * 100;
        }
    }

    // Old format: summary object (for backward compatibility)
    if (entry.summary) {
        // Check if metric is in summary directly
        if (entry.summary[metric] !== undefined) {
            return entry.summary[metric];
        }

        // Check if metric is in percentiles
        if (entry.summary.percentiles_ms && entry.summary.percentiles_ms[metric] !== undefined) {
            return entry.summary.percentiles_ms[metric];
        }

        // Calculate failure rate if requested
        if (metric === 'failure_rate' && entry.summary.total_requests > 0) {
            return (entry.summary.total_failures / entry.summary.total_requests) * 100;
        }
    }

    return null;
}

/**
 * Filter data by date range
 * @param {Array} data - Array of result entries
 * @param {Date} startDate - Start date (inclusive)
 * @param {Date} endDate - End date (inclusive)
 * @returns {Array} - Filtered array
 */
function filterByDateRange(data, startDate, endDate) {
    if (!startDate && !endDate) return data;
    
    return data.filter(entry => {
        const entryDate = new Date(entry.timestamp);
        if (startDate && entryDate < startDate) return false;
        if (endDate && entryDate > endDate) return false;
        return true;
    });
}

/**
 * Get the default date range (last 15 days)
 * @returns {Object} - {startDate, endDate}
 */
function getDefaultDateRange() {
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - 15);
    return { startDate, endDate };
}

/**
 * Format date for input[type="date"]
 * @param {Date} date - Date to format
 * @returns {string} - Formatted date (YYYY-MM-DD)
 */
function formatDateForInput(date) {
    return date.toISOString().split('T')[0];
}

/**
 * Destroy chart if it exists
 * @param {Chart} chart - Chart.js instance
 */
function destroyChart(chart) {
    if (chart && typeof chart.destroy === 'function') {
        chart.destroy();
    }
}
