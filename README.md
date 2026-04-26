# DocumentDB Benchmarks - GitHub Pages Website

This is the GitHub Pages website for visualizing DocumentDB benchmark results.

## Overview

The website provides an interactive dashboard for comparing performance metrics across multiple database engines:
- **Atlas** — MongoDB Atlas (fully managed MongoDB)
- **Azure DocumentDB** — Azure Cosmos DB with MongoDB API
- **AWS DocumentDB** — Amazon DocumentDB (MongoDB-compatible)
- **MongoDB** — Self-hosted MongoDB Community Server

## Architecture

### Tech Stack
- **Static HTML + Vanilla JavaScript** — No build step required
- **Chart.js** — Time-series visualization library
- **Hash-based routing** — Client-side navigation (`#/graphs`, `#/compare`, etc.)
- **GitHub Pages** — Zero-infrastructure hosting

### File Structure

```
/
├── index.html              # Main entry point (SPA shell)
├── css/
│   └── style.css          # Minimal styling (perf.rust-lang.org inspired)
├── js/
│   ├── app.js             # Router, data loading, page coordination
│   ├── charts.js          # Chart.js wrapper functions
│   ├── compare.js         # Compare page logic
│   └── dashboard.js       # Dashboard page logic
└── data/                  # Benchmark data (populated by CI)
    ├── index.json         # Catalog of benchmarks and engines
    └── <benchmark_name>/
        ├── atlas.json
        ├── azure_documentdb.json
        ├── mongodb.json
        └── ...
```

## Pages

### 1. Graphs (`#/graphs`)
- **Default landing page**
- Grid of time-series charts, one per benchmark
- Multiple engines overlaid on each chart
- Controls: date range picker, metric selector, benchmark filter
- URL parameters: `?benchmark=<name>&metric=<metric>`

### 2. Compare (`#/compare`)
- Side-by-side engine comparison
- Sortable table showing latest metrics for each benchmark
- Color-coded improvements/regressions (✅ ❌)
- Summary stats: total comparisons, improvements, regressions
- URL parameters: `?a=<engine>&b=<engine>`

### 3. Dashboard (`#/dashboard`)
- High-level summary charts
- Average latency by benchmark category (insert, count)
- Average RPS across all benchmarks
- P99 latency trends
- Shows big-picture performance over time

### 4. Status (`#/status`)
- List of available benchmarks and engines
- Data freshness indicators
- Useful for verifying data availability

### 5. Help (`#/help`)
- Metric definitions (avg, p50, p95, p99, RPS)
- Database engine descriptions
- Benchmark methodology
- Link to source repository

## Data Format

### `data/index.json`
Catalog of available benchmarks and engines:
```json
{
  "benchmarks": ["insert_no_index", "count_group_sum_single_path_index", ...],
  "engines": ["atlas", "azure_documentdb", "aws_documentdb", "mongodb"],
  "last_updated": "2026-04-26T00:00:00Z"
}
```

### `data/<benchmark_name>/<engine_name>.json`
Append-only array of result entries (newest last):
```json
[
  {
    "timestamp": "2026-04-15T02:00:00Z",
    "benchmark_name": "insert_no_index",
    "run_label": "insert-no-index",
    "database_engine": "atlas",
    "users": 10,
    "run_time": "60s",
    "workload_params": { "seed_docs": 100000, "document_size": 256 },
    "summary": {
      "total_requests": 5200,
      "total_failures": 0,
      "avg_response_time_ms": 11.5,
      "min_response_time_ms": 2.1,
      "max_response_time_ms": 85.3,
      "requests_per_sec": 86.7,
      "percentiles_ms": { "p50": 9.0, "p75": 13.0, "p90": 20.0, "p95": 28.0, "p99": 55.0 }
    },
    "operations": [
      {
        "name": "insert_one",
        "num_requests": 5200,
        "avg_response_time_ms": 11.5,
        "requests_per_sec": 86.7,
        "percentiles_ms": { "p50": 9.0, "p95": 28.0, "p99": 55.0 }
      }
    ]
  },
  ...
]
```

## Local Development

### Quick Start

Serve the website locally using Python's built-in HTTP server:

```bash
python3 -m http.server 8000
```

Then open: http://localhost:8000

### Using Node.js `http-server`

```bash
npm install -g http-server
http-server -p 8000
```

### Using VS Code Live Server Extension

1. Install the "Live Server" extension
2. Right-click `index.html` → "Open with Live Server"

## Deployment

### GitHub Pages Setup

1. **Create a `gh-pages` branch** in your repository
2. **Copy website files** to the root of the `gh-pages` branch:
   ```bash
   git checkout --orphan gh-pages
   git rm -rf .
   cp -r gh-pages/* .
   git add .
   git commit -m "Initial GitHub Pages site"
   git push origin gh-pages
   ```
3. **Enable GitHub Pages** in repository settings:
   - Settings → Pages → Source: "Deploy from a branch"
   - Branch: `gh-pages` / `root`
4. **CI pipeline** (`plan-githubActionsPipeline.prompt.md`) will commit data files to the `gh-pages` branch

### Data Updates

The CI pipeline (defined in `plan-githubActionsPipeline.prompt.md`) automatically:
1. Runs benchmarks against all engines
2. Generates JSON result files
3. Commits/pushes to the `gh-pages` branch
4. GitHub Pages rebuilds the site automatically

## Design Inspiration

The visual design and page structure are inspired by **perf.rust-lang.org**, the Rust compiler performance tracking site:
- Minimal styling, system fonts
- Dense information layout
- Time-series charts with overlaid lines
- Sortable comparison tables
- Clean, functional navigation

Key difference: perf.rust-lang.org uses a Rust server + PostgreSQL; we use static JSON files + client-side rendering for zero infrastructure.

## Chart Customization

Chart colors are defined in `js/charts.js`:

```javascript
const ENGINE_COLORS = {
    'atlas': '#4CAF50',          // Green
    'azure_documentdb': '#2196F3', // Blue
    'aws_documentdb': '#FF9800',  // Orange
    'mongodb': '#00ED64'          // MongoDB brand green
};
```

To add a new engine color, add an entry to this object.

## Browser Compatibility

- **Modern browsers** (Chrome, Firefox, Safari, Edge)
- Requires JavaScript enabled
- Uses ES6+ features (arrow functions, `async`/`await`, `fetch`)
- No IE11 support

## Performance Considerations

- **Lazy data loading** — Only fetches benchmark data when viewing that benchmark
- **In-memory caching** — Avoids redundant fetches within a session
- **Date range filtering** — Limits data points rendered on charts
- **Chart cleanup** — Destroys old Chart.js instances before creating new ones

## Testing

### Sample Data
The `data/` directory includes sample data for testing:
- `data/index.json` — 3 benchmarks, 3 engines
- `data/insert_no_index/` — 5 days of sample data per engine

### Manual Testing Checklist
- [ ] All pages load without errors
- [ ] Graphs page renders charts for all benchmarks
- [ ] Date range picker filters data correctly
- [ ] Metric selector updates charts
- [ ] Benchmark filter shows/hides charts
- [ ] Compare page displays comparison table
- [ ] Engine selectors update comparison
- [ ] Dashboard shows summary charts
- [ ] Status page lists benchmarks
- [ ] Help page content displays correctly
- [ ] Navigation highlights active page
- [ ] Hash-based routing works (back/forward buttons)

## Troubleshooting

### Charts not rendering
- Check browser console for errors
- Verify `data/index.json` loads successfully
- Verify benchmark JSON files exist and are valid JSON
- Check that Chart.js CDN is accessible

### Data not loading
- Verify files are served with correct MIME types (`.json` → `application/json`)
- Check CORS headers if serving from a different origin
- Look for 404 errors in Network tab

### Navigation not working
- Ensure hash-based URLs (`#/graphs`, `#/compare`, etc.)
- Check browser console for JavaScript errors
- Verify all JS files load successfully

## Future Enhancements

Potential additions (not yet implemented):
- **Click-to-expand** small charts → full-size view
- **Permalink support** — Share URLs to specific chart/date/metric
- **Comparison snapshots** — Save and share comparison results
- **Regression detection** — Highlight significant performance changes
- **Data export** — Download comparison table as CSV
- **Mobile optimization** — Improved responsive layouts

## License

Same license as the main DocumentDB Benchmarks repository.

## Contact

For questions or issues, please open an issue on GitHub or contact the maintainers.
