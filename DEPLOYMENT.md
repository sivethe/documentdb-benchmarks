# GitHub Pages Deployment Guide

This guide explains how the DocumentDB Benchmarks website is deployed to GitHub Pages.

## Overview

The website consists of static HTML/CSS/JavaScript files that read benchmark data from JSON files. The CI pipeline automatically commits benchmark results to the `gh-pages` branch, and GitHub Pages serves the static site.

## Branch Structure

The `gh-pages` branch is already set up and contains:
- Website files (index.html, css/, js/)
- Data directory (data/) with benchmark results
- Documentation (README.md, this file)

## GitHub Pages Setup

### Enable GitHub Pages in Repository Settings

1. Go to your repository on GitHub
2. Navigate to **Settings** → **Pages**
3. Under **Source**, select:
   - Branch: `gh-pages`
   - Folder: `/ (root)`
4. Click **Save**
5. GitHub will provide your site URL: `https://<username>.github.io/<repository>/`

### Verify Deployment

After enabling GitHub Pages, the site should be live within a few minutes. Check:
- The Actions tab shows successful deployments
- The URL provided in Settings → Pages loads the website
- All pages (graphs, compare, dashboard) work correctly

## Directory Structure on `gh-pages` Branch

```
/
├── index.html              # Main entry point
├── css/
│   └── style.css          # Styling
├── js/
│   ├── app.js             # Router and data loading
│   ├── charts.js          # Chart rendering
│   ├── compare.js         # Comparison logic
│   └── dashboard.js       # Dashboard logic
└── data/                  # Benchmark data (CI-generated)
    ├── index.json         # Catalog
    └── <benchmark>/
        ├── atlas.json
        ├── azure_documentdb.json
        └── ...
```

## CI Pipeline Integration

### Script: `deploy/publish_results.py`

The CI pipeline runs `publish_results.py` after each benchmark run to:
1. Load benchmark results from the `results/` directory
2. Convert to the website JSON format
3. Append to existing data files on the `gh-pages` branch
4. Update `data/index.json`
5. Commit and push changes

### GitHub Actions Workflow

The GitHub Actions workflow (see `plan-githubActionsPipeline.prompt.md`) includes:
1. **Run benchmarks** — Execute all benchmarks against all engines
2. **Publish results** — Call `publish_results.py` to update the `gh-pages` branch
3. **Automatic deployment** — GitHub Pages rebuilds on every push

## Manual Data Update

To manually add benchmark results to the website:

### 1. Checkout the `gh-pages` Branch

```bash
git checkout gh-pages
```

### 2. Add Benchmark Data

Append results to the appropriate JSON file:

```bash
# Example: Add result to insert_no_index/atlas.json
cat >> data/insert_no_index/atlas.json <<'EOF'
{
  "timestamp": "2026-04-26T02:00:00Z",
  "benchmark_name": "insert_no_index",
  "database_engine": "atlas",
  "summary": {
    "avg_response_time_ms": 12.0,
    "requests_per_sec": 85.0,
    "percentiles_ms": { "p50": 9.5, "p95": 30.0, "p99": 58.0 }
  }
}
EOF
```

**Note**: The JSON file is an array, so ensure proper array structure when appending.

### 3. Update the Index

If adding a new benchmark, update `data/index.json`:

```json
{
  "benchmarks": ["insert_no_index", "count_group_sum", "new_benchmark"],
  "engines": ["atlas", "azure_documentdb", "mongodb"],
  "last_updated": "2026-04-26T00:00:00Z"
}
```

### 4. Commit and Push

```bash
git add data/
git commit -m "Add benchmark results for 2026-04-26"
git push origin gh-pages
```

GitHub Pages will automatically rebuild the site within a few minutes.

## Testing Locally

Test the website locally before committing changes:

### 1. Start Local Server

```bash
python3 -m http.server 8000
```

### 2. Open in Browser

Navigate to http://localhost:8000

### 3. Test All Pages

- **Graphs** (`#/graphs`) — Verify charts render for all benchmarks
- **Compare** (`#/compare`) — Test engine comparisons
- **Dashboard** (`#/dashboard`) — Check summary charts
- **Status** (`#/status`) — Verify benchmark list
- **Help** (`#/help`) — Ensure help content displays

### 4. Run Validation Script

```bash
python3 test_website.py
```

This validates:
- All required files exist
- JSON files are valid
- Data structure is correct

## Deployment Checklist

- [x] Website files in `gh-pages` branch root
- [ ] GitHub Pages enabled in repository settings
- [x] `gh-pages` branch exists with website files
- [ ] Website loads at GitHub Pages URL
- [ ] All pages render without errors
- [ ] Charts display correctly
- [ ] Navigation works (hash routing)
- [x] Data files are valid JSON
- [ ] CI pipeline configured to publish results
- [ ] Local testing completed

## Troubleshooting

### Site Not Loading

**Problem**: GitHub Pages URL returns 404

**Solution**:
1. Verify GitHub Pages is enabled in Settings → Pages
2. Ensure `index.html` is in the root of the `gh-pages` branch
3. Check that the branch is set to `gh-pages` in Settings → Pages
4. Wait 5-10 minutes for initial deployment

### Charts Not Rendering

**Problem**: Blank page or charts not displaying

**Solution**:
1. Open browser console (F12) and check for errors
2. Verify `data/index.json` exists and is valid JSON
3. Check that benchmark data files exist in `data/<benchmark>/<engine>.json`
4. Ensure Chart.js CDN is accessible (not blocked by firewall)

### Data Not Updating

**Problem**: New benchmark results not appearing on the website

**Solution**:
1. Verify data files were committed to the `gh-pages` branch
2. Check GitHub Pages deployment status (Settings → Pages)
3. Hard-refresh the browser (Ctrl+F5 or Cmd+Shift+R)
4. Check browser console for 404 errors when loading data files

### CORS Errors

**Problem**: `fetch()` calls failing with CORS errors

**Solution**:
- GitHub Pages automatically serves files with correct headers
- If testing locally, ensure you're using a local server (not `file://` protocol)
- Use `python3 -m http.server` or similar

## Maintenance

### Data Pruning

To prevent the data files from growing too large, the CI pipeline should:
1. Keep only the last 30-60 days of data
2. Archive older data to a separate repository or storage
3. Run pruning as part of the CI pipeline

See `deploy/publish_results.py` for pruning logic.

### Monitoring

Monitor the following:
- **Data freshness** — Ensure CI runs regularly (daily or weekly)
- **File sizes** — Keep data files under 1MB each for fast loading
- **Performance** — Test page load times periodically
- **Browser compatibility** — Test on Chrome, Firefox, Safari, Edge

## Security Considerations

- **No sensitive data** — Never commit connection strings or credentials to the `gh-pages` branch
- **Public repository** — Assume all data is publicly accessible
- **Read-only** — The website only reads data; it never writes or modifies anything

## Support

For issues or questions:
1. Check the [README](README.md) for usage documentation
2. Review the [main plan](../plan-githubBenchmarkPages.prompt.md) for design details
3. Open an issue on GitHub

## Next Steps

After deployment:
1. Add a link to the website in the main repository README
2. Document the website in CONTRIBUTING.md
3. Set up automated CI runs (daily or weekly)
4. Monitor data freshness and site performance
