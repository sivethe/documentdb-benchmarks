FROM python:3.12-slim-bookworm

LABEL maintainer="documentdb-benchmarks"
LABEL description="Benchmark runner for MongoDB-compatible databases"

# Install minimal OS deps
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (layer cache).
# Copy only pyproject.toml so this layer is invalidated only when
# dependencies change, not when application code changes.
COPY pyproject.toml .
RUN pip install --no-cache-dir \
    $(python -c "import tomllib,pathlib; d=tomllib.loads(pathlib.Path('pyproject.toml').read_text()); print(' '.join(d['project']['dependencies']))")

# Copy application code
COPY benchmark_runner/ benchmark_runner/
COPY benchmark_analyzer/ benchmark_analyzer/
COPY config/ config/

# Install the package (uses already-cached deps)
RUN pip install --no-cache-dir --no-deps -e .

# Results volume mount point
RUN mkdir -p /app/results
VOLUME /app/results

ENTRYPOINT ["tini", "--"]
CMD ["bench-run", "--help"]
