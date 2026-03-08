FROM python:3.12-bookworm

# Install common dev tools
RUN apt-get update \
    && apt-get install -y --no-install-recommends git gnupg curl \
    && curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | gpg --dearmor -o /usr/share/keyrings/mongodb-archive-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/mongodb-archive-keyring.gpg] https://repo.mongodb.org/apt/debian bookworm/mongodb-org/7.0 main" \
       > /etc/apt/sources.list.d/mongodb-org-7.0.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends mongodb-mongosh \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Pre-install Python deps so rebuilds are faster (layer cache).
# Copy only pyproject.toml first — this layer is cached until
# dependencies actually change, avoiding slow pip installs on
# every rebuild.
COPY pyproject.toml /tmp/pyproject.toml
RUN pip install --no-cache-dir \
    $(python -c "import tomllib,pathlib; d=tomllib.loads(pathlib.Path('/tmp/pyproject.toml').read_text()); print(' '.join(d['project']['dependencies'] + d['project'].get('optional-dependencies',{}).get('dev',[])))")
