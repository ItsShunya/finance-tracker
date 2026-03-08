# ---------- builder stage ----------
FROM python:3.13-slim AS builder

WORKDIR /build

# Install git (needed for fava-dashboards)
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip

# Install dependencies
RUN pip install --no-cache-dir \
    beancount>=3.1.0 \
    beancount-reds-plugins>=0.3.0 \
    beangulp>=0.2.0 \
    bs4>=0.0.2 \
    csb43>=1.0.0 \
    ofxparse>=0.21 \
    ofxtools>=0.9.5 \
    petl>=1.7.16 \
    smart-importer>=1.1 \
    titlecase>=2.4.1 \
    fava>=1.30.3 \
    git+https://github.com/andreasgerstmayr/fava-dashboards


# ---------- runtime stage ----------
FROM python:3.13-slim

# Copy venv from builder
COPY --from=builder /venv /venv

ENV PATH="/venv/bin:$PATH"

WORKDIR /data

# Default port for Fava
EXPOSE 5000

# Run Fava
CMD ["fava", "main.bean", "--host", "0.0.0.0", "--port", "5000"]
