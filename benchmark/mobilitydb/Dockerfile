FROM postgres:17.5-bookworm

# Install required PostgreSQL extensions
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        locales \
        tzdata && \
    curl https://install.citusdata.com/community/deb.sh | bash && \
    apt-get install -y --no-install-recommends \
        postgresql-17-postgis-3 \
        postgresql-17-citus-13.0 \
        postgresql-17-mobilitydb; \
    rm -rf /var/lib/apt/lists/*

# Create directory for runtime init scripts
COPY init-db.d/ /docker-entrypoint-initdb.d/

# Healthcheck to verify PostgreSQL is up and responding
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
  CMD pg_isready -U postgres -d postgres || exit 1

# Expose the default PostgreSQL port
EXPOSE 5432

# Start PostgreSQL in foreground, logging to stdout
CMD ["postgres","-c", "shared_preload_libraries=citus"]
