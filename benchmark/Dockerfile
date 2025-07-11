FROM alpine:3.21.3

# Set environment variables
ENV POSTGRES_VERSION=15 \
    POSTGIS_VERSION=3.4.0

# Install dependencies
RUN apk add --no-cache \
    bash \
    su-exec \
    ca-certificates \
    openssl \
    libintl \
    tzdata \
    postgresql17=17.4-r0 \
    postgresql17-contrib \
    postgresql17-postgis \
    postgresql17-postgis-utils \
    && mkdir -p /var/lib/postgresql/data \
    && chown -R postgres:postgres /var/lib/postgresql

# Set the default user
USER postgres

# Expose PostgreSQL port
EXPOSE 5432

# Default entrypoint
ENTRYPOINT ["postgres"]

# Default command
CMD ["-D", "/var/lib/postgresql/data"]
