FROM eclipse-temurin:25-jdk-noble
RUN set -eux; \
    useradd -r -m -d /home/trino -s /bin/bash trino; \
    mkdir -p /usr/lib/trino /data/trino /etc/trino; \
    chown -R trino:trino /usr/lib/trino /data/trino /etc/trino
COPY --chown=trino:trino trino-server /usr/lib/trino
COPY --chown=trino:trino plugin-memory /usr/lib/trino/plugin/memory
COPY --chown=trino:trino plugin-tpch /usr/lib/trino/plugin/tpch
COPY --chown=trino:trino plugin-federation /usr/lib/trino/plugin/trino-federation
COPY --chown=trino:trino etc /etc/trino
EXPOSE 8080
USER trino
CMD ["/usr/lib/trino/bin/launcher", "run", "--etc-dir", "/etc/trino"]
