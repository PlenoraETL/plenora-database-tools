FROM rust@sha256:271849e998ffce5776454bbf98c5dc21baafc854ff8e566197908d3aca9a81e8 AS rust-toolchain

FROM icr.io/db2_community/db2@sha256:2de8151713c261843868c5c3411b57be6ae79d99d70a5b3022337836776bfda6

# Il runner usa l'installazione IBM completa da cui proviene `libdb2o`:
# quel driver espone SQLLEN a 64 bit al driver manager. `libdb2`, destinato
# al percorso CLI/SQLLEN a 32 bit, corrompe gli indicatori di lunghezza di
# unixODBC su x86_64. Le due immagini sorgente sono entrambe fissate per
# digest e nessuna libreria viene incorporata nell'artefatto Rust.
RUN dnf install --assumeyes \
        unixODBC \
        unixODBC-devel \
        python3.12 \
        python3.12-pip \
    && dnf clean all

COPY --from=rust-toolchain /usr/local/cargo /usr/local/cargo
COPY --from=rust-toolchain /usr/local/rustup /usr/local/rustup
COPY docker/db2-client/odbcinst.ini /etc/odbcinst.ini
COPY docker/db2-client/entrypoint.sh /usr/local/bin/plenora-db2-entrypoint
COPY docker/db2-client/healthcheck.sh /usr/local/bin/plenora-db2-healthcheck
COPY docker/db2-client/init-fixture.sh /var/custom/10-plenora-fixture.sh
COPY docker/db2-client/fixture.sql /opt/plenora-fixture/fixture.sql
COPY docker/db2-client/drop-fixture.sql /opt/plenora-fixture/drop-fixture.sql
COPY . /workspace

ENV CARGO_HOME=/usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup
ENV PATH=/usr/local/cargo/bin:${PATH}
ENV DB2DIR=/opt/ibm/db2/V12.1
ENV IBM_DB_HOME=/opt/ibm/db2/V12.1
ENV LD_LIBRARY_PATH=/opt/ibm/db2/V12.1/lib64

WORKDIR /workspace

RUN chmod 0755 \
        /usr/local/bin/plenora-db2-entrypoint \
        /usr/local/bin/plenora-db2-healthcheck \
        /var/custom/10-plenora-fixture.sh \
    && cargo test --locked --package plenora-db-db2 --no-run \
    && cargo build --locked --package plenora-database-cli --no-default-features --features db2 \
    && python3.12 -m pip install --no-cache-dir \
        --requirement requirements-sdk-build.txt \
        --requirement requirements-sdk-tests.txt \
    && cd crates/plenora-database-py \
    && python3.12 -m maturin build \
        --release \
        --locked \
        --features db2 \
        --auditwheel skip \
        --out /opt/plenora-wheel \
    && python3.12 -m pip install --no-cache-dir --no-deps \
        /opt/plenora-wheel/*.whl

ENTRYPOINT ["/usr/local/bin/plenora-db2-entrypoint"]
