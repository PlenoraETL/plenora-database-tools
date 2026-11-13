FROM rust@sha256:462a9af3c54fb4718850d3c602fc0e54452c20b1c12a4e4080fdb001d4b9acbf AS rust-toolchain

FROM icr.io/db2_community/db2@sha256:2de8151713c261843868c5c3411b57be6ae79d99d70a5b3022337836776bfda6

# Il runner usa l'installazione IBM completa da cui proviene `libdb2o`:
# quel driver espone SQLLEN a 64 bit al driver manager. `libdb2`, destinato
# al percorso CLI/SQLLEN a 32 bit, corrompe gli indicatori di lunghezza di
# unixODBC su x86_64. Le due immagini sorgente sono entrambe fissate per
# digest e nessuna libreria viene incorporata nell'artefatto Rust. L'immagine
# IBM abilita anche il repository CentOS Stream CRB, ma questi pacchetti
# provengono dai repository UBI: lo disabilitiamo per non rendere il gate
# dipendente da un mirror esterno non necessario.
RUN dnf --disablerepo=CRB install --assumeyes \
        unixODBC \
        unixODBC-devel \
        gcc make curl tar xz openssl-devel zlib-devel bzip2-devel \
        libffi-devel sqlite-devel xz-devel readline-devel libuuid-devel \
    && dnf clean all

COPY scripts/install_python.sh /tmp/install_python.sh
RUN sh /tmp/install_python.sh /opt/plenora-python
ENV PATH=/opt/plenora-python/bin:${PATH}
ENV LD_LIBRARY_PATH=/opt/plenora-python/lib

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
ENV LD_LIBRARY_PATH=/opt/ibm/db2/V12.1/lib64:/opt/plenora-python/lib

WORKDIR /workspace

RUN chmod 0755 \
        /usr/local/bin/plenora-db2-entrypoint \
        /usr/local/bin/plenora-db2-healthcheck \
        /var/custom/10-plenora-fixture.sh \
    && cargo test --locked --package plenora-db-db2 --no-run \
    && cargo build --locked --package plenora-database-cli --no-default-features --features db2 \
    && python3.14 -m pip install --no-cache-dir \
        --requirement requirements-sdk-build.txt \
        --requirement requirements-sdk-tests.txt \
    && cd crates/plenora-database-py \
    && python3.14 -m maturin build \
        --release \
        --locked \
        --features db2 \
        --auditwheel skip \
        --out /opt/plenora-wheel \
    && python3.14 /workspace/scripts/licenze_artefatto.py --profile wheel-db2-linux /opt/plenora-wheel/*.whl \
    && python3.14 /workspace/scripts/check-licenze-artefatto.py --profile wheel-db2-linux /opt/plenora-wheel/*.whl \
    && python3.14 /workspace/scripts/name_db2_wheel.py /opt/plenora-wheel \
    && python3.14 -m pip install --no-cache-dir --no-deps \
        /opt/plenora-wheel/*-1db2-cp314-abi3-linux_x86_64.whl

ENTRYPOINT ["/usr/local/bin/plenora-db2-entrypoint"]
