FROM rust@sha256:462a9af3c54fb4718850d3c602fc0e54452c20b1c12a4e4080fdb001d4b9acbf

ARG CARGO_DENY_VERSION=0.20.2
RUN cargo install --locked --version "${CARGO_DENY_VERSION}" cargo-deny

WORKDIR /workspace
ENTRYPOINT ["cargo", "deny"]
CMD ["check"]
