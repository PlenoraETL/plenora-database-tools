FROM rust@sha256:f58923369ba295ae1f60bc49d03f2c955a5c93a0b7d49acfb2b2a65bebaf350d

ARG CARGO_DENY_VERSION=0.20.2
RUN cargo install --locked --version "${CARGO_DENY_VERSION}" cargo-deny

WORKDIR /workspace
ENTRYPOINT ["cargo", "deny"]
CMD ["check"]
