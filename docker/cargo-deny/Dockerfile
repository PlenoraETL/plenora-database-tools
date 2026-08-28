FROM rust@sha256:271849e998ffce5776454bbf98c5dc21baafc854ff8e566197908d3aca9a81e8

ARG CARGO_DENY_VERSION=0.20.2
RUN cargo install --locked --version "${CARGO_DENY_VERSION}" cargo-deny

WORKDIR /workspace
ENTRYPOINT ["cargo", "deny"]
CMD ["check"]
