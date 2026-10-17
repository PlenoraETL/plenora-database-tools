# Public profile: database-tools, version 2

Profile identifier: `plenora-database-tools-profile-v2`

Normative target catalog:
[`database-tools-v2.json`](../catalogs/database-tools-v2.json)

It supersedes [profile version 1](database-tools.md) as the current target
([decision 0015](../decisions/0015-intentional-absences-and-planned-surfaces.md)).
Its catalog lists the operation identities of version 1, unchanged, under
Public Catalogs 2.0 (`plenora-public-catalog-v2`); this profile adds the
intentional absences, the content types on the runtime and the bound of a
runtime result, and states the runtime as conditional under that contract.
Version 1 remains an available identity: its catalog and contracts keep
their meaning.

## Applicable contracts

- [Public Surfaces 1.0](../specs/surfaces/PUBLIC-SURFACES-1.0.md)
- [Capability Discovery 2.0](../specs/capabilities/CAPABILITY-DISCOVERY-2.0.md)
- [Typed Errors 1.0](../specs/errors/ERRORS-1.0.md)
- [Public Security 1.0](../specs/security/PUBLIC-SECURITY-1.0.md)
- [Arrow Interchange 1.0](../specs/data/ARROW-INTERCHANGE-1.0.md)
- [Row Diagnostics 1.0](../specs/diagnostics/ROW-DIAGNOSTICS-1.0.md)
- [CLI 2.0](../specs/cli/CLI-2.0.md)
- [Python SDK 1.0](../specs/sdk/PYTHON-SDK-1.0.md)
- [Public Catalogs 2.0](../specs/catalogs/PUBLIC-CATALOGS-2.0.md), for the
  catalog of this profile
- [Runtime Binding 2.0](../specs/runtime/RUNTIME-BINDING-2.0.md), when exposed
- [Surface Bindings 1.0](../specs/surfaces/SURFACE-BINDINGS-1.0.md)
- [Composition 1.0](../specs/composition/COMPOSITION-1.0.md)

## Public purpose

The component exposes discovery, reading, writing and query-oriented database
functionality without requiring a consumer to understand provider internals.

This profile and its operation catalog are normative targets. That status fixes
the expected public boundary; it does not claim that any component release
conforms. An artifact claims conformance only through immutable component-owned
operation and capability-attribute schemas plus the adoption manifest described
in [`ADOPTION.md`](../ADOPTION.md).

## Required operation families

The stable public catalog includes these baseline identifiers:

- `database.test_connection`;
- `database.list_catalogs`;
- `database.list_schemas`;
- `database.list_objects`;
- `database.describe_object`;
- `database.read`;
- `database.write`.

Public query and transaction entry points MUST also be discoverable under the
`database.query` and `database.transaction.*` families when the released
artifact exposes them.

`database.query` is read-only and has no remote mutation effect. Insert,
update, delete and upsert entry points bind to `database.execute` or
`database.write` and conservatively declare remote side effects.

Session constructors such as `connect` and `aconnect` establish persistent
sessions. They do not implement `database.test_connection`, which has dedicated
sync and async entry points and returns a bounded redacted verification result.

No `arcgis.*` operation is selected by this profile. ArcGIS operations,
bindings and composition edges remain absent until ownership is ratified as
REST operations, a dedicated component or another explicit public extension.

## Component-owned contracts

`database-tools` owns and immutably versions every
`plenora-database-*-v1` input or output contract referenced by the catalog,
including `plenora-database-capability-attributes-v1`. Their normative schemas
and operation specifications live in the component repository; this repository
owns their stable identities and cross-surface semantics.

The capability-attributes contract describes the selected provider and the
write modes actually available for that provider. Consumers MUST interpret
`write_modes` only under that contract. A mode unavailable for a provider is
omitted rather than accepted and failed later; the common catalog does not make
every mode universally available.

## Public surfaces

- Rust API: required.
- CLI: required and governed by CLI 2.0.
- Python SDK: required and governed by Python SDK 1.0.
- Runtime: conditional. An artifact that publishes the runtime surface
  follows Runtime Binding 2.0 for every operation it exposes there; an
  artifact without it omits `runtime` from its capability document and
  records no deviation, and the catalog's runtime entries are planned for it
  (CAT-001, CAT-002).

The same operation version exposed on multiple surfaces has equivalent input
validation, results, error axes and remote-effect semantics.

## Surfaces intentionally absent

These gaps in the catalog are decisions, not missing work. Public Catalogs
2.0 CAT-003 keeps them: none of these surfaces is ever added to these
operation identities.

**DB-ABS-001** — `database.transaction.begin`, `database.transaction.commit`,
`database.transaction.rollback` and `database.transaction.savepoint` are bound
to the Rust API and the Python SDK only.

- Not to the CLI: a transaction handle does not survive the process that
  opened it, and CLI 2.0 answers one invocation with one envelope. Spreading
  a transaction over several invocations needs a session process that holds
  the connection between them, which no shared contract defines.
- Not to the runtime: Runtime Binding 2.0 messages are independent requests
  that a transport may route to different workers. A transaction spread over
  several messages needs session affinity, a lease and the recovery of a
  transaction whose owner disappeared, which no shared contract defines.

A process-level caller that needs atomicity uses an operation whose single
invocation is the transaction, such as `database.write`.

**DB-ABS-002** — `database.execute` is not bound to the runtime. It runs a
statement the caller supplies, with remote effects the component cannot
characterize, and its controls accept no idempotency key. Runtime Binding 2.0
does not promise at-most-once delivery: a transport may deliver a request
again after a lost acknowledgement, and the shared defense against a second
execution is an idempotency key (SURF-012, RT-022), which this operation does
not accept. A runtime binding needs a new operation version that accepts the
key and states how a repeated key is recognized.

**DB-ABS-003** — `plenora-database-transaction-handle-v1` and
`plenora-database-savepoint-input-v1` are logical shapes. The Rust API and the
Python SDK realize them as native handle objects (SURF-009); no surface
serializes them, and the `application/json` content type the catalog declares
names the representation a serialized surface would use, not one that
exists. A consumer MUST NOT expect, persist or exchange them as JSON
documents; a handle is valid only in the process and session that created it.

## The runtime surface

**DB-RT-003** — A query result larger than the bound of the runtime surface
fails with `resource_limit` under RT-024, before any row is returned; it is
never cut to the bound. `database.query` is read-only, so the remote effect
is `none`. database-tools declares its bound for the runtime surface in its
capability document, under its component-owned
`plenora-database-capability-attributes-v1`, and in its adoption manifest
while that contract has no member for it; the bound of the transport is the
transport's (RT-024). The vectors
[`database-query-result-limit-error.json`](../vectors/runtime-v2/database-query-result-limit-error.json)
and
[`database-query-transport-limit-error.json`](../vectors/runtime-v2/database-query-transport-limit-error.json)
illustrate both errors.

`database.query` version 1 and `database.write` version 1 keep on the
runtime the input and output content types their catalog declares, Arrow
included; this profile adds no restriction to them, and the identifiers
DB-RT-001 and DB-RT-002 of the first form of decision 0015 are not used. A
representation chosen by the caller on the runtime, or rows only by
artifact reference, needs a new operation version whose input contract
states it.

## Interchange

`database.read` returns tabular results through the Arrow Interchange 1.0
contract when a tabular surface is selected. `database.write` accepts Arrow
under that contract when its descriptor advertises Arrow input.

Read or write failures that expose row-level evidence use
`plenora-row-diagnostics-v1`.

## External safety

Connection selection is exposed as a reference or protected configuration
boundary. Capability documents, errors and ordinary results MUST NOT contain
credentials, DSNs, bound statements or source rows.

Writes and transaction operations declare remote side effects, supported
execution controls and possible ambiguous outcomes in their public
specification.

## Not specified here

This profile does not prescribe provider traits, drivers, pools, SQL rendering,
transaction implementation or Arrow conversion code.
