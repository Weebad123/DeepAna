# COMMONS

COMMONS is the community contribution layer of the platform. Contributions arrive as pull requests to a git repository. COMMONS verifies each one against known-correct data and publishes the approved ones for PRISM and ORACLE to load.

## How it works

- **Contributions are declarative data**: Anchor IDL JSON for decoders, and read-only SQL for data models. Nothing a contributor submits is ever executed as code, so there is no sandbox.
- **A pull request is a submission.** The author is the git author, and `git log` is the attribution.
- **CI runs the verifier** on every pull request: static checks, then tests against golden fixtures with known-correct output, then invariant checks, and a deterministic output digest. A failure blocks the merge.
- **A maintainer's merge is the promotion.** Status (`experimental`, `verified`, `official`) changes only through a reviewed pull request, enforced with `CODEOWNERS` and branch protection.
- **The publish step** exports approved artifacts to a directory and an index file that PRISM and ORACLE load.

## What COMMONS contains

```
commons/src/
├── manifest/    artifact manifests, validation, canonical hashing
├── fixtures/    pinned golden data for verification
├── verify/      the verification pipeline
├── publish/     export for PRISM and ORACLE
├── lib.rs
└── main.rs      CLI: `commons verify <path>`, `commons publish <dir>`
```

## How it fits with the other services

| Service | Relationship to COMMONS |
|---|---|
| PRISM | Loads published decoders. COMMONS reuses PRISM's decoder code as a library for golden-data tests |
| LENS | Optional. A later stage can run SQL models through LENS over HTTP |
| FORGE | No direct link |
| ORACLE | Can load published models and metric definitions |
