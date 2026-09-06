# Conductor agent guidance

## Container builds

- Never use Google Cloud Build or `gcloud builds` for image builds or publishing.
- Use an existing local or cluster-native BuildKit/buildx path. If none is available, report the blocker instead of switching build services.

## Dynamic functions

When work defines, registers, deploys, or tests a Kavun or WebAssembly function,
use `$conductor-functions` from `.agents/skills/conductor-functions/SKILL.md`.

Keep handler versions immutable. Create a new version when executable behavior,
schemas, limits, or the WASM artifact digest changes. Do not put WASM bytes in
Rhiza or expose connector credentials to guest code.
