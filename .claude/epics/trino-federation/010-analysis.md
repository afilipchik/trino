---
issue: 010
title: Kind demo — central + regional clusters
analyzed: 2026-08-20T05:55:55Z
estimated_hours: 5
parallelization_factor: 3.0
---

# Parallel Work Analysis: Task 010

## Overview
Multi-region kind demo reusing the kubernetes-connector epic's assets
(plugin/trino-kubernetes/kind-deploy/*, scripts/setup-kind-trino-superset.sh). Runs in an
ISOLATED WORKTREE; owns docker/kind and .m2 installs this wave; orchestrator merges back.

## Parallel Streams

### Stream A: Demo assets + verified run
**Scope**: `plugin/trino-federation/kind-deploy/**` (new), `scripts/**` (new federation script),
README additions
**Can Start**: immediately
**Estimated Hours**: 5
**Dependencies**: task 007

## Coordination Points
Sole owner of docker/kind and of `.m2` installs (`plugin/trino-federation` artifact) this wave.
Must not delete or recreate existing kind clusters/images from the kubernetes demo.
