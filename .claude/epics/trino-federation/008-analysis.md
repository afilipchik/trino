---
issue: 008
title: Multi-cluster integration test suite
analyzed: 2026-08-20T05:55:55Z
estimated_hours: 5
parallelization_factor: 3.0
---

# Parallel Work Analysis: Task 008

## Overview
Much of the planned suite already exists (TestFederationScan 8, TestFederationPushdown 18+,
TestFederationAggregationPushdown 20, harness with regional query-log evidence). Remaining scope:
gap-filling (multi-region failure modes, control-run correctness sweep, flake check) and suite
consolidation. Runs in the MAIN checkout in parallel with 009/010 (worktrees).

## Parallel Streams

### Stream A: Test gap-fill + stabilization
**Scope**: `plugin/trino-federation/src/test/**` only (no main-code changes without noting them)
**Can Start**: immediately
**Estimated Hours**: 5
**Dependencies**: task 007 (commit 0ecac7e75)

## Coordination Points
Only agent committing to `epic/trino-federation` this wave. Uses `test` goal (no `install`) —
.m2 writes reserved for task 010's server build.
