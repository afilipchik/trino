---
issue: 006
title: Scan pushdowns — filter, projection, limit, TopN, region pruning
analyzed: 2026-08-20T04:53:11Z
estimated_hours: 6
parallelization_factor: 1.0
---

# Parallel Work Analysis: Task 006

## Overview
The non-aggregate pushdown surface in FederationMetadata (applyFilter/applyProjection/applyLimit/
applyTopN + `_region` pruning). Single stream; sole agent, commits its own work.

## Parallel Streams

### Stream A: Pushdown surface
**Scope**: `plugin/trino-federation/src/**` (FederationMetadata apply* methods, sql-package
pushability helper, tests)
**Can Start**: immediately
**Estimated Hours**: 6
**Dependencies**: task 005 (commit 26722eaba)
