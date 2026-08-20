---
issue: 007
title: Aggregation pushdown with connector-side combine
analyzed: 2026-08-20T05:15:59Z
estimated_hours: 12
parallelization_factor: 1.0
---

# Parallel Work Analysis: Task 007

## Overview
applyAggregation + single fan-out split + AggregateCombiningPageSource. Largest task of the epic;
single stream (deep coupling between metadata rewrite and combine execution). Sole agent, commits
its own work.

## Parallel Streams

### Stream A: Aggregation pushdown end to end
**Scope**: `plugin/trino-federation/src/**`
**Can Start**: immediately
**Estimated Hours**: 12
**Dependencies**: task 006 (commit f7cbe0e85)
