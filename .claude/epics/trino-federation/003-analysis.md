---
issue: 003
title: RemoteSqlBuilder — regional SQL generation
analyzed: 2026-08-20T03:41:33Z
estimated_hours: 6
parallelization_factor: 1.0
---

# Parallel Work Analysis: Task 003

## Overview
Pure SQL-text generation (identifiers, literals, TupleDomain→WHERE, LIMIT/TopN, aggregate SELECT
lists) with golden-string unit tests. No SPI or network code. Runs in parallel with task 002.

## Parallel Streams

### Stream A: RemoteSqlBuilder + tests
**Scope**: new files under `plugin/trino-federation/src/main/java/io/trino/plugin/federation/sql/`
and matching tests
**Can Start**: immediately (after 001)
**Estimated Hours**: 6
**Dependencies**: task 001

## Coordination Points
Must not edit the module pom (002 owns it this wave) — existing deps (trino-spi, guava) suffice.
Column descriptor types it consumes are defined locally in the sql package this wave; task 004/005
wire them to real column handles.

## Parallelization Strategy
One agent; concurrent with task 002 agent; orchestrator commits after both finish.
