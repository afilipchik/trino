---
issue: 005
title: Scan path — splits and streaming page source
analyzed: 2026-08-20T04:36:52Z
estimated_hours: 8
parallelization_factor: 1.0
---

# Parallel Work Analysis: Task 005

## Overview
Split manager + per-region streaming page source; first end-to-end SELECT through the connector.
Single stream; sole agent in the tree, commits its own work. The multi-region test harness built
here seeds task 008's suite.

## Parallel Streams

### Stream A: Scan path
**Scope**: `plugin/trino-federation/src/**` (split, split manager, page source provider, tests)
**Can Start**: immediately
**Estimated Hours**: 8
**Dependencies**: tasks 003 (RemoteSqlBuilder, commit e75b24aed) and 004 (handles, 0445d4190)
