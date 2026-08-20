---
issue: 004
title: Metadata, handles, and _region column
analyzed: 2026-08-20T04:18:45Z
estimated_hours: 6
parallelization_factor: 1.0
---

# Parallel Work Analysis: Task 004

## Overview
`FederationMetadata` listing + handle model. Single stream (touches the shared FederationMetadata
stub and module wiring); no concurrent agents this wave, so the agent commits its own work.

## Parallel Streams

### Stream A: Metadata + handles
**Scope**: `plugin/trino-federation/src/**` (metadata, handles, cache), module pom test deps if
needed
**Can Start**: immediately
**Estimated Hours**: 6
**Dependencies**: task 002 (RegionClient listing helpers), committed as e75b24aed

## Coordination Points
None — sole agent in the tree this wave.
