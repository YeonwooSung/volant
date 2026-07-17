# Phase 61 — Flexible configs (Describe/Alter/IncrementalAlter)

## Goals

1. First flexible versions of config APIs:
   - **DescribeConfigs** 0–4 (flexible **v4**)
   - **AlterConfigs** 0–2 (flexible **v2**)
   - **IncrementalAlterConfigs** 0–1 (flexible **v1**)
2. Response header **v1** for those flexible versions
3. Compact strings/arrays + empty TAG_BUFFER
4. Classic paths unchanged
5. Tests + docs honesty

## Non-goals

- Broker / broker-logger resources (TOPIC only)
- Real synonym layering
- APPEND/SUBTRACT on IncrementalAlterConfigs
- Higher versions beyond first flexible

## Wire summary

### DescribeConfigs v4

**Request:** compact resources[{type, name, configuration_keys|null}], include_synonyms, include_documentation, tags.

**Response** (header v1): throttle, compact results[{error, error_message, type, name, configs[{name, value, read_only, source, sensitive, synonyms[], type, documentation, tags}], tags}], tags.

### AlterConfigs v2

**Request:** compact resources[{type, name, configs[{name, value, tags}], tags}], validate_only, tags.

**Response** (header v1): throttle, compact responses[{error, error_message, type, name, tags}], tags.

### IncrementalAlterConfigs v1

**Request:** compact resources[{type, name, configs[{name, op, value, tags}], tags}], validate_only, tags.

**Response:** same shape as AlterConfigs v2 flexible.

## Exit criteria

1. ApiVersions maxes: Describe **4**, Alter **2**, Incremental **1**
2. Alter v2 + Describe v4 roundtrip with config_source + docs
3. Incremental v1 SET succeeds
4. Classic Alter v1 still works
5. Unsupported higher versions → header v1 + UnsupportedVersion
6. phase61 + phase46 + phase37 green

## Honest limitations

- Empty synonyms only
- TOPIC resources only
- No APPEND/SUBTRACT
- Empty tag buffers only
