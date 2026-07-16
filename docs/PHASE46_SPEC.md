# Phase 46 — Kafka Describe/AlterConfigs classic version bumps

## Goals

1. Raise classic config APIs:
   - **DescribeConfigs** 0 → **0–3** (flexible 4+)
   - **AlterConfigs** 0 → **0–1** (flexible 2+)
2. Align response framing with Kafka:
   - Leading `throttle_time_ms` on **all** versions (was missing)
   - DescribeConfigs result order: error → error_message → type → name → configs
3. DescribeConfigs v1+ config_source + empty synonyms; v3+ config_type + documentation
4. Advertise in ApiVersions; tests + docs honesty

## Non-goals

- Flexible DescribeConfigs v4+ / AlterConfigs v2+
- IncrementalAlterConfigs bump (flexible starts at v1; stay at classic v0)
- Broker / broker-logger / client-metrics resources (TOPIC only)
- Real synonym layering (broker defaults → topic overrides)

## Wire summary

### DescribeConfigs

| Ver | Additive |
|-----|----------|
| v0+ | response throttle (leading); is_default on entries |
| v1–2 | request include_synonyms; response config_source + synonyms[] |
| v3 | request include_documentation; response config_type + documentation |

ConfigSource: TOPIC_CONFIG=1 when set, DEFAULT_CONFIG=5 when empty/default.
ConfigType: LONG for retention/segment size keys; STRING otherwise.
Synonyms: always empty array (honest).

### AlterConfigs

| Ver | Additive |
|-----|----------|
| v0–1 | response throttle (leading); v1 wire-identical to v0 |

## Exit criteria

1. ApiVersions: DescribeConfigs max 3; AlterConfigs max 1
2. DescribeConfigs v0 response starts with throttle; field order correct
3. DescribeConfigs v3 returns config_source, empty synonyms, config_type, docs when requested
4. AlterConfigs v1 response starts with throttle
5. phase27 updated for framing; tests green

## Honest limitations

- TOPIC resources only
- No synonym chain / documentation beyond a few known keys
- IncrementalAlterConfigs remains classic v0 only
