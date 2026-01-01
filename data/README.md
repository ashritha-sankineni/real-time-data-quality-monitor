# Synthetic Event Generator

Generates newline-delimited JSON events for streaming demos.

## Output
`data/out/events.ndjson`
## Injected Issues
NEGATIVE_PRICE

MISSING_STORE_ID

NULL_SKU

QUANTITY_SPIKE

LATE_EVENT_TIME
## Run (local)
```bash
python data/event_generator.py --events 200 --bad-rate 0.15 --sleep-ms 50`





