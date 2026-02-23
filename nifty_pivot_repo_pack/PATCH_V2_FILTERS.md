# Patch your signal generation to enforce v2 filters

Apply these changes where your signals are created (likely in `signal_engine.py` / `test_signals.py` logic).

## 1) Entry cutoff helper
```python
def _is_before_entry_cutoff(ts: pd.Timestamp, cutoff_h=14, cutoff_m=0) -> bool:
    t = ts.time()
    return (t.hour < cutoff_h) or (t.hour == cutoff_h and t.minute <= cutoff_m)
```

## 2) SHORT levels: only S1
```python
for level_name in ["S1"]:  # v2: disable S2
    ...
```

## 3) LONG levels remain
```python
for level_name in ["R1", "R2"]:
    ...
```

## 4) Apply cutoff before creating any signal
```python
if not _is_before_entry_cutoff(candle_ts, 14, 0):
    continue
```
```
