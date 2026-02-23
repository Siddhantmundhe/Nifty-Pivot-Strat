"""Example strategy config (v2) for your signal engine/backtest filters."""

ENTRY_CUTOFF_HHMM = 1400  # no new entries after 14:00 IST

ALLOW_LONG_LEVELS = {"R1", "R2"}
ALLOW_SHORT_LEVELS = {"S1"}  # S2 disabled based on current filtered backtest

# Cost model assumptions used in option paper backtest
SLIPPAGE_PER_SIDE = 0.50
CHARGES_PER_LOT_ROUNDTRIP = 60.0
