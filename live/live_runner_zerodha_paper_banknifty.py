from pathlib import Path

from live import live_runner_zerodha_paper as base


# BankNifty underlying / option mapping
base.UNDERLYING_SYMBOL = "NIFTY BANK"
base.OPTION_UNDERLYING_NAME = "BANKNIFTY"

# Same strategy family, but BankNifty-specific target and PP activation
base.TARGET1_POINTS = 80.0
base.ALLOW_LONG_PP_SIGNAL = True
base.ALLOW_SHORT_PP_SIGNAL = True

# Separate logs / stop file so Nifty and BankNifty runs don't clash
base.SIGNALS_LOG_CSV = base.LOG_DIR / "live_paper_signals_log_banknifty.csv"
base.TRADES_LOG_CSV = base.LOG_DIR / "live_paper_option_trades_log_banknifty.csv"
base.ERROR_LOG_CSV = base.LOG_DIR / "live_paper_errors_log_banknifty.csv"
base.STATE_SNAPSHOT_JSON = base.LOG_DIR / "live_paper_state_snapshot_banknifty.json"
base.STOP_FILE = base.LOG_DIR / "STOP_TRADING_BANKNIFTY.txt"


if __name__ == "__main__":
    print("Configured for BANKNIFTY | PP enabled | TARGET1_POINTS=80")
    base.main()
