from __future__ import annotations

import argparse
import os
import time
from datetime import date
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
from dotenv import dotenv_values
from kiteconnect import KiteConnect


ROOT = Path(__file__).resolve().parent.parent
ACCESS_TOKEN_FILE = ROOT / "broker" / "access_token.txt"


def load_kite() -> KiteConnect:
    vals = dotenv_values(ROOT / ".env")
    api_key = (vals.get("KITE_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("Missing KITE_API_KEY in .env")

    if not ACCESS_TOKEN_FILE.exists():
        raise RuntimeError(f"Missing access token file: {ACCESS_TOKEN_FILE}")
    access_token = ACCESS_TOKEN_FILE.read_text(encoding="utf-8").strip()
    if not access_token:
        raise RuntimeError("access_token.txt is empty. Login again.")

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    profile = kite.profile()
    print(f"Auth OK | {profile.get('user_name') or profile.get('user_id')}")
    return kite


def round_to_step(x: float, step: int) -> int:
    return int(round(float(x) / step) * step)


def strike_step(profile: str) -> int:
    return 100 if profile.upper() == "BANKNIFTY" else 50


def pick_option_symbol(
    kite: KiteConnect,
    profile: str,
    option_type: str,
    explicit_symbol: Optional[str] = None,
) -> Tuple[str, int, int]:
    nfo = pd.DataFrame(kite.instruments("NFO"))
    if nfo.empty:
        raise RuntimeError("NFO instruments is empty")

    if explicit_symbol:
        row = nfo[nfo["tradingsymbol"].astype(str).str.upper() == explicit_symbol.upper()]
        if row.empty:
            raise RuntimeError(f"tradingsymbol not found: {explicit_symbol}")
        r = row.iloc[0]
        return str(r["tradingsymbol"]), int(r["instrument_token"]), int(r.get("lot_size", 0) or 0)

    idx_symbol = "NIFTY BANK" if profile.upper() == "BANKNIFTY" else "NIFTY 50"
    quote = kite.quote([f"NSE:{idx_symbol}"])
    ltp = float(quote[f"NSE:{idx_symbol}"]["last_price"])
    step = strike_step(profile)
    strike = round_to_step(ltp, step)

    df = nfo.copy()
    df = df[df["segment"].astype(str).str.upper() == "NFO-OPT"]
    df = df[df["name"].astype(str).str.upper() == profile.upper()]
    df = df[df["instrument_type"].astype(str).str.upper() == option_type.upper()]
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df = df[df["strike"] == float(strike)]
    df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce")
    df = df.dropna(subset=["expiry"])
    df = df[df["expiry"].dt.date >= date.today()]
    if df.empty:
        raise RuntimeError(f"No {profile} {option_type} contract found at strike {strike}")

    df = df.sort_values(["expiry", "tradingsymbol"]).reset_index(drop=True)
    r = df.iloc[0]
    return str(r["tradingsymbol"]), int(r["instrument_token"]), int(r.get("lot_size", 0) or 0)


def get_order_snapshot(kite: KiteConnect, order_id: str) -> Optional[dict]:
    orders = kite.orders()
    for o in reversed(orders):
        if str(o.get("order_id")) == str(order_id):
            return o
    return None


def wait_for_status(
    kite: KiteConnect,
    order_id: str,
    statuses: set[str],
    timeout_sec: int,
    poll_sec: float = 1.0,
) -> Optional[dict]:
    t0 = time.time()
    while time.time() - t0 <= timeout_sec:
        o = get_order_snapshot(kite, order_id)
        if o and str(o.get("status", "")).upper() in statuses:
            return o
        time.sleep(poll_sec)
    return get_order_snapshot(kite, order_id)


def place_limit(
    kite: KiteConnect,
    exchange: str,
    tradingsymbol: str,
    tx_type: str,
    qty: int,
    price: float,
    product: str,
) -> str:
    oid = kite.place_order(
        variety=kite.VARIETY_REGULAR,
        exchange=exchange,
        tradingsymbol=tradingsymbol,
        transaction_type=tx_type,
        quantity=int(qty),
        product=product,
        order_type=kite.ORDER_TYPE_LIMIT,
        price=round(float(price), 1),
        validity=kite.VALIDITY_DAY,
    )
    return str(oid)


def place_market(
    kite: KiteConnect,
    exchange: str,
    tradingsymbol: str,
    tx_type: str,
    qty: int,
    product: str,
) -> str:
    oid = kite.place_order(
        variety=kite.VARIETY_REGULAR,
        exchange=exchange,
        tradingsymbol=tradingsymbol,
        transaction_type=tx_type,
        quantity=int(qty),
        product=product,
        order_type=kite.ORDER_TYPE_MARKET,
        validity=kite.VALIDITY_DAY,
    )
    return str(oid)


def main() -> None:
    parser = argparse.ArgumentParser(description="Live order smoke test for Zerodha API")
    parser.add_argument("--mode", choices=["nofill", "roundtrip"], required=True)
    parser.add_argument("--profile", choices=["nifty", "banknifty"], default="banknifty")
    parser.add_argument("--option-type", choices=["CE", "PE"], default="CE")
    parser.add_argument("--tradingsymbol", default=None, help="Optional exact NFO tradingsymbol")
    parser.add_argument("--lots", type=int, default=1)
    parser.add_argument("--product", choices=["NRML", "MIS"], default="NRML")
    parser.add_argument("--nofill-offset-pct", type=float, default=0.20)
    parser.add_argument("--fill-timeout-sec", type=int, default=30)
    parser.add_argument("--confirm", required=True, help="Must be LIVE_TEST to run")
    args = parser.parse_args()

    if args.confirm != "LIVE_TEST":
        raise RuntimeError("Refusing to run. Pass --confirm LIVE_TEST explicitly.")

    print("WARNING: This script places REAL ORDERS in your account.")
    kite = load_kite()

    profile = args.profile.upper()
    symbol, token, lot_size = pick_option_symbol(
        kite=kite,
        profile=profile,
        option_type=args.option_type,
        explicit_symbol=args.tradingsymbol,
    )
    if lot_size <= 0:
        raise RuntimeError(f"Invalid lot size for {symbol}: {lot_size}")
    qty = int(lot_size * max(1, args.lots))

    quote = kite.quote([f"NFO:{symbol}"])
    ltp = float(quote[f"NFO:{symbol}"]["last_price"])
    print(f"Instrument: {symbol} | token={token} | lot_size={lot_size} | qty={qty} | ltp={ltp}")

    exchange = "NFO"
    buy_tx = kite.TRANSACTION_TYPE_BUY
    sell_tx = kite.TRANSACTION_TYPE_SELL

    if args.mode == "nofill":
        price = max(0.05, ltp * (1.0 - float(args.nofill_offset_pct)))
        oid = place_limit(
            kite=kite,
            exchange=exchange,
            tradingsymbol=symbol,
            tx_type=buy_tx,
            qty=qty,
            price=price,
            product=args.product,
        )
        print(f"Placed NOFILL BUY LIMIT | order_id={oid} | price={price:.2f}")
        time.sleep(2)
        kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=oid)
        o = wait_for_status(kite, oid, {"CANCELLED", "COMPLETE", "REJECTED"}, timeout_sec=10)
        print(f"Final order status: {o.get('status') if o else 'UNKNOWN'}")
        return

    # roundtrip mode
    buy_price = max(0.05, ltp + 0.1)
    buy_oid = place_limit(
        kite=kite,
        exchange=exchange,
        tradingsymbol=symbol,
        tx_type=buy_tx,
        qty=qty,
        price=buy_price,
        product=args.product,
    )
    print(f"Placed BUY LIMIT | order_id={buy_oid} | price={buy_price:.2f}")

    buy_state = wait_for_status(kite, buy_oid, {"COMPLETE", "CANCELLED", "REJECTED"}, timeout_sec=args.fill_timeout_sec)
    buy_status = str((buy_state or {}).get("status", "UNKNOWN")).upper()
    print(f"BUY status: {buy_status}")

    if buy_status != "COMPLETE":
        try:
            kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=buy_oid)
            print("BUY not filled in time -> cancelled.")
        except Exception:
            pass
        return

    q2 = kite.quote([f"NFO:{symbol}"])
    ltp2 = float(q2[f"NFO:{symbol}"]["last_price"])
    sell_price = max(0.05, ltp2 - 0.1)

    sell_oid = place_limit(
        kite=kite,
        exchange=exchange,
        tradingsymbol=symbol,
        tx_type=sell_tx,
        qty=qty,
        price=sell_price,
        product=args.product,
    )
    print(f"Placed SELL LIMIT | order_id={sell_oid} | price={sell_price:.2f}")

    sell_state = wait_for_status(kite, sell_oid, {"COMPLETE", "CANCELLED", "REJECTED"}, timeout_sec=args.fill_timeout_sec)
    sell_status = str((sell_state or {}).get("status", "UNKNOWN")).upper()
    print(f"SELL status: {sell_status}")

    if sell_status != "COMPLETE":
        try:
            kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=sell_oid)
        except Exception:
            pass
        print("SELL not filled in time -> trying MARKET exit for safety.")
        moid = place_market(
            kite=kite,
            exchange=exchange,
            tradingsymbol=symbol,
            tx_type=sell_tx,
            qty=qty,
            product=args.product,
        )
        print(f"MARKET EXIT placed | order_id={moid}")

    print("Roundtrip smoke test finished.")


if __name__ == "__main__":
    main()
