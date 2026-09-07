# AlgoBot Pro v6 V10.1 24x7 Exit Fix

- Backend Auto Paper entry threshold: 75% or higher.
- Persistent open positions are checked against a fresh public ticker about every 5 seconds.
- Full signal scans still use the configured 30s+ interval; exits are independent of signal scans.
- TP and SL automatically close positions and write EXIT events/trade history.
- Continues with browser closed while the Render service is awake.
- Paper only; no automatic live-money execution.
