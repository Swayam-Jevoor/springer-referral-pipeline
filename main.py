
#!/usr/bin/env python3
"""
main.py
Lightweight pipeline to load provided CSVs, profile tables, and generate a deduplicated final report (one row per referral_id).
Generated for Springer Capital take-home assessment (Option C: Pandas + aggregation).
"""
import pandas as pd
import numpy as np
from pathlib import Path
import pytz

DATA_DIR = Path("/data_input")  # when running in container mount your host upload folder to /data_input
OUT_DIR = Path("/data_output")  # mount an output folder to capture final report

OUT_DIR.mkdir(parents=True, exist_ok=True)

def load_csv(name):
    path = DATA_DIR / name
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()

def parse_dt_col(df, col):
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

def main():
    ur = load_csv("user_referrals.csv")
    tx = load_csv("paid_transactions.csv")
    rewards = load_csv("referral_rewards.csv")
    statuses = load_csv("user_referral_statuses.csv")
    ulogs = load_csv("user_logs.csv")
    leads = load_csv("lead_log.csv")
    urlogs = load_csv("user_referral_logs.csv")

    # Basic profiling saved to OUT_DIR
    for name, df in [("user_referrals", ur), ("paid_transactions", tx), ("referral_rewards", rewards), ("user_logs", ulogs), ("lead_logs", leads), ("user_referral_logs", urlogs), ("user_referral_statuses", statuses)]:
        if not df.empty:
            prof = pd.DataFrame({"column": df.columns, "null_count": df.isnull().sum().values, "distinct_count": df.nunique(dropna=True).reindex(df.columns).values})
            prof.to_csv(OUT_DIR / f"profile_{name}.csv", index=False)

    # Merge logic (similar to earlier notebook)
    if not ur.empty:
        if not rewards.empty and "id" in rewards.columns and "referral_reward_id" in ur.columns:
            ur = ur.merge(rewards.add_prefix("reward_"), left_on="referral_reward_id", right_on="reward_id", how="left")
        if not statuses.empty and "id" in statuses.columns and "user_referral_status_id" in ur.columns:
            ur = ur.merge(statuses.add_prefix("status_"), left_on="user_referral_status_id", right_on="status_id", how="left")
        if not tx.empty and "transaction_id" in ur.columns and "transaction_id" in tx.columns:
            ur = ur.merge(tx.add_prefix("tx_"), left_on="transaction_id", right_on="tx_transaction_id", how="left")
        if not ulogs.empty and "user_id" in ulogs.columns and "referrer_id" in ur.columns:
            ur = ur.merge(ulogs.add_prefix("referrer_"), left_on="referrer_id", right_on="referrer_user_id", how="left")
        if not leads.empty and "lead_id" in ur.columns and "lead_id" in leads.columns:
            ur = ur.merge(leads.add_prefix("lead_"), left_on="lead_id", right_on="lead_lead_id", how="left")
        if not urlogs.empty and "user_referral_id" in urlogs.columns and "id" in ur.columns:
            logs_agg = urlogs.groupby("user_referral_id", as_index=False).agg({"is_reward_granted":"max","created_at":"min"}).rename(columns={"is_reward_granted":"log_is_reward_granted","created_at":"log_created_at"})
            ur = ur.merge(logs_agg, left_on="id", right_on="user_referral_id", how="left")

        # coerce and parse
        if "reward_reward_value" in ur.columns:
            ur["reward_reward_value"] = pd.to_numeric(ur["reward_reward_value"], errors="coerce")
        for col in ["referral_at","updated_at","tx_transaction_at","created_at"]:
            if col in ur.columns:
                ur[col] = pd.to_datetime(ur[col], errors="coerce", utc=True)

        # derive category
        def derive_cat(row):
            src = str(row.get("referral_source","")).strip()
            if src == "User Sign Up": return "Online"
            if src == "Draft Transaction": return "Offline"
            if src == "Lead":
                return row.get("lead_source_category") if "lead_source_category" in row.index else None
            return None
        ur["referral_source_category"] = ur.apply(derive_cat, axis=1)

        # simple local conversion (UTC fallback)
        def to_local(dt, tz):
            if pd.isna(dt): return pd.NaT
            try:
                if pd.isna(tz) or tz== "": return dt.tz_convert(pytz.UTC).tz_localize(None)
                try:
                    tzobj = pytz.timezone(tz)
                    return dt.tz_convert(tzobj).tz_localize(None)
                except Exception:
                    return dt.tz_convert(pytz.UTC).tz_localize(None)
            except Exception:
                return pd.NaT
        ur["referral_at_local"] = ur.apply(lambda r: to_local(r.get("referral_at"), r.get("referrer_timezone_homeclub") if "referrer_timezone_homeclub" in r.index else None), axis=1)
        ur["transaction_at_local"] = ur.apply(lambda r: to_local(r.get("tx_transaction_at"), r.get("tx_timezone_transaction") if "tx_timezone_transaction" in r.index else None), axis=1)

        # flags and logic
        ur["referral_status_norm"] = ur.get("status_description", ur.get("referral_status","")).astype(str).str.strip().str.lower()
        ur["transaction_status_norm"] = ur.get("tx_transaction_status","").astype(str).str.strip().str.lower()
        ur["transaction_type_norm"] = ur.get("tx_transaction_type","").astype(str).str.strip().str.lower()
        ur["reward_granted_flag"] = ur.get("log_is_reward_granted", False).fillna(False).astype(bool) if "log_is_reward_granted" in ur.columns else False
        ur["tx_after_referral"] = ur["transaction_at_local"].notna() & ur["referral_at_local"].notna() & (ur["transaction_at_local"] > ur["referral_at_local"])
        ur["tx_same_month"] = ur["transaction_at_local"].notna() & ur["referral_at_local"].notna() & (ur["transaction_at_local"].dt.year == ur["referral_at_local"].dt.year) & (ur["transaction_at_local"].dt.month == ur["referral_at_local"].dt.month)
        ur["referrer_membership_valid"] = True
        if "referrer_membership_expired_date" in ur.columns:
            ur["referrer_membership_expired_date"] = pd.to_datetime(ur["referrer_membership_expired_date"], errors="coerce")
            mask = ur["referrer_membership_expired_date"].notna() & ur["referral_at_local"].notna()
            ur.loc[mask, "referrer_membership_valid"] = ur.loc[mask, "referrer_membership_expired_date"] > ur.loc[mask, "referral_at_local"].dt.normalize()

        def eval_logic(row):
            try:
                reward_value = row.get("reward_reward_value", np.nan)
                status = row.get("referral_status_norm","")
                has_tx = pd.notna(row.get("transaction_id")) and str(row.get("transaction_id")).strip() != ""
                tx_status = row.get("transaction_status_norm","")
                tx_type = row.get("transaction_type_norm","")
                tx_after = row.get("tx_after_referral", False)
                tx_same = row.get("tx_same_month", False)
                member_valid = row.get("referrer_membership_valid", True)
                not_deleted = not bool(row.get("referrer_is_deleted", False))
                reward_granted = row.get("reward_granted_flag", False)

                if (not pd.isna(reward_value) and float(reward_value)>0 and status=="berhasil" and has_tx and tx_status=="paid" and tx_type=="new" and tx_after and tx_same and member_valid and not_deleted and reward_granted):
                    return True
                if status in ["menunggu","tidak berhasil"] and (pd.isna(reward_value) or float(reward_value)==0):
                    return True
                if (not pd.isna(reward_value) and float(reward_value)>0 and status!="berhasil"): return False
                if (not pd.isna(reward_value) and float(reward_value)>0 and not has_tx): return False
                if (pd.isna(reward_value) or float(reward_value)==0) and has_tx and tx_status=="paid" and tx_after: return False
                if status=="berhasil" and (pd.isna(reward_value) or float(reward_value)==0): return False
                if has_tx and pd.notna(row.get("transaction_at_local")) and pd.notna(row.get("referral_at_local")) and (row.get("transaction_at_local") <= row.get("referral_at_local")): return False
                return False
            except Exception:
                return False

        ur["is_business_logic_valid"] = ur.apply(eval_logic, axis=1)

        # deduplicate per referral_id
        ur["referral_at_local"] = ur["referral_at_local"]
        ur_sorted = ur.sort_values(by=["referral_id","transaction_at_local","updated_at"], ascending=[True, False, False])
        ur_agg = ur_sorted.drop_duplicates(subset=["referral_id"], keep="first")
        out_path = OUT_DIR / "final_report_46.csv"
        ur_agg.to_csv(out_path, index=False)
        print("Saved aggregated report to:", out_path)

if __name__ == "__main__":
    main()
