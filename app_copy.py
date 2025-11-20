# app.py
import streamlit as st
import pandas as pd
import numpy as np
import requests
import json
from math import ceil
from datetime import datetime

st.set_page_config(page_title="Mixpanel → Payment Tier Automation", layout="wide")
st.title("📦 Mixpanel → Payment Tier Automation App")

st.markdown(
    """
This app will:
- Fetch **Payment API** events automatically from Mixpanel (`New Payment Made`)
- Fetch **Unpaid signup** events automatically from Mixpanel (`Unpaid Signup User Details`)
- Accept **manual** uploads for `Payment Mixpanel export` (aggregated amounts) and `Pipedrive contacts`
- Produce final merged CSV with tiers and allow download
"""
)

# -------------------------
# Secrets / credentials
# -------------------------
try:
    MIXPANEL_API_KEY = st.secrets["MIXPANEL_API_KEY"]
    MIXPANEL_PROJECT_ID = st.secrets["MIXPANEL_PROJECT_ID"]
except Exception:
    st.error(
        "Missing Mixpanel credentials in st.secrets. Create `.streamlit/secrets.toml` with:\n\n"
        'MIXPANEL_API_KEY="your_basic_auth_string"\n'
        'MIXPANEL_PROJECT_ID="your_project_id"'
    )
    st.stop()

# -------------------------
# User Inputs
# -------------------------
st.sidebar.header("Export & Files")
col1, col2 = st.columns(2)
with col1:
    from_date = st.date_input("From date (for Mixpanel fetch)", datetime(2025, 8, 1))
with col2:
    to_date = st.date_input("To date (for Mixpanel fetch)", datetime(2025, 8, 31))

st.sidebar.markdown("### Manual uploads (required)")
payment_mixpanel_file = st.sidebar.file_uploader(
    "Payment Mixpanel export CSV (manual upload)", type=["csv"]
)
pipedrive_file = st.sidebar.file_uploader("Pipedrive contacts CSV (manual upload)", type=["csv"])

output_filename = st.text_input("Output CSV filename", "final_merged_output.csv")

run_button = st.button("🚀 Run full workflow")

# -------------------------
# Helper: fetch Mixpanel export by event name
# -------------------------
def fetch_mixpanel_event(event_name: str, from_date_str: str, to_date_str: str, where_expr: str = ""):
    """
    Fetch NDJSON export from Mixpanel for a single event.
    Returns a pandas DataFrame or raises Exception.
    """
    event_array_json = json.dumps([event_name])
    url = (
        f"https://data-eu.mixpanel.com/api/2.0/export?project_id={MIXPANEL_PROJECT_ID}"
        f"&from_date={from_date_str}&to_date={to_date_str}&event={event_array_json}"
    )
    if where_expr:
        url += f"&where={where_expr}"

    headers = {
        "accept": "text/plain",
        "authorization": f"Basic {MIXPANEL_API_KEY}",
    }

    resp = requests.get(url, headers=headers, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"Mixpanel fetch failed for '{event_name}' (status {resp.status_code})")
    text = resp.text.strip()
    if not text:
        return pd.DataFrame()  # empty
    # parse NDJSON lines
    objs = [json.loads(line) for line in text.splitlines() if line.strip()]
    df = pd.DataFrame(objs)
    # if properties exists, flatten it
    if "properties" in df.columns:
        prop = pd.json_normalize(df["properties"])
        df = pd.concat([df.drop(columns=["properties"]), prop], axis=1)
    return df

# -------------------------
# Main workflow
# -------------------------
if run_button:
    # validate manual uploads presence
    if payment_mixpanel_file is None or pipedrive_file is None:
        st.error("Please upload both: Payment Mixpanel export (CSV) and Pipedrive contacts (CSV) in the sidebar.")
        st.stop()

    # format dates
    from_date_str = from_date.strftime("%Y-%m-%d")
    to_date_str = to_date.strftime("%Y-%m-%d")

    # 1) Fetch Payment API Export (New Payment Made)
    with st.spinner("⏳ Fetching 'New Payment Made' from Mixpanel..."):
        try:
            payment_api_export = fetch_mixpanel_event("New Payment Made", from_date_str, to_date_str)
            st.success(f"Fetched 'New Payment Made' — rows: {len(payment_api_export)}")
        except Exception as e:
            st.error(f"Failed to fetch 'New Payment Made': {e}")
            st.stop()

    # 2) Fetch Unpaid Signup Users
    with st.spinner("⏳ Fetching 'Unpaid Signup User Details' from Mixpanel..."):
        try:
            unpaid_user_df = fetch_mixpanel_event("Unpaid Signup User Details", from_date_str, to_date_str)
            st.success(f"Fetched 'Unpaid Signup User Details' — rows: {len(unpaid_user_df)}")
        except Exception as e:
            st.error(f"Failed to fetch 'Unpaid Signup User Details': {e}")
            st.stop()

    # 3) Read manual Payment Mixpanel CSV (aggregated amounts)
    with st.spinner("⏳ Reading uploaded Payment Mixpanel CSV..."):
        try:
            payment_mixpanel_export = pd.read_csv(payment_mixpanel_file)
            st.success(f"Loaded Payment Mixpanel upload — rows: {len(payment_mixpanel_export)}")
        except Exception as e:
            st.error(f"Failed to read uploaded Payment Mixpanel CSV: {e}")
            st.stop()

    # 4) Read Pipedrive contacts CSV
    with st.spinner("⏳ Reading uploaded Pipedrive contacts CSV..."):
        try:
            pipedrive_contacts = pd.read_csv(pipedrive_file, low_memory=False)
            st.success(f"Loaded Pipedrive contacts — rows: {len(pipedrive_contacts)}")
        except Exception as e:
            st.error(f"Failed to read Pipedrive CSV: {e}")
            st.stop()

    # -------------------------
    # Processing: replicate your notebook logic
    # -------------------------
    with st.spinner("🔧 Processing Payment API data (extract email/time)..."):
        try:
            # ensure columns exist in payment_api_export
            # original notebook used: 'time', '$email', 'distinct_id', '$distinct_id_before_identity'
            pay = payment_api_export.copy()
            # handle missing columns gracefully
            for col in ["time", "$email", "distinct_id", "$distinct_id_before_identity"]:
                if col not in pay.columns:
                    pay[col] = pd.NA

            pay = pay[["time", "$email", "distinct_id", "$distinct_id_before_identity"]].copy()

            pay["email"] = pay.apply(
                lambda x: (
                    x["$email"]
                    if "@" in str(x["$email"])
                    else (
                        x["$distinct_id_before_identity"]
                        if "@" in str(x["$distinct_id_before_identity"])
                        else (
                            x["distinct_id"]
                            if "@" in str(x["distinct_id"])
                            else None
                        )
                    )
                ),
                axis=1
            )

            pay.columns = pay.columns.str.strip().str.title()
            # convert time if numeric seconds; if time already datetime-ish, let pandas handle
            try:
                pay["Time"] = pd.to_datetime(pay["Time"], unit="s", errors="coerce")
            except Exception:
                pay["Time"] = pd.to_datetime(pay["Time"], errors="coerce")
            pay["Date"] = pay["Time"].dt.date
            pay = pay[["Email", "Time", "Date"]]
            pay = pay.dropna(subset=["Email"])
            # first and last payment
            pay1 = pay.groupby("Email")["Date"].agg(First_Payment="min", Last_Payment="max").reset_index()
            pay1["First_Payment"] = pd.to_datetime(pay1["First_Payment"])
            today = pd.Timestamp.today()

            def months_since_first(date):
                if pd.isna(date):
                    return 0
                delta_years = today.year - date.year
                delta_months = today.month - date.month
                delta_days = today.day - date.day
                total_months = delta_years * 12 + delta_months + delta_days / 30
                return ceil(total_months)

            pay1["Duration_Months"] = pay1["First_Payment"].apply(months_since_first)
            st.success("Payment API processing done.")
        except Exception as e:
            st.error(f"Error processing payment_api_export: {e}")
            st.stop()

    with st.spinner("🔧 Processing Payment Mixpanel uploaded file (aggregations)..."):
        try:
            # We expect an 'Email' column and amount columns:
            # 'A. Payment (all time)', 'B. Amount (Year)', 'C. Amount (Month)', 'Workspace'
            pay2 = payment_mixpanel_export.copy()
            # normalize column names if needed
            # Group to ensure aggregated shape
            required_cols = ['Email', 'A. Payment (all time)', 'B. Amount (Year)', 'C. Amount (Month)', 'Workspace']
            for c in required_cols:
                if c not in pay2.columns:
                    pay2[c] = 0 if "Amount" in c or "Payment" in c else pd.NA
            pay2 = pay2.groupby('Email').agg({
                'A. Payment (all time)': 'sum',
                'B. Amount (Year)': 'sum',
                'C. Amount (Month)': 'sum',
                'Workspace': 'first'
            }).reset_index()
            st.success("Payment Mixpanel processing done.")
        except Exception as e:
            st.error(f"Error processing uploaded payment_mixpanel CSV: {e}")
            st.stop()

    with st.spinner("🔧 Merging payment datasets and calculating Amount_per_month..."):
        try:
            pay_merged = pd.merge(pay1, pay2, on='Email', how='right')
            pay_merged['Duration_Months'] = pay_merged['Duration_Months'].fillna(0).astype(int)
            pay_merged['A. Payment (all time)'] = pay_merged['A. Payment (all time)'].fillna(0)
            pay_merged['Amount_per_month'] = np.where(
                pay_merged['Duration_Months'] > 0,
                pay_merged['A. Payment (all time)'] / pay_merged['Duration_Months'],
                pay_merged['A. Payment (all time)']
            ).round(2)
            st.success("Merged payment data.")
        except Exception as e:
            st.error(f"Error merging payments: {e}")
            st.stop()

    with st.spinner("🔧 Assigning tiers..."):
        try:
            def assign_tier(row):
                duration = row.get('Duration_Months', 0) or 0
                amount = row.get('Amount_per_month', 0) or 0
                try:
                    duration = int(duration)
                except:
                    duration = 0
                try:
                    amount = float(amount)
                except:
                    amount = 0.0

                if duration >= 24 and amount >= 30:
                    return 'VIP'
                if (duration >= 12 and amount > 120) or \
                   (duration >= 6 and amount > 180) or \
                   (duration >= 3 and amount > 300):
                    return 'Platinum'
                if (duration >= 6 and amount > 80) or \
                   (duration >= 3 and amount > 120):
                    return 'Gold'
                if (duration >= 6 and amount >= 60) or \
                   (duration >= 3 and amount > 80):
                    return 'Silver'
                return 'Bronze'

            pay_merged['Tier'] = pay_merged.apply(assign_tier, axis=1)
            st.success("Tiers assigned.")
        except Exception as e:
            st.error(f"Error assigning tiers: {e}")
            st.stop()

    with st.spinner("🔧 Processing Pipedrive contacts and merging..."):
        try:
            pipedrive_contacts.columns = pipedrive_contacts.columns.str.title()
            if 'Email' not in pipedrive_contacts.columns:
                st.warning("Pipedrive file has no 'Email' column — merging may fail or produce many NaNs.")
            pipedrive_contacts['Email'] = pipedrive_contacts['Email'].astype(str).str.strip()
            # fill missing phone country columns if not present
            if 'Phone_Country_Name' not in pipedrive_contacts.columns:
                pipedrive_contacts['Phone_Country_Name'] = pd.NA
            pipedrive_contacts = pipedrive_contacts.drop_duplicates(subset='Email')
            pay_merged_contacts = pd.merge(pay_merged, pipedrive_contacts, on='Email', how='left')
            pay_merged_contacts.drop_duplicates(subset='Email', inplace=True)
            st.success("Pipedrive merge done.")
        except Exception as e:
            st.error(f"Error merging pipedrive contacts: {e}")
            st.stop()

    with st.spinner("🔧 Processing unpaid signup data and final phone cleanup..."):
        try:
            # unpaid_user_df should contain fields like '$email', 'Phone Number', 'Phone Number Country'
            unpaid = unpaid_user_df.copy()
            # adapt column names
            if '$email' not in unpaid.columns and 'Email' in unpaid.columns:
                unpaid = unpaid.rename(columns={'Email': '$email'})
            for col in ['$email', 'Phone Number', 'Phone Number Country']:
                if col not in unpaid.columns:
                    unpaid[col] = pd.NA
            unpaid_user = unpaid[['$email', 'Phone Number', 'Phone Number Country']].rename(columns={'$email': 'Email'})
            unpaid_user = unpaid_user.drop_duplicates(subset='Email')

            final_merged = pd.merge(pay_merged_contacts, unpaid_user, on="Email", how='left')

            # phone cleanup
            undefined_values = ['', ' ', '  ', 'undefined', 'Undefined', 'none', 'None', 'nan', 'NaN']
            # ensure columns exist before operations
            for col in ['Phone Number', 'Phone', 'Phone_Country_Name', 'Phone Number Country']:
                if col not in final_merged.columns:
                    final_merged[col] = pd.NA

            final_merged['Phone Number'] = final_merged['Phone Number'].astype(str).str.strip().replace(undefined_values, pd.NA)
            final_merged['Phone'] = final_merged['Phone'].astype(str).str.strip().replace(undefined_values, pd.NA)
            final_merged['Phone_Country_Name'] = final_merged['Phone_Country_Name'].astype(str).str.strip().replace(undefined_values, pd.NA)
            final_merged['Phone Number Country'] = final_merged['Phone Number Country'].astype(str).str.strip().replace(undefined_values, pd.NA)

            final_merged['Phone_Number'] = final_merged['Phone Number'].fillna(final_merged['Phone'])
            final_merged['Phone_Country'] = final_merged['Phone_Country_Name'].fillna(final_merged['Phone Number Country'])

            # select final columns (if missing columns, create them)
            final_columns = ['Email', 'Full_Name', 'First_Name', 'Last_Name', 'Phone_Number', 'Phone_Country',
                             'First_Payment', 'Last_Payment', 'Duration_Months', 'A. Payment (all time)',
                             'B. Amount (Year)', 'C. Amount (Month)', 'Workspace', 'Amount_per_month', 'Tier']
            for c in final_columns:
                if c not in final_merged.columns:
                    final_merged[c] = pd.NA

            final_merged = final_merged[final_columns]
            st.success("Final merged dataset prepared.")
        except Exception as e:
            st.error(f"Error preparing final merged dataset: {e}")
            st.stop()

    # Tier summary table (no chart as requested)
    with st.spinner("🔎 Calculating tier summary..."):
        try:
            tier_summary = final_merged.groupby('Tier').agg(Number_of_Users=('Email', 'count')).reset_index()
            tier_summary['Percentage'] = (tier_summary['Number_of_Users'] / tier_summary['Number_of_Users'].sum() * 100).round(2)
            tier_order = ['VIP', 'Platinum', 'Gold', 'Silver', 'Bronze']
            tier_summary['Tier'] = pd.Categorical(tier_summary['Tier'], categories=tier_order, ordered=True)
            tier_summary = tier_summary.sort_values('Tier').reset_index(drop=True)
            st.success("Tier summary ready.")
        except Exception as e:
            st.error(f"Error calculating tier summary: {e}")
            st.stop()

    # -------------------------
    # Display outputs and download
    # -------------------------
    st.header("✅ Results")
    st.subheader("Tier Summary")
    st.dataframe(tier_summary)

    st.subheader("Final Merged Data")
    st.dataframe(final_merged, use_container_width=True)

    csv_bytes = final_merged.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download Final CSV", csv_bytes, file_name=output_filename, mime="text/csv")
