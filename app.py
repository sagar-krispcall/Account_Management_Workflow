import streamlit as st
import pandas as pd
import numpy as np
from math import ceil

st.set_page_config(page_title="Payment Tier Automation", layout="wide")

st.title("💰 Payment Tier Automation App")
st.write("Upload all required CSV files below and generate the final report automatically.")

# ===============================
# FILE UPLOADS
# ===============================
st.subheader("📂 Upload Files")

payment_api_file = st.file_uploader("Payment API Export CSV", type=["csv"])
payment_mixpanel_file = st.file_uploader("Payment Mixpanel Export CSV", type=["csv"])
pipedrive_contacts_file = st.file_uploader("Pipedrive Contacts CSV", type=["csv"])
unpaid_user_file = st.file_uploader("Unpaid User Signup CSV", type=["csv"])

if st.button("🚀 Process Data"):

    if not (payment_api_file and payment_mixpanel_file and pipedrive_contacts_file and unpaid_user_file):
        st.error("Please upload **all four files** to continue.")
        st.stop()

    # Read files
    payment_api_export = pd.read_csv(payment_api_file, low_memory=False)
    payment_mixpanel_export = pd.read_csv(payment_mixpanel_file)
    pipedrive_contacts = pd.read_csv(pipedrive_contacts_file, low_memory=False)
    unpaid_user = pd.read_csv(unpaid_user_file)

    # ===============================
    # 1. PAYMENT API PROCESSING
    # ===============================
    pay = payment_api_export[['time', '$email', 'distinct_id', '$distinct_id_before_identity']]

    pay = pay.copy()
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
    pay["Time"] = pd.to_datetime(pay["Time"], unit="s")
    pay["Date"] = pay["Time"].dt.date

    pay = pay[['Email', 'Time', 'Date']]
    pay = pay.dropna(subset=['Email'])

    # first + last payment
    pay1 = pay.groupby('Email')['Date'].agg(
        First_Payment='min',
        Last_Payment='max'
    ).reset_index()

    today = pd.Timestamp.today()

    pay1['First_Payment'] = pd.to_datetime(pay1['First_Payment'])

    def months_since_first(date):
        delta_years = today.year - date.year
        delta_months = today.month - date.month
        delta_days = today.day - date.day
        total_months = delta_years * 12 + delta_months + delta_days / 30
        return ceil(total_months)

    pay1['Duration_Months'] = pay1['First_Payment'].apply(months_since_first)

    # ===============================
    # 2. MIXPANEL PAYMENT SUMMARY
    # ===============================
    pay2 = payment_mixpanel_export.groupby('Email').agg({
        'A. Payment (all time)': 'sum',
        'B. Amount (Year)': 'sum',
        'C. Amount (Month)': 'sum',
        'Workspace': 'first'
    }).reset_index()

    # Merge
    pay_merged = pd.merge(pay1, pay2, on='Email', how='right')

    pay_merged['Amount_per_month'] = np.where(
        pay_merged['Duration_Months'] > 0,
        pay_merged['A. Payment (all time)'] / pay_merged['Duration_Months'],
        pay_merged['A. Payment (all time)']
    )

    pay_merged['Amount_per_month'] = pay_merged['Amount_per_month'].round(2)

    # ===============================
    # TIER FUNCTION
    # ===============================

    def assign_tier(row):
        duration = row['Duration_Months']
        amount = row['Amount_per_month']

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

    # ===============================
    # 3. PIPEDRIVE CONTACTS MERGE
    # ===============================
    pipedrive_contacts.columns = pipedrive_contacts.columns.str.title()
    pipedrive_contacts['Email'] = pipedrive_contacts['Email'].str.strip()
    pipedrive_contacts['Phone_Country_Name'] = pipedrive_contacts['Phone_Country_Name'].str.strip()

    pipedrive_contacts = pipedrive_contacts.drop_duplicates(subset='Email')

    pay_merged_contacts = pd.merge(pay_merged, pipedrive_contacts, on='Email', how='left')
    pay_merged_contacts.drop_duplicates(subset='Email', inplace=True)

    # ===============================
    # 4. UNPAID USER MERGE
    # ===============================
    unpaid_user = unpaid_user[['$email', 'Phone Number', 'Phone Number Country']]
    unpaid_user = unpaid_user.rename(columns={'$email': 'Email'})
    unpaid_user = unpaid_user.drop_duplicates(subset='Email')

    final_merged = pd.merge(pay_merged_contacts, unpaid_user, on="Email", how='left')

    # ===============================
    # PHONE CLEANING
    # ===============================
    undefined_values = ['', ' ', '  ', 'undefined', 'Undefined', 'none', 'None', 'nan', 'NaN']

    final_merged['Phone Number'] = final_merged['Phone Number'].astype(str).str.strip().replace(undefined_values, pd.NA)
    final_merged['Phone'] = final_merged['Phone'].astype(str).str.strip().replace(undefined_values, pd.NA)
    final_merged['Phone_Country_Name'] = final_merged['Phone_Country_Name'].astype(str).str.strip().replace(undefined_values, pd.NA)
    final_merged['Phone Number Country'] = final_merged['Phone Number Country'].astype(str).str.strip().replace(undefined_values, pd.NA)

    final_merged['Phone_Number'] = final_merged['Phone Number'].fillna(final_merged['Phone'])
    final_merged['Phone_Country'] = final_merged['Phone_Country_Name'].fillna(final_merged['Phone Number Country'])

    final_merged = final_merged[['Email', 'Full_Name', 'First_Name', 'Last_Name', 'Phone_Number',
                                 'Phone_Country', 'First_Payment', 'Last_Payment', 'Duration_Months',
                                 'A. Payment (all time)', 'B. Amount (Year)', 'C. Amount (Month)',
                                 'Workspace', 'Amount_per_month', 'Tier']]

    # ===============================
    # TIER SUMMARY
    # ===============================
    tier_summary = final_merged.groupby('Tier').agg(
        Number_of_Users=('Email', 'count')
    ).reset_index()

    tier_summary['Percentage'] = (tier_summary['Number_of_Users'] /
                                  tier_summary['Number_of_Users'].sum() * 100).round(2)

    # ORDER TIERS
    tier_order = ['VIP', 'Platinum', 'Gold', 'Silver', 'Bronze']
    tier_summary['Tier'] = pd.Categorical(tier_summary['Tier'], categories=tier_order, ordered=True)
    tier_summary = tier_summary.sort_values('Tier').reset_index(drop=True)


    # ============================================
    # OUTPUT SECTION
    # ============================================
    st.success("Processing complete!")

    st.subheader("📊 Tier Summary")
    st.dataframe(tier_summary)

    st.subheader("📈 Tier Summary Chart")
    st.bar_chart(tier_summary.set_index("Tier")["Number_of_Users"])

    st.subheader("📄 Final Merged Data")
    st.dataframe(final_merged, use_container_width=True)

    # DOWNLOAD BUTTON
    csv = final_merged.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="⬇ Download Final CSV",
        data=csv,
        file_name="final_merged_output.csv",
        mime="text/csv"
    )
