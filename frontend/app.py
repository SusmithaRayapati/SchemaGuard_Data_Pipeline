
import streamlit as st
import pandas as pd
import urllib.request
import json

# --------------------------------------------------
# PAGE CONFIGURATION
# --------------------------------------------------

st.set_page_config(
    page_title="SchemaGuard Dashboard",
    page_icon="🛡️",
    layout="wide"
)

API = "http://127.0.0.1:8000"

# --------------------------------------------------
# CUSTOM CSS - DARK PROFESSIONAL THEME
# --------------------------------------------------

st.markdown("""
<style>

/* Main Background */
.stApp {
    background: linear-gradient(135deg, #0b1120, #111c35);
    color: #f8fafc;
}

/* Main Container */
.block-container {
    padding-top: 2rem;
    padding-bottom: 2rem;
}

/* Main Title */
h1 {
    color: #67e8f9 !important;
    font-size: 42px !important;
    font-weight: 800 !important;
}

/* Section Headings */
h2 {
    color: #c4b5fd !important;
    font-weight: 700 !important;
}

h3 {
    color: #93c5fd !important;
}

/* Caption */
.stCaption {
    color: #94a3b8 !important;
}

/* Metric Cards */
div[data-testid="stMetric"] {
    background: linear-gradient(135deg, #1e293b, #312e81);
    border: 1px solid #6366f1;
    padding: 20px;
    border-radius: 15px;
    box-shadow: 0 5px 18px rgba(99, 102, 241, 0.18);
    transition: 0.3s ease;
}

div[data-testid="stMetric"]:hover {
    border-color: #22d3ee;
    transform: translateY(-3px);
}

/* Metric Labels */
div[data-testid="stMetricLabel"] {
    color: #cbd5e1 !important;
    font-size: 14px;
}

/* Metric Values */
div[data-testid="stMetricValue"] {
    color: #67e8f9 !important;
    font-size: 30px !important;
    font-weight: 800 !important;
}

/* Buttons */
.stButton > button {
    background: linear-gradient(90deg, #6366f1, #06b6d4);
    color: white;
    border: none;
    border-radius: 10px;
    padding: 10px 22px;
    font-weight: bold;
    transition: 0.3s ease;
}

.stButton > button:hover {
    background: linear-gradient(90deg, #4f46e5, #0891b2);
    color: white;
    border: 1px solid #67e8f9;
}

/* Select Box */
div[data-baseweb="select"] > div {
    background-color: #1e293b;
    border-color: #6366f1;
    border-radius: 8px;
}

/* DataFrame Border */
div[data-testid="stDataFrame"] {
    border: 1px solid #475569;
    border-radius: 10px;
    overflow: hidden;
}

/* Dividers */
hr {
    border-color: #475569;
}

/* Info and Warning Messages */
div[data-testid="stAlert"] {
    border-radius: 10px;
}

/* JSON Display */
div[data-testid="stJson"] {
    background-color: #1e293b;
    border-radius: 10px;
    padding: 10px;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background-color: #111827;
}

</style>
""", unsafe_allow_html=True)


# --------------------------------------------------
# API CONNECTION
# --------------------------------------------------

def get_api_data(endpoint):
    try:
        request = urllib.request.Request(API + endpoint)

        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))

    except Exception as e:
        st.error(f"API Error: {e}")
        return {}


# --------------------------------------------------
# DASHBOARD HEADER
# --------------------------------------------------

st.title("🛡️ SchemaGuard")

st.caption(
    "Intelligent Parquet Schema Evolution & Data Quality Pipeline"
)

st.markdown("---")


# --------------------------------------------------
# FETCH BACKEND DATA
# --------------------------------------------------

health = get_api_data("/api/health")

runs_data = get_api_data("/api/runs")

runs = runs_data.get("runs", [])


if not runs and not health:
    st.warning(
        "Backend API is not responding. "
        "Please start FastAPI first."
    )


# --------------------------------------------------
# PIPELINE OVERVIEW
# --------------------------------------------------

st.subheader("📊 Pipeline Overview")

total = len(runs)

success = sum(
    r.get("status") == "SUCCESS" for r in runs
)

partial = sum(
    r.get("status") == "PARTIAL" for r in runs
)

failed = sum(
    r.get("status") == "FAILED" for r in runs
)

input_count = sum(
    r.get("total_input_records", 0) or 0 for r in runs
)

output_count = sum(
    r.get("total_output_records", 0) or 0 for r in runs
)

rejected_count = sum(
    r.get("total_rejected_records", 0) or 0 for r in runs
)


# --------------------------------------------------
# RUN STATUS CARDS
# --------------------------------------------------

c1, c2, c3, c4 = st.columns(4)

c1.metric("Total Runs", total)

c2.metric("Successful Runs", success)

c3.metric("Partial Runs", partial)

c4.metric("Failed Runs", failed)


# --------------------------------------------------
# RECORD COUNT CARDS
# --------------------------------------------------

c5, c6, c7 = st.columns(3)

c5.metric("Input Records", input_count)

c6.metric("Output Records", output_count)

c7.metric("Rejected Records", rejected_count)


st.markdown("---")


# --------------------------------------------------
# PIPELINE RUN HISTORY
# --------------------------------------------------

st.subheader("📁 Pipeline Run History")

if runs:

    history_df = pd.DataFrame(runs)

    st.dataframe(
        history_df,
        use_container_width=True,
        hide_index=True
    )

else:

    st.info("No pipeline runs found.")


st.markdown("---")


# --------------------------------------------------
# RUN DETAILS
# --------------------------------------------------

st.subheader("🔍 Run Details")

if runs:

    run_ids = [
        r["run_id"]
        for r in runs
    ]

    selected_run = st.selectbox(
        "Select Pipeline Run ID",
        run_ids
    )

    if st.button("📥 Load Run Details"):

        details = get_api_data(
            f"/api/runs/{selected_run}"
        )

        batches = get_api_data(
            f"/api/runs/{selected_run}/batches"
        )

        quarantine = get_api_data(
            f"/api/runs/{selected_run}/quarantine"
        )

        metrics = get_api_data(
            f"/api/runs/{selected_run}/metrics"
        )


        # Batch Details
        st.markdown("### 📦 Batch Details")

        batch_data = batches.get("batches", [])

        if batch_data:

            st.dataframe(
                pd.DataFrame(batch_data),
                use_container_width=True,
                hide_index=True
            )

        else:

            st.info("No batch details available.")


        # Quarantine History
        st.markdown("### ⚠️ Quarantine History")

        quarantine_data = quarantine.get(
            "quarantine_logs", []
        )

        if quarantine_data:

            st.dataframe(
                pd.DataFrame(quarantine_data),
                use_container_width=True,
                hide_index=True
            )

        else:

            st.info("No quarantine records available.")


        # Pipeline Metrics
        st.markdown("### 📈 Pipeline Metrics")

        metrics_data = metrics.get("metrics", [])

        if metrics_data:

            metrics_df = pd.DataFrame(metrics_data)

            st.dataframe(
                metrics_df,
                use_container_width=True,
                hide_index=True
            )

        else:

            st.info("No metrics available.")


        # Run Summary
        st.markdown("### 📋 Run Summary")

        if details:

            st.json(details)


# --------------------------------------------------
# REFRESH DASHBOARD
# --------------------------------------------------

st.markdown("---")

if st.button("🔄 Refresh Dashboard"):

    st.rerun()


# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.markdown("---")

st.markdown(
    "<center style='color:#94a3b8;'>"
    "SchemaGuard | Intelligent Data Engineering Pipeline"
    "</center>",
    unsafe_allow_html=True
)
