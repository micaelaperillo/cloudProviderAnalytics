import streamlit as st
import requests
import pandas as pd
from datetime import date, timedelta

API_URL = "http://localhost:8000"

st.set_page_config(page_title="Analytics Dashboard", layout="wide")

st.title("📊 Cloud Provider Analytics Dashboard")
st.markdown("Use the tabs below to execute analytics queries.")

# Create tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📅 Daily Costs & Requests",
    "🏆 Top-N Services",
    "🚨 SLA & Critical Tickets",
    "💰 Monthly Revenue",
    "🤖 GenAI Tokens"
])


# Helper to format results nicely
def show_table(df):
    st.dataframe(df, use_container_width=True)


def show_chart(df, x, y, type="line"):
    if type == "bar":
        st.bar_chart(df.set_index(x)[y])
    else:
        st.line_chart(df.set_index(x)[y])


# ============================================================
# TAB 1 — Daily Costs & Requests
# ============================================================
with tab1:
    st.header("📅 Costos y Requests Diarios")

    with st.container(border=True):
        org = st.text_input("Organización", key="q1_org")
        service = st.text_input("Servicio", key="q1_service")
        col1, col2 = st.columns(2)
        with col1:
            start = st.date_input("Fecha inicio", value=date.today() - timedelta(days=7), key="q1_start")
        with col2:
            end = st.date_input("Fecha fin", value=date.today(), key="q1_end")

        run = st.button("▶ Run Query", key="run_q1")

    if run:
        payload = {
            "organization": org,
            "service": service,
            "start_date": str(start),
            "end_date": str(end)
        }
        res = requests.post(f"{API_URL}/query/costs-daily", json=payload).json()

        daily = pd.DataFrame(res["daily"])

        st.subheader("📈 Cost Trend")
        show_chart(daily, "date", "cost_usd")

        st.subheader("📊 Requests Trend")
        show_chart(daily, "date", "requests")

        st.subheader("📋 Detailed Table")
        show_table(daily)


# ============================================================
# TAB 2 — Top-N Services
# ============================================================
with tab2:
    st.header("🏆 Top-N Servicios por Costo")

    with st.container(border=True):
        org = st.text_input("Organización", key="q2_org")
        top_n = st.number_input("Top-N", min_value=1, max_value=50, value=5, key="q2_topn")
        run = st.button("▶ Run Query", key="run_q2")

    if run:
        payload = {"organization": org, "top_n": top_n}
        res = requests.post(f"{API_URL}/query/top-services", json=payload).json()

        df = pd.DataFrame(res["services"])

        st.subheader("🏆 Ranking de Servicios")
        show_chart(df, "service", "total_cost_usd", type="bar")

        st.subheader("📋 Detalle")
        show_table(df)


# ============================================================
# TAB 3 — SLA & Critical Tickets
# ============================================================
with tab3:
    st.header("🚨 Tickets Críticos & SLA Breach")

    with st.container(border=True):
        org = st.text_input("Organización", key="q3_org")
        run = st.button("▶ Run Query", key="run_q3")

    if run:
        payload = {"organization": org}
        res = requests.post(f"{API_URL}/query/sla-evolution", json=payload).json()

        df = pd.DataFrame(res["days"])

        st.subheader("📉 Critical Tickets Over Time")
        show_chart(df, "date", "critical_tickets")

        st.subheader("⚠ SLA Breach Rate (%)")
        df["SLA_breach_%"] = df["sla_breach_rate"] * 100
        show_chart(df, "date", "SLA_breach_%")

        st.subheader("📋 Detailed Table")
        show_table(df)


# ============================================================
# TAB 4 — Monthly Revenue
# ============================================================
with tab4:
    st.header("💰 Revenue Mensual (Normalizado)")

    with st.container(border=True):
        org = st.text_input("Organización", key="q4_org")
        col1, col2 = st.columns(2)
        with col1:
            year = st.number_input("Año", 2000, 2100, date.today().year, key="q4_year")
        with col2:
            month = st.number_input("Mes", 1, 12, date.today().month, key="q4_month")

        run = st.button("▶ Run Query", key="run_q4")

    if run:
        payload = {"organization": org, "year": year, "month": month}
        res = requests.post(f"{API_URL}/query/monthly-revenue", json=payload).json()

        st.subheader("📌 Summary Metrics")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Revenue Base (USD)", f"{res['revenue_usd_normalized']:.2f}")
        c2.metric("Créditos Aplicados", f"{res['credits_applied_usd']:.2f}")
        c3.metric("Impuestos", f"{res['taxes_usd']:.2f}")
        c4.metric("Revenue Final (USD)", f"{res['final_revenue_usd']:.2f}")


# ============================================================
# TAB 5 — GenAI Tokens
# ============================================================
with tab5:
    st.header("🤖 GenAI Tokens & Estimated Cost")

    with st.container(border=True):
        org = st.text_input("Organización", key="q5_org")
        col1, col2 = st.columns(2)
        with col1:
            start = st.date_input("Fecha inicio", value=date.today() - timedelta(days=7), key="q5_start")
        with col2:
            end = st.date_input("Fecha fin", value=date.today(), key="q5_end")

        run = st.button("▶ Run Query", key="run_q5")

    if run:
        payload = {"organization": org, "start_date": str(start), "end_date": str(end)}
        res = requests.post(f"{API_URL}/query/genai-tokens", json=payload).json()

        df = pd.DataFrame(res["daily"])

        st.subheader("🔢 Tokens Input")
        show_chart(df, "date", "tokens_input")

        st.subheader("🔤 Tokens Output")
        show_chart(df, "date", "tokens_output")

        st.subheader("💵 Estimated Daily Cost")
        show_chart(df, "date", "cost_usd")

        st.subheader("📋 Detailed Table")
        show_table(df)
