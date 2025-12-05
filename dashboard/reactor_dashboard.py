import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# -------- Spark session (cached) --------
@st.cache_resource
def get_spark():
    return (
        SparkSession.builder
        .appName("ReactorDashboard")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

spark = get_spark()
gold_path = "output/reactor_metrics_1min"

st.set_page_config(page_title="Micro-Reactor Telemetry", layout="wide")
st.title("⚛️ Micro-Reactor Telemetry Dashboard")
st.caption("Simulator → Socket → Flink → Delta Lake → Spark → Dashboard")

# -------- Data loader (cached with small TTL) --------
@st.cache_data(ttl=10)
def load_data():
    df = spark.read.format("delta").load(gold_path)
    df = df.orderBy(col("window_start").desc())
    pdf = df.toPandas()
    return pdf

data = load_data()

if data.empty:
    st.warning("No data available yet. Make sure simulator, Flink, and Spark jobs have run.")
    st.stop()

# -------- Filters --------
reactors = sorted(data["reactor_id"].unique())
selected_reactor = st.sidebar.selectbox("Reactor", reactors)

reactor_df = (
    data[data["reactor_id"] == selected_reactor]
    .sort_values("window_start")
)

# -------- KPI cards --------
st.subheader(f"Current Status – Reactor {selected_reactor}")
latest = reactor_df.iloc[-1]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Avg Power (MW)", f"{latest['avg_power_mw']:.2f}")
col2.metric("Coolant Temp (°C)", f"{latest['avg_temp']:.1f}")
col3.metric("Ambient Pressure", f"{latest['avg_pressure']:.1f}")
col4.metric("Humidity (%)", f"{latest['avg_humidity']:.1f}")

# -------- Time series charts --------
st.subheader("Time Series (1-min windows)")

tab1, tab2 = st.tabs(["Power Output", "Environment"])

with tab1:
    st.line_chart(
        reactor_df.set_index("window_start")[["avg_power_mw", "max_power_mw", "min_power_mw"]]
    )

with tab2:
    st.line_chart(
        reactor_df.set_index("window_start")[["avg_temp", "avg_pressure", "avg_humidity"]]
    )

# -------- Raw metrics table --------
st.subheader("Recent Metrics")
st.dataframe(
    reactor_df.tail(200),
    use_container_width=True
)

st.sidebar.caption("Tip: refresh the browser or use Streamlit's rerun button to get fresh data.")
