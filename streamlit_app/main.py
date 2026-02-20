import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import psycopg2

# 1. Konfigurasi Halaman Dasar (Gunakan layout wide agar luas)
st.set_page_config(page_title="IHSG Market Monitor", layout="wide", page_icon="📈")

# 2. Fungsi Ekstrak Data (Koneksi Langsung Psycopg2 Tanpa Error)
@st.cache_data(ttl=3600) # Cache 1 jam
def load_data():
    db_uri = st.secrets["SUPABASE_URI"]
    conn = psycopg2.connect(db_uri)
    
    # Ambil data dari terlama ke terbaru
    query = "SELECT * FROM market_sentiment ORDER BY tanggal ASC"
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Pre-processing untuk UI: Hitung selisih harga untuk warna grafik Volume
    df['diff'] = df['ihsg_close'].diff()
    # Jika naik/sama = Hijau Neon, Jika turun = Merah Neon
    df['vol_color'] = df['diff'].apply(lambda x: '#00FA9A' if x >= 0 else '#FF3333')
    
    return df

try:
    df = load_data()
    
    # Ambil baris data terbaru dan kemarin
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    
    # Judul Dasbor
    st.markdown("<h1 style='text-align: center;'>📈 Dashboard Analisis Tren IHSG & Likuiditas Pasar</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: gray;'>Automated Data Pipeline via Apache Airflow | Algorithmic Sentiment Analysis</p>", unsafe_allow_html=True)
    st.divider()
    
    # 3. BARIS METRIK UTAMA (Scorecard)
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("IHSG Close", f"Rp {latest['ihsg_close']:,.0f}", f"{latest['ihsg_close'] - prev['ihsg_close']:,.0f}")
    with col2:
        st.metric("Status Trend (MA50)", latest['status_pasar'])
    with col3:
        # Format volume menjadi 'Juta Lembar'
        vol_juta = latest['top4_volume'] / 1000000
        st.metric("Likuiditas Top 4 Bank", f"{vol_juta:,.1f} Juta Lembar")
    with col4:
        st.metric("Momentum (RSI 14)", f"{latest['rsi']:.2f}")

    st.markdown("<br>", unsafe_allow_html=True)

    # 4. GRAFIK UTAMA: Harga IHSG vs MA50 (Area Chart Style)
    st.subheader("Pergerakan Harga IHSG & Tren Jangka Menengah")
    fig1 = go.Figure()
    
    # Garis Harga dengan Area Fill (Efek Neon Cyan)
    fig1.add_trace(go.Scatter(
        x=df['tanggal'], y=df['ihsg_close'], 
        fill='tozeroy', mode='lines', 
        line=dict(color='#00F0FF', width=3), 
        fillcolor='rgba(0, 240, 255, 0.1)', 
        name='IHSG Close'
    ))
    
    # Garis MA50 (Putus-putus Emas)
    fig1.add_trace(go.Scatter(
        x=df['tanggal'], y=df['ma50'], 
        mode='lines', 
        line=dict(color='#FFD700', dash='dash', width=2), 
        name='MA50 (Batas Tren)'
    ))
    
    # Setting tampilan agar Dark Mode dan Profesional
    fig1.update_layout(
        template="plotly_dark",
        height=450, 
        margin=dict(l=0, r=0, t=10, b=0),
        xaxis_title="", 
        yaxis_title="Harga Indeks", 
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    # Set range sumbu Y agar grafik tidak mulai dari 0 (agar fluktuasi terlihat jelas)
    min_y = df['ihsg_close'].min() * 0.95
    fig1.update_yaxes(range=[min_y, df['ihsg_close'].max() * 1.05])
    
    st.plotly_chart(fig1, use_container_width=True)

    # 5. GRAFIK BAWAH: Likuiditas & Spidometer RSI
    col_grafik1, col_grafik2 = st.columns([1.2, 1]) # Kolom kiri sedikit lebih lebar
    
    with col_grafik1:
        st.subheader("Volume Transaksi Harian (Lembar)")
        fig2 = go.Figure()
        # Bar Chart dengan warna dinamis
        fig2.add_trace(go.Bar(
            x=df['tanggal'], y=df['top4_volume'], 
            marker_color=df['vol_color'], 
            name='Volume'
        ))
        fig2.update_layout(
            template="plotly_dark",
            height=300, 
            margin=dict(l=0, r=0, t=10, b=0),
            xaxis_title="", yaxis_title=""
        )
        st.plotly_chart(fig2, use_container_width=True)

    with col_grafik2:
        st.subheader("Indikator RSI")
        # Gauge Chart (Spidometer)
        fig3 = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = latest['rsi'],
            number = {'suffix': " RSI", 'font': {'size': 30, 'color': "white"}},
            gauge = {
                'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "white"},
                'bar': {'color': "rgba(255, 255, 255, 0.4)", 'thickness': 0.3}, # Jarum penunjuk
                'bgcolor': "black",
                'borderwidth': 2,
                'bordercolor': "gray",
                'steps': [
                    {'range': [0, 30], 'color': '#00FA9A'},  # Zona Hijau (Murah/Oversold)
                    {'range': [30, 70], 'color': '#333333'},  # Zona Abu (Netral)
                    {'range': [70, 100], 'color': '#FF3333'}  # Zona Merah (Mahal/Overbought)
                ],
            }
        ))
        fig3.update_layout(
            template="plotly_dark",
            height=300, 
            margin=dict(l=30, r=30, t=20, b=20)
        )
        st.plotly_chart(fig3, use_container_width=True)
        
    # 6. TABEL DATA MENTAH
    st.markdown("<br>", unsafe_allow_html=True)
    with st.expander("📂 Tampilkan Data Historis Database (Raw Data)"):
        df_display = df.drop(columns=['vol_color']).copy()
        df_display['updated_at'] = pd.to_datetime(df_display['updated_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Ubah nama kolom database menjadi bahasa manusia
        df_display = df_display.rename(columns={
            'tanggal': 'Tanggal',
            'ihsg_close': 'IHSG Close',
            'diff': 'Perubahan Harga', 
            'top4_volume': 'Volume Top 4',
            'bbca_close': 'BBCA Close',
            'bbri_close': 'BBRI Close',
            'bmri_close': 'BMRI Close',
            'bbni_close': 'BBNI Close',
            'ma50': 'MA50',
            'rsi': 'RSI (14)',
            'status_pasar': 'Status Pasar',
            'updated_at': 'Terakhir Update'
        })
        
        # D. Susun ulang urutan kolom agar logis dan enak dibaca
        urutan_kolom = [
            'Tanggal', 'IHSG Close', 'Perubahan Harga', 'Volume Top 4', 
            'BBCA Close', 'BBRI Close', 'BMRI Close', 'BBNI Close', 
            'MA50', 'RSI (14)', 'Status Pasar', 'Terakhir Update'
        ]
        df_display = df_display[urutan_kolom]

        # E. Tampilkan tabel dengan format angka
        st.dataframe(df_display.sort_values('Tanggal', ascending=False).style.format({
            "IHSG Close": "{:,.2f}",
            "Perubahan Harga": "{:,.2f}",
            "Volume Top 4": "{:,.0f}",
            "BBCA Close": "{:,.0f}",
            "BBRI Close": "{:,.0f}",
            "BMRI Close": "{:,.0f}",
            "BBNI Close": "{:,.0f}",
            "MA50": "{:,.2f}",
            "RSI (14)": "{:.2f}"
        }), use_container_width=True)

except Exception as e:
    st.error(f"Gagal memuat data dari database. Pastikan koneksi aman. Error: {e}")