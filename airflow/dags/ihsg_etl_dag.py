from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import pandas as pd
import yfinance as yf
import requests
import os


default_args = {
    'owner': 'shirayuq',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='ihsg_market_monitor_v5',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 18 * * 1-5',
    catchup=False,
    tags=['ihsg', 'latihan']
)
def ihsg_etl_pipeline():

    # ---------------------------------------------------------
    # TASK 1: EXTRACT IHSG (6 Bulan)
    # ---------------------------------------------------------
    @task
    def extract_ihsg():
        print("Menarik data sejarah IHSG...")
        ihsg = yf.download('^JKSE', period='6mo')
        
        # Meratakan MultiIndex & memberi nama spesifik agar tidak bentrok
        if isinstance(ihsg.columns, pd.MultiIndex):
            ihsg.columns = [f"{col[0]}_IHSG" for col in ihsg.columns]
        else:
            ihsg.columns = [f"{col}_IHSG" for col in ihsg.columns]
            
        ihsg.reset_index(inplace=True)
        # Ambil tanggalnya saja YYYY-MM-DD
        ihsg['Date'] = ihsg['Date'].astype(str).str[:10] 
        return ihsg.to_dict('list')

    # ---------------------------------------------------------
    # TASK 2: EXTRACT TOP STOCKS (6 Bulan Penuh)
    # ---------------------------------------------------------
    @task
    def extract_top_stocks():
        print("Menarik data 6 bulan terakhir dari 4 Bank Besar...")
        tickers = ['BBCA.JK', 'BBRI.JK', 'BMRI.JK', 'BBNI.JK']
        data = yf.download(tickers, period='6mo')
        
        # Meratakan MultiIndex -> cth: 'Close_BBCA'
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = [f"{col[0]}_{col[1].replace('.JK', '')}" for col in data.columns]
            
        data.reset_index(inplace=True)
        data['Date'] = data['Date'].astype(str).str[:10]
        return data.to_dict('list')

    # ---------------------------------------------------------
    # TASK 3: TRANSFORM (Pengolahan Pandas)
    # ---------------------------------------------------------
    @task
    def transform_data(ihsg_raw, stocks_raw):
        print("Memulai Transformasi Data Gabungan...")
        df_ihsg = pd.DataFrame(ihsg_raw)
        df_stocks = pd.DataFrame(stocks_raw)
        
        # Gabungkan berdasarkan tanggal
        df = pd.merge(df_ihsg, df_stocks, on='Date', how='inner')
        
        # 1. Buat Metrik top4_volume (Likuiditas Pasar)
        df['top4_volume'] = (
            df['Volume_BBCA'] + df['Volume_BBRI'] + 
            df['Volume_BMRI'] + df['Volume_BBNI']
        )
        
        # 2. Hitung Indikator Teknikal IHSG
        df['ma50'] = df['Close_IHSG'].rolling(window=50).mean()
        
        delta = df['Close_IHSG'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # 3. Buang data yang MA50-nya kosong (50 hari pertama)
        df = df.dropna().copy()
        
        # 4. Logika Bisnis: Status Pasar (Dihitung untuk SEMUA baris)
        def get_status(row):
            if row['Close_IHSG'] > row['ma50'] and row['rsi'] < 70:
                return "Bullish Trend"
            elif row['Close_IHSG'] < row['ma50'] and row['rsi'] < 30:
                return "Oversold (Potensi Rebound)"
            elif row['rsi'] >= 70:
                return "Overbought (Rawan Koreksi)"
            else:
                return "Sideways / Consolidation"
                
        df['status_pasar'] = df.apply(get_status, axis=1)
        
        # 5. Susun kolom sesuai database
        final_df = pd.DataFrame({
            'tanggal': df['Date'],
            'ihsg_close': df['Close_IHSG'],
            'top4_volume': df['top4_volume'],
            'bbca_close': df['Close_BBCA'],
            'bbri_close': df['Close_BBRI'],
            'bmri_close': df['Close_BMRI'],
            'bbni_close': df['Close_BBNI'],
            'ma50': df['ma50'].round(2),
            'rsi': df['rsi'].round(2),
            'status_pasar': df['status_pasar']
        })
        
        # Convert ke list of dictionary (Sangat aman untuk Airflow XCom)
        return final_df.to_dict('records')

    # ---------------------------------------------------------
    # TASK 4: DATA QUALITY CHECK
    # ---------------------------------------------------------
    @task
    def check_data_quality(final_data):
        print(f"Total data bersih siap diload: {len(final_data)} baris.")
        assert len(final_data) > 0, "ERROR: Data kosong setelah dibuang NaN!"
        
        # Cek baris terakhir (data terbaru)
        latest = final_data[-1]
        assert latest['ihsg_close'] > 0, "ERROR: Harga IHSG minus!"
        assert latest['top4_volume'] > 0, "ERROR: Volume 4 bank tidak terhitung!"
        print("Data Quality Check: PASSED!")
        return final_data

    # ---------------------------------------------------------
    # TASK 5: BRANCHING
    # ---------------------------------------------------------
    @task.branch
    def cek_hari_bursa():
        hari_ini = datetime.today().weekday()
        if hari_ini < 5: 
            return 'load_to_supabase'
        else:
            return 'skip_load'

    # ---------------------------------------------------------
    # TASK 6: LOAD (Metode TRUNCATE & APPEND)
    # ---------------------------------------------------------
    @task
    def load_to_supabase(final_data):
        print("Memulai proses FULL LOAD ke Supabase...")
        db_uri = os.environ.get('SUPABASE_URI')
        engine = create_engine(db_uri)
        df_load = pd.DataFrame(final_data)
        
        # TRUNCATE: Kosongkan tabel tapi biarkan strukturnya tetap utuh
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE market_sentiment;"))
            
        # APPEND: Masukkan seluruh data yang sudah diproses
        df_load.to_sql('market_sentiment', engine, if_exists='append', index=False)
        print(f"{len(df_load)} baris data sukses dimasukkan!")
        return f"Sukses Load {len(df_load)} baris data historis (V4)."

    # ---------------------------------------------------------
    # TASK 7 & 8: SKIP & ALERT
    # ---------------------------------------------------------
    skip_load = EmptyOperator(task_id='skip_load')

    @task(trigger_rule='all_done')
    def send_telegram_alert(ti):
        # MENGAMBIL RAHASIA DARI BRANKAS AIRFLOW SECARA AMAN
        from airflow.models import Variable
        TELEGRAM_TOKEN = Variable.get("TELEGRAM_TOKEN")
        TELEGRAM_CHAT_ID = Variable.get("TELEGRAM_CHAT_ID")
        
        state_load = ti.xcom_pull(task_ids='load_to_supabase')
        
        pesan = f"*Airflow ETL Report V5*\n\n"
        pesan += f"Pipeline: `ihsg_market_monitor_v5`\n"
        
        if ti.xcom_pull(task_ids='cek_hari_bursa') == 'skip_load':
            pesan += "Status: *Skipped (Weekend)* 💤\n"
        elif state_load:
            pesan += f"Status: *Berhasil* ✅\nDetail: {state_load}"
        else:
            pesan += "Status: *Gagal / Error* ❌\nHarap cek log Airflow!"

        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": pesan, "parse_mode": "Markdown"}
        requests.post(url, json=payload)

    # ==========================================
    # MENYUSUN DEPENDENCIES
    # ==========================================
    ihsg_data = extract_ihsg()
    stocks_data = extract_top_stocks()
    transformed_data = transform_data(ihsg_data, stocks_data)
    validated_data = check_data_quality(transformed_data)
    branch_task = cek_hari_bursa()
    load_task = load_to_supabase(validated_data)
    
    validated_data >> branch_task
    branch_task >> load_task
    branch_task >> skip_load
    [load_task, skip_load] >> send_telegram_alert()

etl_dag = ihsg_etl_pipeline()