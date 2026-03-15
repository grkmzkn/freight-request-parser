import pyodbc
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Pulling SERVER_NAME and DB_NAME from .env file.
SERVER_NAME = os.getenv("DB_SERVER_NAME")
DB_NAME = os.getenv("DB_NAME")

def get_connection(db=None):
    """Helper function to provide MSSQL connection."""
    # Using the built-in '{SQL Server}' Driver on Windows
    conn_str = f"DRIVER={{SQL Server}};SERVER={SERVER_NAME};Trusted_Connection=yes;"
    if db:
        conn_str += f"DATABASE={db};"
    
    return pyodbc.connect(conn_str)

def init_db():
    # 1. Connect to the master database first to create our project database if it doesn't exist
    conn = get_connection()
    conn.autocommit = True # CREATE DATABASE command cannot run inside a transaction (autocommit=False)
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT DB_ID('{DB_NAME}')")
    row = cursor.fetchone()
    if not row[0]:
        cursor.execute(f"CREATE DATABASE {DB_NAME}")
        print(f"[{DB_NAME}] Database successfully created on MSSQL.")
    
    cursor.close()
    conn.close()

    # 2. Connect to our database and create tables
    conn_db = get_connection(DB_NAME)
    cursor_db = conn_db.cursor()

    # Trailer Types Table
    cursor_db.execute('''
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='romork_tipleri' and xtype='U')
    CREATE TABLE romork_tipleri (
        id INT IDENTITY(1,1) PRIMARY KEY,
        isim NVARCHAR(255) NOT NULL UNIQUE
    )
    ''')

    # Freight Requests Table
    cursor_db.execute('''
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='nakliye_talepleri' and xtype='U')
    CREATE TABLE nakliye_talepleri (
        id INT IDENTITY(1,1) PRIMARY KEY,
        is_turu NVARCHAR(255),
        tarih NVARCHAR(255),
        romork_tipi_id INT,
        sicaklik_araligi NVARCHAR(255),
        adr_sinifi NVARCHAR(255),
        gtip_kodlari NVARCHAR(MAX), 
        yuk_turu NVARCHAR(255),
        tonaj NVARCHAR(255),
        kalkis_noktasi NVARCHAR(255),
        varis_noktasi NVARCHAR(255),
        yukleme_tipi NVARCHAR(255),
        talep_durumu NVARCHAR(255),
        rota_notu NVARCHAR(MAX),
        FOREIGN KEY (romork_tipi_id) REFERENCES romork_tipleri (id)
    )
    ''')

    # Insert Default (Common) Trailer Types
    default_trailers = ["Frigo", "Tenteli", "Mega", "Açık", "Standart"]
    for trailer in default_trailers:
        cursor_db.execute('''
        IF NOT EXISTS (SELECT 1 FROM romork_tipleri WHERE isim = ?)
            INSERT INTO romork_tipleri (isim) VALUES (?)
        ''', (trailer, trailer))

    conn_db.commit()
    cursor_db.close()
    conn_db.close()
    print("MSSQL Database and tables are ready for use.")

def get_romork_id(romork_isim: str):
    """
    Fetches the ID value from the MSSQL database based on the given trailer name.
    If it doesn't exist in the database, it performs no action (doesn't insert a new record) and returns None.
    """
    if not romork_isim or romork_isim.strip().lower() in ["belirtilmemiş", "not specified", "null"]:
        return None

    conn = get_connection(DB_NAME)
    cursor = conn.cursor()

    formatted_name = romork_isim.strip().title()
    
    cursor.execute('SELECT id FROM romork_tipleri WHERE isim = ?', (formatted_name,))
    row = cursor.fetchone()
    
    if row:
        romork_id = row[0]
    else:
        # We don't add a new row if it doesn't exist.
        romork_id = None

    conn.close()
    return romork_id

def insert_freight_request(parsed_data: dict):
    """
    Inserts the JSON (dict) formatted data received from the LLM into the MSSQL database.
    """
    if not parsed_data:
        print("No valid data found.")
        return

    # 1. Convert Trailer text to Numeric ID
    romork_text = parsed_data.get("romork_cinsi", "")
    romork_id = get_romork_id(romork_text)

    # 2. Save GTIP codes as a text string (since they usually come as a List)
    gtip_val = parsed_data.get("gtip_kodlari")
    if isinstance(gtip_val, list):
        gtip_kodlari = json.dumps(gtip_val)
    else:
        gtip_kodlari = str(gtip_val) if gtip_val else None

    # 3. Main Table Saving Process
    conn = get_connection(DB_NAME)
    cursor = conn.cursor()

    sql = '''
        SET NOCOUNT ON;
        INSERT INTO nakliye_talepleri (
            is_turu, tarih, romork_tipi_id, sicaklik_araligi, adr_sinifi, 
            gtip_kodlari, yuk_turu, tonaj, kalkis_noktasi, varis_noktasi, 
            yukleme_tipi, talep_durumu, rota_notu
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        SELECT SCOPE_IDENTITY();
    '''
    
    # Mapping parameters to send to MSSQL
    params = (
        parsed_data.get("is_turu"),
        parsed_data.get("tarih"),
        romork_id,  # Numeric ID
        parsed_data.get("sicaklik_araligi"),
        parsed_data.get("adr_sinifi"),
        gtip_kodlari,
        parsed_data.get("yuk_turu"),
        parsed_data.get("tonaj"),
        parsed_data.get("kalkis_noktasi"),
        parsed_data.get("varis_noktasi"),
        parsed_data.get("yukleme_tipi"),
        parsed_data.get("talep_durumu"),
        parsed_data.get("rota_notu")
    )

    cursor.execute(sql, params)
    inserted_id = int(cursor.fetchone()[0]) # ID of the inserted record
    
    conn.commit()
    conn.close()
    
    print(f"[*] Request successfully added to MSSQL database! (Request ID: {inserted_id}, Romork ID: {romork_id})")

if __name__ == "__main__":
    # Create/check tables
    init_db()
    
    # Sample data assuming it came from LLM (for testing)
    sample_llm_data = {
        "is_turu": "FCA",
        "tarih": "15.03.2026",
        "romork_cinsi": "Frigo",
        "sicaklik_araligi": "+5 +10",
        "adr_sinifi": "3",
        "gtip_kodlari": [32091000, 32089091, 32081090],
        "yuk_turu": "Kimyasal Ürün",
        "tonaj": "21 ton",
        "kalkis_noktasi": "Brugerio",
        "varis_noktasi": "Uralsk",
        "yukleme_tipi": "FTL",
        "talep_durumu": "Yük hazır",
        "rota_notu": "Rusya ile geçebilir"
    }

    print("\nStarting simulation...")
    insert_freight_request(sample_llm_data)