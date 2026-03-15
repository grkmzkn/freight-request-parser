import json
import ollama
import os
import pyodbc
from dotenv import load_dotenv

try:
    import google.generativeai as genai
except ImportError:
    pass

# Load environment variables
load_dotenv()

# --- SETTINGS ---
LLM_TYPE = "offline"  # "offline" or "online"
OLLAMA_MODEL = "qwen2.5:3b"
GOOGLE_MODEL = "gemini-2.5-flash"

SERVER_NAME = os.getenv("DB_SERVER_NAME")
DB_NAME = os.getenv("DB_NAME")


# ==========================================
# LLM & PARSING FUNCTIONS
# ==========================================

def parse_with_ollama(prompt: str, system_prompt: str, model_name: str) -> str:
    """Performs inference with Ollama and returns the text."""
    response = ollama.chat(
        model=model_name,
        messages=[
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': prompt}
        ],
        options={"temperature": 0.0}
    )
    return response['message']['content']

def parse_with_google(prompt: str, system_prompt: str, model_name: str, api_key: str) -> str:
    """Performs inference with Google Gemini and returns the text."""
    if 'genai' not in globals():
        raise ImportError("google-generativeai library is not installed.")
    
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(
        model_name=model_name,
        system_instruction=system_prompt,
        generation_config={"temperature": 0.0}
    )
    response = model.generate_content(prompt)
    return response.text

def parse_freight_email(email_content: str) -> dict:
    """Reads the given email text and returns structured data in JSON format."""
    prompt = f"""Aşağıdaki e-posta içeriğinden lojistik ve nakliye bilgilerini analiz ederek JSON formatında çıkar:
- iş türü (örnek: FCA, EXW, DAP)
- tarih (örnek: 15.03.2024, yarın, vs. Cümle içinde geçiyorsa yakala)
- römork cinsi (örnek: Frigo, Tenteli, Mega)
- sıcaklık aralığı (örnek: +5 +10, yoksa null)
- adr sınıfı (örnek: 3, 9, yoksa null)
- g tip kodları (örnek: [32091000, 32089091], mutlaka liste (array) formunda olmalı)
- yük türü (örnek: Kimyasal Ürün, Gıda, Tekstil vs.)
- tonaj (örnek: 21 ton, 22.000 kg)
- kalkış noktası (örnek: Brugerio, İtalya veya Brugerio)
- varış noktası (örnek: Uralsk, Kazakistan veya Uralsk)
- yükleme tipi (örnek: FTL, LTL, Parsiyel)
- talep durumu (örnek: Yük hazır, Cuma hazır olacak)
- rota notu (örnek: Rusya üzerinden geçebilir)

ÖNEMLİ KURALLAR:
1. SADECE geçerli bir JSON objesi döndür. Ekstra metin, ```json veya açıklama OLMASIN.
2. Bir bilgi çıkarılamıyorsa değerine null veya "belirtilmemiş" yaz.
3. JSON anahtarları tam olarak şunlar olmalıdır: "is_turu", "tarih", "romork_cinsi", "sicaklik_araligi", "adr_sinifi", "gtip_kodlari", "yuk_turu", "tonaj", "kalkis_noktasi", "varis_noktasi", "yukleme_tipi", "talep_durumu", "rota_notu".

E-posta içeriği:
{email_content}
"""
    system_prompt = 'Sen, Türkçe, İngilizce ve Rusça gelen nakliye metinlerini anlayan ve istenen bilgileri kesinlikle formatı bozulmamış bir JSON olarak ayıklayan uzman bir AI lojistik asistanısın.'
    
    try:
        if LLM_TYPE.lower() == "online":
            api_key = os.getenv("GOOGLE_API_KEY")
            if not api_key:
                print("ERROR: GOOGLE_API_KEY not found.")
                return None
            print(f"[*] Analyzing with Google model ({GOOGLE_MODEL}) [ONLINE]...")
            result_text = parse_with_google(prompt, system_prompt, GOOGLE_MODEL, api_key)
        elif LLM_TYPE.lower() == "offline":
            print(f"[*] Analyzing with Ollama model ({OLLAMA_MODEL}) [OFFLINE]...")
            result_text = parse_with_ollama(prompt, system_prompt, OLLAMA_MODEL)
        else:
            print(f"ERROR: Invalid LLM_TYPE '{LLM_TYPE}'.")
            return None

        result_text = result_text.strip()
        if result_text.startswith("```json"): result_text = result_text[7:]
        if result_text.startswith("```"): result_text = result_text[3:]
        if result_text.endswith("```"): result_text = result_text[:-3]
        result_text = result_text.strip()
        
        return json.loads(result_text)
        
    except json.JSONDecodeError:
        print("ERROR: Text returned from model is not a valid JSON.\nText:", result_text)
        return None
    except Exception as e:
        print("An unexpected error occurred:", str(e))
        return None


# ==========================================
# DATABASE (MSSQL) FUNCTIONS
# ==========================================

def get_connection(db=None):
    """Provides MSSQL connection."""
    conn_str = f"DRIVER={{SQL Server}};SERVER={SERVER_NAME};Trusted_Connection=yes;"
    if db:
        conn_str += f"DATABASE={db};"
    return pyodbc.connect(conn_str)

def init_db():
    """Creates database and tables."""
    conn = get_connection()
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"SELECT DB_ID('{DB_NAME}')")
    if not cursor.fetchone()[0]:
        cursor.execute(f"CREATE DATABASE {DB_NAME}")
        print(f"[{DB_NAME}] Database successfully created on MSSQL.")
    cursor.close()
    conn.close()

    conn_db = get_connection(DB_NAME)
    cursor_db = conn_db.cursor()

    cursor_db.execute('''
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='romork_tipleri' and xtype='U')
    CREATE TABLE romork_tipleri (
        id INT IDENTITY(1,1) PRIMARY KEY,
        isim NVARCHAR(255) NOT NULL UNIQUE
    )
    ''')

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

    for trailer in ["Frigo", "Tenteli", "Mega", "Açık", "Standart"]:
        cursor_db.execute('''
        IF NOT EXISTS (SELECT 1 FROM romork_tipleri WHERE isim = ?)
            INSERT INTO romork_tipleri (isim) VALUES (?)
        ''', (trailer, trailer))

    conn_db.commit()
    cursor_db.close()
    conn_db.close()

def get_trailer_id(trailer_name: str):
    """Fetches Trailer ID from database by name."""
    if not trailer_name or trailer_name.strip().lower() in ["belirtilmemiş", "not specified", "null"]:
        return None
    conn = get_connection(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('SELECT id FROM romork_tipleri WHERE isim = ?', (trailer_name.strip().title(),))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None

def insert_freight_request(parsed_data: dict):
    """Saves parsed LLM data to MSSQL."""
    if not parsed_data:
        return

    trailer_id = get_trailer_id(parsed_data.get("romork_cinsi", ""))
    gtip_val = parsed_data.get("gtip_kodlari")
    gtip_codes = json.dumps(gtip_val) if isinstance(gtip_val, list) else (str(gtip_val) if gtip_val else None)

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
    
    params = (
        parsed_data.get("is_turu"), parsed_data.get("tarih"), trailer_id,
        parsed_data.get("sicaklik_araligi"), parsed_data.get("adr_sinifi"),
        gtip_codes, parsed_data.get("yuk_turu"), parsed_data.get("tonaj"),
        parsed_data.get("kalkis_noktasi"), parsed_data.get("varis_noktasi"),
        parsed_data.get("yukleme_tipi"), parsed_data.get("talep_durumu"),
        parsed_data.get("rota_notu")
    )

    cursor.execute(sql, params)
    inserted_id = int(cursor.fetchone()[0])
    conn.commit()
    conn.close()
    
    print(f"[*] Request successfully added to MSSQL database! (Request ID: {inserted_id}, Trailer ID: {trailer_id})")

