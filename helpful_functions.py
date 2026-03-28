import json
import ollama
import os
import pyodbc
import pandas as pd
import re
from datetime import datetime
from dotenv import load_dotenv

try:
    import google.generativeai as genai
except ImportError:
    pass

# Load environment variables
load_dotenv()

# --- SETTINGS ---
LLM_TYPE = "offline"  # "offline" or "online"
OLLAMA_MODEL = "qwen2.5:7b" #3b 7b
GOOGLE_MODEL = "gemini-2.5-flash"

SERVER_NAME = os.getenv("DB_SERVER_NAME")
DB_NAME = os.getenv("DB_NAME")


# ==========================================
# TEXT PROCESSING & IO FUNCTIONS
# ==========================================

def clean_email_body(body):
    """Maillerin içeriğini temizleyip, imza ve uyarıları ayırır."""
    if not body:
        return ""
        
    # 1. Bilinen veda kalıplarından böl (case-insensitive)
    sign_offs = [
        r"best regards", r"kind regards", r"regards", r"sincerely", 
        r"saygilarimla", r"saygılarımla", r"iyi calismalar", r"iyi çalışmalar",
        r"с уважением", r"с наилучшими пожеланиями", r"mit freundlichen grüßen",
        r"mit freundlichen gruessen"
    ]
    pattern = re.compile(r'\b(?:' + '|'.join(sign_offs) + r')\b', re.IGNORECASE)
    body = pattern.split(body)[0]
    
    # 2. Ayrılmış çizgi bloklarını (--) sil
    body = re.split(r'\r?\n[ \t]*(?:--+|___+)[ \t]*\r?\n', body)[0]
    
    # 3. Yasal uyarılar ve KVKK metinlerinden böl
    disclaimers = [
        r"pursuant to reg", r"confidential information", 
        r"if you are not the addressee", r"bu e-posta", 
        r"this email and any attachments", r"the information contained in",
        r"gizlilik uyarisi", r"legal disclaimer"
    ]
    disc_pattern = re.compile(r'(?:' + '|'.join(disclaimers) + r')', re.IGNORECASE)
    body = disc_pattern.split(body)[0]
    
    # 4. Paragraf bazlı otomatik imza tespiti 
    # (Özellikle kalıp kullanmadan doğrudan imza atanlar için)
    # Metni 2 veya daha fazla "satır atlama" ile paragraflara ayır
    blocks = re.split(r'(?:\r?\n[ \t]*){2,}', body)
    valid_body = blocks[0]
    
    for block in blocks[1:]:
        # Bir bloğun "imza" kabul edilmesi için güçlü belirtiler:
        is_signature = False
        
        # 'mailto:' linki içermesi (Outlook doğrudan ekler)
        if re.search(r'mailto:', block, re.IGNORECASE):
            is_signature = True
        # Web sitesi / URL bulunması (http, https veya www)
        elif re.search(r'\b(?:https?://|www\.)[a-z0-9-]+(?:\.[a-z0-9-]+)+\b', block, re.IGNORECASE):
            is_signature = True
        # Hem telefon hem e-mail'in aynı paragrafta yer alması
        elif re.search(r'\+\d{2,3}[\s.-]?\d{3,}', block) and re.search(r'@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', block):
            is_signature = True
            
        if is_signature:
            break # Bu ve sonrasını imza kabul edip almayı bırakıyoruz
        else:
            valid_body += "\n\n" + block
            
    # 5. Kalan temiz metnin içindeki yeni satırları ve çift boşlukları toparla
    cleaned_content = valid_body.replace('\r\n', '\n')
    cleaned_content = ' '.join(cleaned_content.split(' ')).strip()
    
    return cleaned_content

def log_to_excel(email_content, parsed_data):
    """Logs the email content and parsed data to a daily Excel file."""
    today_str = datetime.now().strftime('%d-%m-%Y')
    log_file = f'log_{today_str}.xlsx'
    
    # Prepare the data for the new log entry
    log_entry_data = {
        'İşlenme Zamanı': datetime.now().strftime('%d-%m-%Y %H:%M:%S')
    }

    # If parsing was successful, add the parsed data as separate columns
    if parsed_data:
        # Convert list values to a string representation
        for key, value in parsed_data.items():
            if isinstance(value, list):
                parsed_data[key] = ', '.join(map(str, value))
        log_entry_data.update(parsed_data)
        
    # 'mail_icerik' sütunu en sonda olması için veriyi en sona ekliyoruz
    log_entry_data['mail_icerik'] = email_content
    
    # Create a new DataFrame for the current log entry
    new_log_entry = pd.DataFrame([log_entry_data])
    
    # Check if the log file already exists
    if os.path.exists(log_file):
        try:
            # Read the existing file
            df = pd.read_excel(log_file)
            # Append the new log entry
            df = pd.concat([df, new_log_entry], ignore_index=True)
        except Exception as e:
            print(f"Error reading Excel file: {e}")
            # If reading fails, create a new file with the current entry
            df = new_log_entry
    else:
        # If the file doesn't exist, the new entry is the DataFrame
        df = new_log_entry
        
    # Save the DataFrame to the Excel file
    try:
        df.to_excel(log_file, index=False)
    except Exception as e:
        print(f"Error writing to Excel file: {e}")


# ==========================================
# LLM & PARSING FUNCTIONS
# ==========================================

def parse_with_ollama(prompt: str, system_prompt: str, model_name: str):
    """Performs inference with Ollama and returns the text, also printing token usage."""
    response = ollama.chat(
        model=model_name,
        messages=[
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': prompt}
        ],
        options={
            "temperature": 0.0,
            "num_predict": 512,  # Cevaplar 120-160 token arası geldiği için limit 512'den 256'ya düşürüldü
            "num_ctx": 2048      # Gelen promptlar sabit olarak 1024 civarında geldiği için, biraz pay bırakılarak 1536 idealdir
        }
    )
    
    # Extract token metrics for Ollama
    prompt_tokens = response.get('prompt_eval_count', 0)
    eval_tokens = response.get('eval_count', 0)
    total_tokens = prompt_tokens + eval_tokens
    
    print(f"\n[TOKEN USAGE - Ollama] Prompt: {prompt_tokens} | Response: {eval_tokens} | Total: {total_tokens}\n")
    
    return response['message']['content']

def parse_with_google(prompt: str, system_prompt: str, model_name: str, api_key: str):
    """Performs inference with Google Gemini and returns both the text and token metadata."""
    if 'genai' not in globals():
        raise ImportError("google-generativeai library is not installed.")
    
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(
        model_name=model_name,
        system_instruction=system_prompt,
        generation_config={"temperature": 0.0}
    )
    response = model.generate_content(prompt)
    
    # Try to extract token metrics if available
    usage_metadata = getattr(response, 'usage_metadata', None)
    if usage_metadata:
        try:
            print(f"\n[TOKEN USAGE - Google] Prompt: {usage_metadata.prompt_token_count} | Response: {usage_metadata.candidates_token_count} | Total: {usage_metadata.total_token_count}\n")
        except AttributeError:
            pass
            
    return response.text

def parse_freight_email(email_content: str) -> dict:
    """Reads the given email text and returns structured data in JSON format."""
    prompt = f"""Aşağıdaki e-posta içeriğinden lojistik/nakliye bilgilerini çıkar ve SADECE JSON döndür.

Alan tanımları (uzman kurallar):
- romork_cinsi: Araç/ekipman tipi (Frigo, Tente, Mega, Kapalı Kasa, 45lik konteyner, swap body, konteyner şase vb.).
- yukleme_sehri: Yüklemenin yapılacağı şehir.
- yukleme_ulkesi: Yüklemenin yapılacağı ülke. (İPUCU: Metinde sadece şehir yazıyorsa bile, coğrafi bilgini kullanarak o şehrin hangi ülkede olduğunu bul ve buraya yaz. Örneğin 'Milano' görürsen ülkeye 'İtalya' yaz.)
- bosaltma_sehri: Teslimatın/boşaltmanın yapılacağı şehir.
- bosaltma_ulkesi: Teslimatın/boşaltmanın yapılacağı ülke. (İPUCU: Metinde sadece şehir yazıyorsa bile, coğrafi bilgini kullanarak o şehrin hangi ülkede olduğunu bul ve buraya yaz. Örneğin 'Münih' görürsen ülkeye 'Almanya' yaz.)
- yukleme_tipi: Metinde geçen değere göre FTL, LTL, LCL veya parsiyel vb. olduğu gibi yaz.
- tarih: Yükleme tarihi/haftası
- sicaklik_araligi: Taşıma sıcaklığı (+5 +10 gibi)
- adr_sinifi: ADR sınıfı
- gtip_kodlari: GTIP kodları listesi
- tonaj: Ağırlık
- notlar: Yükleme ile ilgili metinde geçen diğer önemli ek bilgiler, özel talepler, "Rusya'dan geçebilir", "Express teslimat", "Araçta tahta olmalı" gibi güzergah veya araca özel notlar.

ÖNEMLİ KURALLAR:
1. SADECE geçerli bir JSON objesi döndür. Ekstra metin, ```json veya açıklama OLMASIN.
2. Bir bilgi çıkarılamıyorsa değerine null veya "belirtilmemiş" yaz.
3. JSON anahtarları tam olarak şunlar olmalıdır: "romork_cinsi", "yukleme_tipi", "yukleme_sehri", "yukleme_ulkesi", "bosaltma_sehri", "bosaltma_ulkesi", "tarih", "sicaklik_araligi", "adr_sinifi", "gtip_kodlari", "tonaj", "notlar".

Örnek E-posta (One-Shot Example):
"
Konu: SB972/ FCA Brugerio (Italy)  - Uralsk / FTL +5+10

Merhaba Yağmur hanım 

FCA Brugerio – Uralsk 
Frigo +5+10
ADR – 3 sınıf
GTİP - 32091000, 32089091, 32081090 
21 ton
Rusya ile geçebilir 
Yük hazır

С уважением / Best regards,
"

Örnek Çıktı:
{{
  "romork_cinsi": "Frigo",
  "yukleme_tipi": "FTL",
  "yukleme_sehri": "Brugerio",
  "yukleme_ulkesi": "İtalya",
  "bosaltma_sehri": "Uralsk",
  "bosaltma_ulkesi": "Kazakistan",
  "tarih": null,
  "sicaklik_araligi": "+5 +10",
  "adr_sinifi": "3",
  "gtip_kodlari": [32091000, 32089091, 32081090],
  "tonaj": "21 ton",
  "notlar": "Rusya ile geçebilir, Yük hazır"
}}

Şimdi aşağıdaki e-posta içeriği için aynı işlemi gerçekleştir:
{email_content}
"""
    system_prompt = (
        "Sen lojistik ve tedarik zinciri alaninda uzmanlasmis, cok dilli (Turkce, Ingilizce, Rusca) "
        "kidemli bir veri cikarma asistanisin. Gorevin, nakliye e-postalarindan istenen alanlari "
        "ayiklayip SADECE gecerli bir JSON objesi uretmektir.\n\n"
        "KESIN KURALLAR:\n"
        "1. JSON disinda tek kelime yazma (selamlama, aciklama, kod blogu yok).\n"
        "2. Metinde yoksa uydurma; emin degilsen null değerini kullan.\n"
        "3. JSON anahtarlarini ASLA degistirme; sadece istenen anahtarlarla cevap ver."
    )
    
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
        
        parsed_data = json.loads(result_text)
        
        # --- KONTROL / POST-PROCESSING ADIMI ---
        
        # 1. Römork Cinsi Kontrolü
        # Eğer römork cinsi gelmemişse (null, empty, belirtilmemiş vb.) her zaman 'tente' yap.
        romork_val = parsed_data.get("romork_cinsi")
        if not romork_val or str(romork_val).strip().lower() in ["null", "none", "belirtilmemiş", ""]:
            parsed_data["romork_cinsi"] = "tenteli"
            
        # 1.1 Intermodal Kontrolü
        romork_cinsi_lower = str(parsed_data.get("romork_cinsi", "")).strip().lower()
        if romork_cinsi_lower in ["45lik konteyner", "swap body", "konteyner şase"]:
            parsed_data["intermodal"] = "Evet"
        else:
            parsed_data["intermodal"] = "Hayır"
            
        # 2. Yükleme Tipi Kontrolü (FTL vs LTL)
        yukleme_val = str(parsed_data.get("yukleme_tipi") or "").strip().lower()
        if "ftl" in yukleme_val:
            parsed_data["yukleme_tipi"] = "komple"
        elif any(kw in yukleme_val for kw in ["ltl", "lcl", "parsiyel"]):
            parsed_data["yukleme_tipi"] = "LTL"
            
        # 3. İş Türü Kontrolü ve Hesaplanması
        yukleme_ulkesi = str(parsed_data.get("yukleme_ulkesi") or "").strip().lower()
        bosaltma_ulkesi = str(parsed_data.get("bosaltma_ulkesi") or "").strip().lower()
        
        turkiye_aliases = ["türkiye", "turkiye", "turkey", "tr"]
        
        is_yuk_tr = any(alias in yukleme_ulkesi for alias in turkiye_aliases)
        is_bos_tr = any(alias in bosaltma_ulkesi for alias in turkiye_aliases)
        
        if not yukleme_ulkesi or yukleme_ulkesi == "null" or not bosaltma_ulkesi or bosaltma_ulkesi == "null":
            parsed_data["is_turu"] = "Belirsiz"
        elif is_yuk_tr and is_bos_tr:
            parsed_data["is_turu"] = "Yurtiçi"
        elif is_yuk_tr and not is_bos_tr:
            parsed_data["is_turu"] = "İhracat"
        elif not is_yuk_tr and is_bos_tr:
            parsed_data["is_turu"] = "İthalat"
        else:
            parsed_data["is_turu"] = "Transit"
            
        return parsed_data
        
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