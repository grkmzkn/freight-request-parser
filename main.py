import json
import ollama
import os
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()

# 'offline' or 'online'
LLM_TYPE = "offline"

OLLAMA_MODEL = "qwen2.5:3b"
GOOGLE_MODEL = "gemini-2.5-flash"

def parse_with_ollama(prompt: str, system_prompt: str, model_name: str) -> str:
    """Ollama ile çıkarım yapar ve metni döner."""
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
    """Google Gemini ile çıkarım yapar ve metni döner."""
    genai.configure(api_key=api_key)
    
    model = genai.GenerativeModel(
        model_name=model_name,
        system_instruction=system_prompt,
        generation_config={"temperature": 0.0}
    )
    
    response = model.generate_content(prompt)
    return response.text

def parse_freight_email(email_content: str) -> dict:
    """
    Verilen mail metninden istenilen nakliye bilgilerini çıkarır ve JSON formatında döner.
    LLM sağlayıcısını LLM_TYPE değişkeninden belirler.
    """
    
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
3. JSON anahtarları şunlar olmalıdır: "is_turu", "tarih", "romork_cinsi", "sicaklik_araligi", "adr_sinifi", "gtip_kodlari", "yuk_turu", "tonaj", "kalkis_noktasi", "varis_noktasi", "yukleme_tipi", "talep_durumu", "rota_notu".

E-posta içeriği:
{email_content}
"""

    system_prompt = 'Sen, Türkçe, İngilizce ve Rusça gelen nakliye metinlerini anlayan ve istenen bilgileri kesinlikle formatı bozulmamış bir JSON olarak ayıklayan uzman bir AI lojistik asistanısın.'
    
    try:
        # Seçili Modele İstek Atma
        if LLM_TYPE.lower() == "online":
            api_key = os.getenv("GOOGLE_API_KEY")
            if not api_key:
                print("HATA: .env dosyasında GOOGLE_API_KEY bulunamadı.")
                return None
            
            print(f"[*] Google modeli ({GOOGLE_MODEL}) ile analiz ediliyor [ONLINE]...")
            result_text = parse_with_google(prompt, system_prompt, GOOGLE_MODEL, api_key)
            
        elif LLM_TYPE.lower() == "offline":
            print(f"[*] Ollama modeli ({OLLAMA_MODEL}) ile analiz ediliyor [OFFLINE]...")
            result_text = parse_with_ollama(prompt, system_prompt, OLLAMA_MODEL)
            
        else:
            print(f"HATA: Geçersiz LLM_TYPE '{LLM_TYPE}'. 'offline' veya 'online' olmalı.")
            return None

        # Markdown formatında kod bloğu geldiyse temizle
        result_text = result_text.strip()
        if result_text.startswith("```json"): result_text = result_text[7:]
        if result_text.startswith("```"): result_text = result_text[3:]
        if result_text.endswith("```"): result_text = result_text[:-3]
        result_text = result_text.strip()
        
        # JSON'a ayrıştır
        parsed_data = json.loads(result_text)
        return parsed_data
        
    except json.JSONDecodeError:
        print("HATA: Modelden dönen metin geçerli bir JSON değil.\nDönen Metin:", result_text)
        return None
    except Exception as e:
        print("Beklenmeyen bir hata oluştu:", str(e))
        return None

if __name__ == "__main__":

    sample_email = """
    Konu: SB972/FCA Brugerio (Italy) - Uralsk / FTL +5+10
    Merhaba Yağmur hanım

    FCA Brugerio - Uralsk
    Frigo +5+10
    ADR-3 sınıf
    GTİP - 32091000, 32089091, 32081090
    21 ton
    Rusya ile geçebilir
    Yük hazır

    Saygılarımla / Best regards / С уважением

    """
    
    print("\n=== Nakliye Talebi Ayrıştırma ===")
    extracted_info = parse_freight_email(sample_email)
    
    if extracted_info:
        print("\nÇIKARILAN JSON BİLGİSİ:")
        print(json.dumps(extracted_info, indent=4, ensure_ascii=False))