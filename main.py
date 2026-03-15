import json
import ollama
import os
from dotenv import load_dotenv

try:
    import google.generativeai as genai
except ImportError:
    pass # If online is selected and it's not installed, we'll raise an error below.

# Load environment variables from .env file
load_dotenv()

# Select the operating mode here: 'offline' or 'online'
LLM_TYPE = "online" # "offline" runs the local Ollama model, "online" runs the Gemini model.

OLLAMA_MODEL = "qwen2.5:3b"
GOOGLE_MODEL = "gemini-2.5-flash"

def parse_with_ollama(prompt: str, system_prompt: str, model_name: str) -> str:
    """Performs inference using Ollama and returns the text."""
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
    """Performs inference using Google Gemini and returns the text."""
    if 'genai' not in globals():
        raise ImportError("google-generativeai library is not installed. Run 'pip install google-generativeai'.")
    
    genai.configure(api_key=api_key)
    
    # System instruction can be provided in Gemini models
    model = genai.GenerativeModel(
        model_name=model_name,
        system_instruction=system_prompt,
        generation_config={"temperature": 0.0}
    )
    
    response = model.generate_content(prompt)
    return response.text

def parse_freight_email(email_content: str) -> dict:
    """
    Extracts the requested freight information from the given email text and returns it in JSON format.
    Determines the LLM provider based on the LLM_TYPE variable.
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
3. JSON anahtarları tam olarak şunlar olmalıdır (Bunları İngilizce olarak ayarla): "job_type", "date", "trailer_type", "temperature_range", "adr_class", "gtip_codes", "cargo_type", "tonnage", "departure_point", "destination_point", "loading_type", "request_status", "route_note".

E-posta içeriği:
{email_content}
"""

    system_prompt = 'Sen, Türkçe, İngilizce ve Rusça gelen nakliye metinlerini anlayan ve istenen bilgileri kesinlikle formatı bozulmamış bir JSON olarak ayıklayan uzman bir AI lojistik asistanısın.'
    
    try:
        # Sending request to the selected model
        if LLM_TYPE.lower() == "online":
            api_key = os.getenv("GOOGLE_API_KEY")
            if not api_key:
                print("ERROR: GOOGLE_API_KEY could not be found in the .env file.")
                return None
            
            print(f"[*] Analyzing with Google model ({GOOGLE_MODEL}) [ONLINE]...")
            result_text = parse_with_google(prompt, system_prompt, GOOGLE_MODEL, api_key)
            
        elif LLM_TYPE.lower() == "offline":
            print(f"[*] Analyzing with Ollama model ({OLLAMA_MODEL}) [OFFLINE]...")
            result_text = parse_with_ollama(prompt, system_prompt, OLLAMA_MODEL)
            
        else:
            print(f"ERROR: Invalid LLM_TYPE '{LLM_TYPE}'. It must be 'offline' or 'online'.")
            return None

        # Clean if a markdown code block is returned
        result_text = result_text.strip()
        if result_text.startswith("```json"): result_text = result_text[7:]
        if result_text.startswith("```"): result_text = result_text[3:]
        if result_text.endswith("```"): result_text = result_text[:-3]
        result_text = result_text.strip()
        
        # Parse into JSON
        parsed_data = json.loads(result_text)
        return parsed_data
        
    except json.JSONDecodeError:
        print("ERROR: The text returned from the model is not a valid JSON.\nReturned Text:", result_text)
        return None
    except Exception as e:
        print("An unexpected error occurred:", str(e))
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
    
    print("\n=== Freight Request Parsing ===")
    extracted_info = parse_freight_email(sample_email)
    
    if extracted_info:
        print("\nEXTRACTED JSON INFORMATION:")
        print(json.dumps(extracted_info, indent=4, ensure_ascii=False))