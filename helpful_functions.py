import json
import ollama
import os
import pyodbc
import pandas as pd
import re
from datetime import datetime
from dotenv import load_dotenv

try:
    from geopy.geocoders import Nominatim
except ImportError:
    pass

try:
    import google.generativeai as genai
except ImportError:
    pass

# Ortam değişkenlerini yükle
load_dotenv()

# --- AYARLAR ---
LLM_TYPE = "offline"  # "offline" or "online"
OLLAMA_MODEL = "qwen2.5:7b" #qwen2.5:3b qwen2.5:7b qwen3:8b qwen2.5:14b
GOOGLE_MODEL = "gemini-2.5-flash"

SERVER_NAME = os.getenv("DB_SERVER_NAME")
DB_NAME = os.getenv("DB_NAME")


# ==========================================
# METİN İŞLEME VE G/Ç (Girdi/Çıktı) FONKSİYONLARI
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
        # Sadece Telefon numarası (Uluslararası format veya sık kullanılan tanımlar: Tel, Mob vs.) 
        # tespit edilirse, bu bloğu ve sonrasını imza kabul et 
        elif re.search(r'(?:\b(?:tel|mob|gsm|phone|telefon|cell|t|m)\b[\s.:]*\+?\d{2,})|(?:\+\d{2,3}[\s.-]?\d{3,}[\s.-]?\d{2,})', block, re.IGNORECASE):
            is_signature = True
        # Zaten e-mail adresi (isim@domain.com) geçiyorsa doğrudan imza kabul et
        elif re.search(r'@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', block):
            is_signature = True
            
        if is_signature:
            break # Bu ve sonrasını imza kabul edip almayı bırakıyoruz
        else:
            valid_body += "\n\n" + block
            
    # 5. Kalan temiz metnin içindeki yeni satırları ve çift boşlukları toparla
    cleaned_content = valid_body.replace('\r\n', '\n')
    cleaned_content = ' '.join(cleaned_content.split(' ')).strip()
    
    return cleaned_content

def verify_city(city_name, country_name=None):
    """Verilen lokasyonun (semt/ilçe) bağlı olduğu asıl Şehri ve Ülkeyi bulur."""
    # Bazı ülkelerin isimlerini standartlaştırma
    if country_name and country_name.strip().lower() in ["ozman", "ozakman"]:
        country_name = "Uzbekistan"

    is_city_empty = not city_name or str(city_name).lower() in ["null", "none", "", "belirtilmemiş"]
    is_country_empty = not country_name or str(country_name).lower() in ["null", "none", "", "belirtilmemiş"]
    
    if is_city_empty and is_country_empty:
        return city_name, country_name
        
    search_query = ""
    if not is_city_empty:
        search_query += str(city_name)
    if not is_country_empty:
        if search_query:
            search_query += f", {country_name}"
        else:
            search_query = str(country_name)
        
    try:
        # Eğer geopy yüklü değilse orjinal ismi ve boş ülke dön (kod kırılasın)
        if 'Nominatim' not in globals():
            return city_name, country_name
            
        geolocator = Nominatim(user_agent="mutlular_lojistik_bot")
        
        import time
        # 1. Önce Türkçe dilde çek
        location_tr = geolocator.geocode(search_query, addressdetails=True, namedetails=True, timeout=3, language="tr")
        time.sleep(1.5) # Nominatim 1 saniye kuralı için 1.5 sn uyuma (5 saniye programı çok yavaşlatır)
        # 2. Sonra İngilizce dilde çek
        location_en = geolocator.geocode(search_query, addressdetails=True, namedetails=True, timeout=3, language="en")
        
        location = location_tr or location_en
        
        if location and 'address' in location.raw:
            address = location_tr.raw['address'] if location_tr else location_en.raw['address']
            names = location.raw.get('namedetails', {})
            
            country_tr = location_tr.raw['address'].get('country') if location_tr and 'address' in location_tr.raw else ""
            country_en = location_en.raw['address'].get('country') if location_en and 'address' in location_en.raw else ""
            
            llm_country = str(country_name).strip().lower() if country_name else ""
            
            # LLM'in İngilizce (Belarus) veya Türkçe (Beyaz Rusya) eşleşmelerinden 
            # haritanın çevirilerinden birine uyuyorsa LLM'in orijinal halini (Örn: Belarus) koru
            if llm_country and (llm_country == str(country_en).strip().lower() or llm_country == str(country_tr).strip().lower()):
                real_country = country_name.strip()
            else:
                real_country = country_tr or country_en # Uymuyorsa haritanın Türkçe/İngilizce birleşimini al
                
            # Bazı ülkelerin isimlerini standartlaştırma (Örn: Rusya Federasyonu -> Rusya)
            if real_country and real_country.strip().lower() in ["rusya federasyonu", "russian federation"]:
                real_country = "Rusya"

            # Bazı ülkelerin isimlerini standartlaştırma (Örn: Rusya Federasyonu -> Rusya)
            if real_country and real_country.strip().lower() in ["beyaz rusya"]:
                real_country = "Belarus"
                
            # Sadece ülke girdiysek, şehri boş döndür ama ülkeyi doğrulanmış dön
            if is_city_empty:
                return None, real_country
                
            # Türkiye'de ilçeler 'town', şehirler (iller) 'province' olarak döner. 
            # Lojistikte il bazında gruplama yapmak için TR'deysek önceliği province'e (İl'e) veriyoruz.
            if address.get('country_code', '').lower() == 'tr':
                default_city = address.get('province') or address.get('city') or address.get('town') or address.get('state') or city_name
            else:
                default_city = address.get('city') or address.get('town') or address.get('province') or address.get('state') or city_name
            
            # "Eyaleti", "Region", "İli" gibi harita servisinden gelen idari ekleri temizleyen yardımcı fonksiyon
            def clean_suffix(name):
                if not name or not isinstance(name, str): return name
                suffixes = [r'\s+eyaleti$', r'\s+region$', r'\s+province$', r'\s+ili$', r'\s+oblast$', r'\s+oblastı$', r'\s+vilayeti$', r'\s+city$', r'\s+district$']
                for suffix in suffixes:
                    name = re.sub(suffix, '', name, flags=re.IGNORECASE)
                return name.strip()
                
            cleaned_input = clean_suffix(city_name).lower()
            
            # Öncelikli kontrol edilecek diller (Türkçe ve İngilizce) ile harita varsayılanı
            tr_name = clean_suffix(names.get('name:tr'))
            en_name = clean_suffix(names.get('name:en'))
            def_name = clean_suffix(default_city)
            
            # --- ÜST HİYERARŞİ (İL/ŞEHİR) KONTROLÜ ---
            # Bulunan objenin kendi tercümeleri (names.values())
            valid_names_lower = [clean_suffix(str(v)).lower() for v in names.values() if v]
            is_parent_entity = True
            
            if def_name:
                for vn in valid_names_lower:
                    if def_name.lower() == vn or def_name.lower() in vn or vn in def_name.lower():
                        is_parent_entity = False
                        break
            else:
                is_parent_entity = False
                
            # Eğer haritanın döndürdüğü üst şehir/il (def_name), bulunan hedefin (Erenköy)
            # çevirilerinden hiçbirine uymuyorsa, obje bir alt birimdir (ilçe/semt) demektir.
            # Lojistikte ana şehri/ili kullanmak için bu üst birimi döndürüyoruz.
            if is_parent_entity and def_name:
                return def_name, real_country
            
            prioritized_locations = []
            if tr_name: prioritized_locations.append(tr_name)
            if en_name: prioritized_locations.append(en_name)
            if def_name: prioritized_locations.append(def_name)
            
            final_city = clean_suffix(city_name)
            match_found = False
            
            # Öncelikle TR veya EN adı, LLM'in çıkardığı ifadenin içinde geçiyor mu diye bak
            # Örn: loc="Rotterdam", cleaned_input="rotterdam-waalhaven" -> Eşleşir ve "Rotterdam" döner
            for loc in prioritized_locations:
                if loc.lower() in cleaned_input:
                    final_city = loc
                    match_found = True
                    break
                    
            if not match_found:
                # Eşleşme yoksa eski uluslararası liste (esnek) eşleşmesini ara (Yazım hatası varsa orijinali koru)
                valid_names = [clean_suffix(str(v)).lower() for v in names.values() if v]
                valid_names.append(def_name.lower())
                
                is_flexible_match = False
                for vn in valid_names:
                    if cleaned_input == vn or cleaned_input in vn or vn in cleaned_input:
                        is_flexible_match = True
                        break
                        
                if not is_flexible_match:
                    final_city = def_name # Hiçbir şekilde eşleşmiyorsa haritanın varsayılan ismini kullan
            
            return final_city, real_country
            
    except Exception as e:
        print(f"[Uyarı] Şehir/Lokasyon doğrulanırken hata ({city_name}, {country_name}): {e}")
        
    return city_name, country_name # Bulamazsa veya hata alırsa LLM'in verdiği orijinal değeri döner

def log_to_excel(email_content, parsed_data):
    """Mail içeriğini ve ayrıştırılmış verileri günlük bir Excel dosyasına kaydeder."""
    today_str = datetime.now().strftime('%d-%m-%Y')
    log_file = f'log_{today_str}.xlsx'
    
    # Yeni log kaydı için veriyi hazırla
    log_entry_data = {
        'İşlenme Zamanı': datetime.now().strftime('%d-%m-%Y %H:%M:%S')
    }

    # Ayrıştırma başarılıysa, ayrıştırılan verileri ayrı sütunlar olarak ekle
    if parsed_data:
        # Liste değerlerini metin (string) formatına dönüştür
        for key, value in parsed_data.items():
            if isinstance(value, list):
                parsed_data[key] = ', '.join(map(str, value))
        log_entry_data.update(parsed_data)
        
    # 'mail_icerik' sütunu en sonda olması için veriyi en sona ekliyoruz
    log_entry_data['mail_icerik'] = email_content
    
    # Sadece bu log kaydı için yeni bir DataFrame oluştur
    new_log_entry = pd.DataFrame([log_entry_data])
    
    # Kayıt (log) dosyasının zaten var olup olmadığını kontrol et
    if os.path.exists(log_file):
        try:
            # Var olan dosyayı oku
            df = pd.read_excel(log_file)
            # Yeni log kaydını dosyanın sonuna ekle
            df = pd.concat([df, new_log_entry], ignore_index=True)
        except Exception as e:
            print(f"Excel dosyası okunurken hata oluştu: {e}")
            # Okuma başarısız olursa, güncel kayıt ile yeni bir dosya oluştur
            df = new_log_entry
    else:
        # Eğer dosya yoksa, yeni kayıt DataFrame'in ta kendisidir
        df = new_log_entry
        
    # DataFrame'i Excel dosyasına kaydet
    try:
        df.to_excel(log_file, index=False)
    except Exception as e:
        print(f"Excel dosyasına yazılırken hata oluştu: {e}")


# ==========================================
# DİL MODELİ & AYRIŞTIRMA FONKSİYONLARI
# ==========================================

def parse_with_ollama(prompt: str, system_prompt: str, model_name: str):
    """Ollama ile çıkarım yapar ve metni döndürür, aynı zamanda token kullanımını ekrana yazdırır."""
    response = ollama.chat(
        model=model_name,
        messages=[
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': prompt}
        ],
        #keep_alive=0, İşlem biter bitmez modeli RAM/VRAM'den boşaltır (performans sorunu yaşamamak için)
        options={
            "temperature": 0.0,
            "num_predict": 1024,  # Modelin üretebileceği MAKSİMUM ÇIKTI (token) sınırı. Düşük olursa uretilen JSON yarım kalır.
            "num_ctx": 2048,      # Modelin hafızası (Girdi + Çıktı toplamı). Düşük olursa model e-postanın başını (prompt kurallarını) unutur.
            "num_thread": 8
        }
    )
    
    # Ollama için token metriklerini çıkar
    prompt_tokens = response.get('prompt_eval_count', 0)
    eval_tokens = response.get('eval_count', 0)
    total_tokens = prompt_tokens + eval_tokens
    
    print(f"\n[TOKEN KULLANIMI - Ollama] Girdi (Prompt): {prompt_tokens} | Çıktı (Response): {eval_tokens} | Toplam: {total_tokens}\n")
    
    return response['message']['content']

def parse_with_google(prompt: str, system_prompt: str, model_name: str, api_key: str):
    """Google Gemini ile çıkarım yapar ve dönen metni ekrana yazdırır."""
    if 'genai' not in globals():
        raise ImportError("google-generativeai kütüphanesi yüklü değil.")
    
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(
        model_name=model_name,
        system_instruction=system_prompt,
        generation_config={"temperature": 0.0}
    )
    response = model.generate_content(prompt)
    
    # Token metriklerini mümkünse almayı dene
    usage_metadata = getattr(response, 'usage_metadata', None)
    if usage_metadata:
        try:
            print(f"\n[TOKEN KULLANIMI - Google] Girdi (Prompt): {usage_metadata.prompt_token_count} | Çıktı (Response): {usage_metadata.candidates_token_count} | Toplam: {usage_metadata.total_token_count}\n")
        except AttributeError:
            pass
            
    return response.text

def extract_multiple_requests(email_content: str) -> list:
    """Verilen mailde çoklu talep olduğu bilindiğinde, tüm rotaları/talepleri bulup JSON dizisi olarak çıkarır."""
    prompt = f"""Aşağıdaki gönderilen e-posta içeriğinde BİRDEN FAZLA farklı navlun talebi (farklı yükleme-boşaltma lokasyonları, farklı araç tipleri vb.) geçmektedir.
GÖREVİN:
E-postayı analiz et ve içindeki TÜM rotaları/talepleri ayrı ayrı çıkar.
SADECE bir JSON ARRAY (Liste) formatında çıktı üret. Listenin her bir elemanı bir rota/talep için aşağıdaki alanlara sahip bir JSON NESNESİ olmalıdır:

## ALAN TANIMLARI

| Alan | Açıklama | Örnek Değerler |
|------|----------|----------------|
| romork_cinsi | Araç/ekipman tipi | "Frigo", "Tente", "Mega", "Kapalı Kasa" |
| yukleme_tipi | Eğer metinde AÇIKÇA (FTL, LTL, LCL, Parsiyel, Komple vb.) belirtilmemişse boş (null) bırak, asla kendin tahmin etme. Eğer geçiyorsa olduğu gibi yaz. | "FTL", "LTL", "LCL", "Parsiyel", null |
| yukleme_sehri | Yükleme şehrini yaz. Semt gibi bilgileri dahil etme, yalnızca şehir bilgisini çıkar. | "Milano" |
| yukleme_ulkesi | Yükleme ülkesi — şehirden çıkar gerekirse | "İtalya" |
| bosaltma_sehri | Boşaltma şehrini yaz. Semt gibi bilgileri dahil etme, yalnızca şehir bilgisini çıkar. | "Uralsk" |
| bosaltma_ulkesi | Boşaltma ülkesi — şehirden çıkar gerekirse | "Kazakistan" |
| tarih | Yükleme tarihi veya haftası (metin olarak) | "15.07.2025", "W28" |
| sicaklik_araligi | Taşıma sıcaklığı (orijinal formatta) | "+2 +8", "-18" |
| adr_sinifi | ADR tehlike sınıfı (yalnızca rakam/kod) | "3", "6.1" |
| gtip_kodlari | GTİP/HS kodları listesi (string olarak) | ["32091000", "32089091"] |
| tonaj | Ağırlık bilgisi (birimle birlikte) | "21 ton", "24.500 kg" |
| notlar | Özel talepler, geçiş kısıtları, ek bilgiler | "Rusya transit" |

Taleplere ortak olan bilgileri (tarih, ağırlık, gönderici notu) çıkardığın her bir JSON nesnesinin içine dahil et.

## ŞEHİR → ÜLKE ÇIKARIM KURALLARI
- Şehir açıkça yazılmış ama ülke yoksa, bilinen coğrafyadan ülkeyi çıkar.
- Emin değilsen null bırak, uydurma.
- Örnekler: Uralsk → Kazakistan, Brugerio → İtalya, Duisburg → Almanya

E-posta:
{email_content}
    """
    system_prompt = "Sen uzman bir lojistik analiz motorusun. Çıktın her zaman YALNIZCA geçerli bir JSON Listesi (Array of objects) olmalıdır."
    
    try:
        if LLM_TYPE.lower() == "online":
            api_key = os.getenv("GOOGLE_API_KEY")
            result_text = parse_with_google(prompt, system_prompt, GOOGLE_MODEL, api_key)
        elif LLM_TYPE.lower() == "offline":
            result_text = parse_with_ollama(prompt, system_prompt, OLLAMA_MODEL)
            
        result_text = result_text.strip()
        
        # Markdown etiketlerini temizle ve JSON ayırma
        if "```json" in result_text:
            result_text = result_text.split("```json")[-1].split("```")[0].strip()
        elif result_text.startswith("```"): 
            result_text = result_text[3:]
        if result_text.endswith("```"): 
            result_text = result_text[:-3]

        start_idx = result_text.find('[')
        end_idx = result_text.rfind(']')
        if start_idx != -1 and end_idx != -1:
            result_text = result_text[start_idx:end_idx+1]
            
        result_text = result_text.replace('\\_', '_')
        
        try:
            import ast
            parsed_list = json.loads(result_text)
        except json.JSONDecodeError:
            try:
                parsed_list = ast.literal_eval(result_text)
            except Exception as e:
                print(f"HATA: Modelden çoklu talep için dönen metin geçerli bir JSON dizisi değil. Metin:\n{result_text}")
                parsed_list = []
                
        if not isinstance(parsed_list, list) or len(parsed_list) == 0:
            print("[HATA] Model JSON listesi (array) döndüremedi. Boş liste veya tekil hatalı satır dönüyor.")
            parsed_list = [{"notlar": "HATA: Otomatik çoklu ayrıştırma başarısız oldu veya liste alınamadı."}]
            
        print(f"\n[BİLGİ] E-postadan toplam {len(parsed_list)} farklı talep çıkarıldı.")
        
        # Temel Post-Processing her bir öğe için
        for parsed_data in parsed_list:
            if not isinstance(parsed_data, dict):
                continue
                
            # Nominatim (OpenStreetMap) Geocoding - Şehir/Semt Düzeltme ve Ülke Tespiti
            yuk_sehir, yuk_ulke = verify_city(parsed_data.get("yukleme_sehri"), parsed_data.get("yukleme_ulkesi"))
            if yuk_sehir: parsed_data["yukleme_sehri"] = yuk_sehir
            if yuk_ulke: parsed_data["yukleme_ulkesi"] = yuk_ulke
            
            bos_sehir, bos_ulke = verify_city(parsed_data.get("bosaltma_sehri"), parsed_data.get("bosaltma_ulkesi"))
            if bos_sehir: parsed_data["bosaltma_sehri"] = bos_sehir
            if bos_ulke: parsed_data["bosaltma_ulkesi"] = bos_ulke
                
            # 1. Römork Cinsi Kontrolü
            romork_val = parsed_data.get("romork_cinsi")
            if not romork_val or str(romork_val).strip().lower() in ["null", "none", "belirtilmemiş", ""]:
                parsed_data["romork_cinsi"] = "Tenteli"
            elif str(romork_val).strip().lower() in ["tente", "tenteli"]:
                parsed_data["romork_cinsi"] = "Tenteli"
                
            romork_cinsi_lower = str(parsed_data.get("romork_cinsi", "")).strip().lower()
            if romork_cinsi_lower in ["45lik konteyner", "swap body", "konteyner şase"]:
                parsed_data["intermodal"] = "Evet"
            else:
                parsed_data["intermodal"] = "Hayır"
                
            yukleme_val = str(parsed_data.get("yukleme_tipi") or "").strip().lower()
            if "ftl" in yukleme_val:
                parsed_data["yukleme_tipi"] = "komple"
            elif any(kw in yukleme_val for kw in ["ltl", "lcl", "parsiyel"]):
                parsed_data["yukleme_tipi"] = "LTL"
                
            yuk_ulk = str(parsed_data.get("yukleme_ulkesi") or "").strip().lower()
            bos_ulk = str(parsed_data.get("bosaltma_ulkesi") or "").strip().lower()
            turkiye_aliases = ["türkiye", "turkiye", "turkey", "tr"]
            is_yuk_tr = any(alias in yuk_ulk for alias in turkiye_aliases)
            is_bos_tr = any(alias in bos_ulk for alias in turkiye_aliases)
            
            if not yuk_ulk or yuk_ulk == "null" or not bos_ulk or bos_ulk == "null":
                parsed_data["is_turu"] = "Belirsiz"
            elif is_yuk_tr and is_bos_tr:
                parsed_data["is_turu"] = "Yurtiçi"
            elif is_yuk_tr and not is_bos_tr:
                parsed_data["is_turu"] = "İhracat"
            elif not is_yuk_tr and is_bos_tr:
                parsed_data["is_turu"] = "İthalat"
            else:
                parsed_data["is_turu"] = "Transit"
                
        return parsed_list

    except Exception as e:
        print(f"[HATA - Çoklu Çıkarım]: {e}")
        return [{"notlar": f"HATA: Beklenmeyen bir istisna oluştu ({str(e)})"}]

def parse_freight_email(email_content: str) -> list:
    """Verilen e-posta metnini okur ve yapılandırılmış JSON formatında DİCT listesi [{}, {}] döndürür."""
    prompt = f"""Aşağıdaki e-postadan lojistik bilgilerini çıkar. SADECE JSON döndür.

## ALAN TANIMLARI

| Alan | Açıklama | Örnek Değerler |
|------|----------|----------------|
| coklu_secenek_var_mi | E-postada BİRDEN FAZLA TEKLİF/ROTA (ayrı güzergahlar) varsa "Evet", yoksa "Hayır" | "Evet", "Hayır" |
| romork_cinsi | Araç/ekipman tipi | "Frigo", "Tente", "Mega", "Kapalı Kasa" |
| yukleme_tipi | Metinde geçen değere göre FTL, LTL, LCL veya parsiyel vb. olduğu gibi yaz. | "FTL", "LTL", "LCL", "Parsiyel" |
| yukleme_sehri | Yükleme  şehrini yaz. Semt gibi bilgileri dahil etme, yalnızca şehir bilgisini çıkar. | "Milano" |
| yukleme_ulkesi | Yükleme ülkesi — şehirden çıkar gerekirse | "İtalya" |
| bosaltma_sehri | Boşaltma  şehrini yaz. Semt gibi bilgileri dahil etme, yalnızca şehir bilgisini çıkar. | "Uralsk" |
| bosaltma_ulkesi | Boşaltma ülkesi — şehirden çıkar gerekirse | "Kazakistan" |
| tarih | Yükleme tarihi veya haftası (metin olarak) | "15.07.2025", "W28" |
| sicaklik_araligi | Taşıma sıcaklığı (orijinal formatta) | "+2 +8", "-18" |
| adr_sinifi | ADR tehlike sınıfı (yalnızca rakam/kod) | "3", "6.1" |
| gtip_kodlari | GTİP/HS kodları listesi (string olarak) | ["32091000", "32089091"] |
| tonaj | Ağırlık bilgisi (birimle birlikte) | "21 ton", "24.500 kg" |
| notlar | Özel talepler, geçiş kısıtları, ek bilgiler | "Rusya transit" |

## ŞEHİR → ÜLKE ÇIKARIM KURALLARI
- Şehir açıkça yazılmış ama ülke yoksa, bilinen coğrafyadan ülkeyi çıkar.
- Emin değilsen null bırak, uydurma.
- Örnekler: Uralsk → Kazakistan, Brugerio → İtalya, Duisburg → Almanya

### Coğrafi Kod Çözme
Lokasyon bilgisi şu formatlarda gelebilir — her durumda şehir ve ülkeyi çöz:

| Format | Örnek | Çözüm |
|--------|-------|-------|
| ISO Ülke-Posta Kodu | IT-30100 | Şehir: Venedik, Ülke: İtalya |
| ISO Ülke-Bölge Kodu | FR-49 | Şehir: null, Ülke: Fransa |
| Sadece ülke kodu | DE, PL, RO | Ülkeye çevir, şehir null |
| LOCODE | DEHAM | Hamburg, Almanya |
| Serbest şehir adı | Uralsk | Ülkeyi coğrafyadan çıkar → Kazakistan |

Emin değilsen null bırak, asla uydurma.

## ÖRNEK (One-Shot)

Metin:
```
Konu: SB972/ FCA Brugerio (Italy) - Uralsk / FTL +5+10

Merhaba Yağmur hanım,
FCA Brugerio – Uralsk 
Frigo +5+10 / ADR – 3 sınıf
GTİP - 32091000, 32089091, 32081090 
21 ton / Rusya ile geçebilir / Yük hazır
```

Çıktı:
{{
  "coklu_secenek_var_mi": "Hayır",
  "romork_cinsi": "Frigo",
  "yukleme_tipi": "FTL",
  "yukleme_sehri": "Brugerio",
  "yukleme_ulkesi": "İtalya",
  "bosaltma_sehri": "Uralsk",
  "bosaltma_ulkesi": "Kazakistan",
  "tarih": null,
  "sicaklik_araligi": "+5 +10",
  "adr_sinifi": "3",
  "gtip_kodlari": ["32091000", "32089091", "32081090"],
  "tonaj": "21 ton",
  "notlar": "Rusya transit geçiş, yük hazır"
}}

## ŞİMDİ AŞAĞIDAKİ E-POSTADAN BİLGİ ÇIKAR:

{email_content}
"""
    system_prompt = """
Sen lojistik/nakliye e-postalarından yapılandırılmış JSON çıkaran uzman bir motorusun.

ÇIKTI KURALLARI:
- YALNIZCA geçerli JSON döndür. Markdown, açıklama, yorum yasak.
- Bilgi yoksa: null
- Veri uydurma. Belirsizse null tercih et.
- JSON anahtar adlarını değiştirme.
"""
    
    try:
        if LLM_TYPE.lower() == "online":
            api_key = os.getenv("GOOGLE_API_KEY")
            if not api_key:
                print("HATA: GOOGLE_API_KEY bulunamadı.")
                return None
            print(f"[*] Google modeli ({GOOGLE_MODEL}) ile analiz ediliyor [ONLINE]...")
            result_text = parse_with_google(prompt, system_prompt, GOOGLE_MODEL, api_key)
        elif LLM_TYPE.lower() == "offline":
            print(f"[*] Ollama modeli ({OLLAMA_MODEL}) ile analiz ediliyor [OFFLINE]...")
            result_text = parse_with_ollama(prompt, system_prompt, OLLAMA_MODEL)
        else:
            print(f"HATA: Geçersiz LLM_TYPE '{LLM_TYPE}'.")
            return None

        result_text = result_text.strip()
        if result_text.startswith("```json"): result_text = result_text[7:]
        if result_text.startswith("```"): result_text = result_text[3:]
        if result_text.endswith("```"): result_text = result_text[:-3]
        result_text = result_text.strip()
        
        try:
            parsed_data = json.loads(result_text)
            if not isinstance(parsed_data, dict):
                parsed_data = {}
        except json.JSONDecodeError:
            print("HATA: Modelden dönen metin geçerli bir JSON değil.\nMetin:", result_text)
            parsed_data = {}
        
        # Eğer dict tamamen boş ise veya kritik veri parse edilemediyse hata notu düşelim
        if not parsed_data:
            parsed_data["notlar"] = "HATA: Otomatik ayrıştırma başarısız oldu veya LLM geçerli bir çıktı veremedi."
        
        # --- ÇOKLU TALEP (CONDITIONAL ROUTING) KONTROLÜ ---
        coklu_mu = str(parsed_data.get("coklu_secenek_var_mi", "Hayır")).strip().lower()
        if coklu_mu in ["evet", "x", "yes", "true", "1"]:
            print("\n[BİLGİ] Model e-postada ÇOKLU TALEP tespit etti. İkinci aşama (Çoklu Çıkarım) başlatılıyor...")
            return extract_multiple_requests(email_content)
            
        # Excel'e yazılmadan önce gereksiz anahtarı temizle
        parsed_data.pop("coklu_secenek_var_mi", None)
        
        # --- KONTROL / POST-PROCESSING ADIMI ---
        
        # Nominatim (OpenStreetMap) Geocoding - Şehir/Semt Düzeltme ve Ülke Tespiti
        yuk_sehir, yuk_ulke = verify_city(parsed_data.get("yukleme_sehri"), parsed_data.get("yukleme_ulkesi"))
        if yuk_sehir: parsed_data["yukleme_sehri"] = yuk_sehir
        if yuk_ulke: parsed_data["yukleme_ulkesi"] = yuk_ulke
            
        bos_sehir, bos_ulke = verify_city(parsed_data.get("bosaltma_sehri"), parsed_data.get("bosaltma_ulkesi"))
        if bos_sehir: parsed_data["bosaltma_sehri"] = bos_sehir
        if bos_ulke: parsed_data["bosaltma_ulkesi"] = bos_ulke
        
        # 1. Römork Cinsi Kontrolü
        # Eğer römork cinsi gelmemişse (null, empty, belirtilmemiş vb.) her zaman 'tente' yap.
        romork_val = parsed_data.get("romork_cinsi")
        if not romork_val or str(romork_val).strip().lower() in ["null", "none", "belirtilmemiş", ""]:
            parsed_data["romork_cinsi"] = "Tenteli"
        elif str(romork_val).strip().lower() in ["tente", "tenteli"]:
            parsed_data["romork_cinsi"] = "Tenteli"
            
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
            
        return [parsed_data]

    except Exception as e:
        print("Beklenmeyen bir hata oluştu:", str(e))
        return [{
            "notlar": f"HATA: Beklenmeyen bir istisna oluştu ({str(e)})",
            "romork_cinsi": "tenteli",
            "is_turu": "Belirsiz",
            "intermodal": "Hayır",
            "yukleme_tipi": "Belirsiz"
        }]


# ==========================================
# VERİTABANI (MSSQL) FONKSİYONLARI
# ==========================================

def get_connection(db=None):
    """MSSQL bağlantısı sağlar."""
    conn_str = f"DRIVER={{SQL Server}};SERVER={SERVER_NAME};Trusted_Connection=yes;"
    if db:
        conn_str += f"DATABASE={db};"
    return pyodbc.connect(conn_str)

def init_db():
    """Veritabanını ve tablolarını oluşturur."""
    conn = get_connection()
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"SELECT DB_ID('{DB_NAME}')")
    if not cursor.fetchone()[0]:
        cursor.execute(f"CREATE DATABASE {DB_NAME}")
        print(f"[{DB_NAME}] Veritabanı MSSQL üzerinde başarıyla oluşturuldu.")
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
    """Treyler ismine göre Veritabanından Treyler ID'sini getirir."""
    if not trailer_name or trailer_name.strip().lower() in ["belirtilmemiş", "not specified", "null"]:
        return None
    conn = get_connection(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('SELECT id FROM romork_tipleri WHERE isim = ?', (trailer_name.strip().title(),))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None

def insert_freight_request(parsed_data: dict):
    """Ayrıştırılmış (LLM) verilerini MSSQL'e kaydeder."""
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
    
    print(f"[*] Talep MSSQL veritabanına başarıyla eklendi! (Talep ID: {inserted_id}, Treyler ID: {trailer_id})")