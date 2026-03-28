from flask import Flask, jsonify, request
from helpful_functions import parse_freight_email, log_to_excel, clean_email_body
import os
import win32com.client
from datetime import datetime
import pythoncom
import time
import threading

app = Flask(__name__)

def process_new_emails():
    """Outlook'tan SADECE YENİ (okunmamış) e-postaları okur, işler ve okundu olarak işaretler."""
    # COM nesneleri için gerekli
    pythoncom.CoInitialize()
    processed_count = 0
    
    try:
        outlook = win32com.client.Dispatch("Outlook.Application")
        namespace = outlook.GetNamespace("MAPI")
        
        # Hedef klasörü (genAI) bulma işlemi
        target_folder = None
        inbox = namespace.GetDefaultFolder(6)  # 6 = olFolderInbox
        
        try:
            # 1. Önce Gelen Kutusu (Inbox) altında alt klasör mü diye kontrol et
            target_folder = inbox.Folders.Item("genAI")
        except:
            # 2. Inbox'ta değilse kök klasörlerin (hesapların) hemen altında mı diye bak
            for folder in namespace.Folders:
                try:
                    target_folder = folder.Folders.Item("genAI")
                    if target_folder:
                        break
                except:
                    continue

        if not target_folder:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] HATA: 'genAI' adında bir klasör bulunamadı!")
            return 0

        # Yalnızca klasördeki okunmamış (yeni) mailleri kısıtla
        messages = target_folder.Items.Restrict("[UnRead] = True")
        messages.Sort("[ReceivedTime]", True)
        
        if messages.Count > 0:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {messages.Count} yeni e-posta bulundu, işleniyor...")
            
            for msg in messages:
                try:
                    body = msg.Body
                    if not body or not body.strip():
                        msg.UnRead = False
                        msg.Save()
                        continue
                        
                    # Metin temizleme ve imza ayırma
                    cleaned_content = clean_email_body(body)
                    
                    # Modeli çağır ve ayrıştır
                    parsed_data = parse_freight_email(cleaned_content)
                    
                    # Eğer ayrıştırma başarılı olduysa, gönderici bilgilerini ekle
                    if parsed_data is not None:
                        sender_name = getattr(msg, 'SenderName', 'Bilinmeyen Gönderici')
                        sender_email = getattr(msg, 'SenderEmailAddress', '')
                        
                        # Bazen Outlook içi (Exchange) sunucularında mail adresi X500 formatında ("/O=...") dönebilir.
                        # Bu durumda doğrudan SMTP mail adresini almayı deneriz.
                        if sender_email and "@" not in sender_email:
                            try:
                                sender_email = msg.Sender.GetExchangeUser().PrimarySmtpAddress
                            except:
                                pass
                                
                        # Mail uzantısından şirket adını çıkar (örn: hbibicik@stantelogistics.com -> Stantelogistics)
                        company_name = "Bilinmeyen"
                        if sender_email and "@" in sender_email:
                            domain_part = sender_email.split('@')[1]           # stantelogistics.com
                            company_name = domain_part.split('.')[0]           # stantelogistics
                            company_name = company_name.capitalize()           # Stantelogistics
                            
                        parsed_data['Musteri'] = company_name
                        parsed_data['Mail Gonderen'] = sender_name
                    
                    # Excel'e logla
                    log_to_excel(body, parsed_data)
                    
                    # Mail başarıyla işlendiği için okundu olarak işaretle
                    msg.UnRead = False
                    msg.Save()
                    
                    processed_count += 1
                except Exception as e:
                    print(f"Tekil e-posta okunurken hata: {e}")
                    continue
        return processed_count
    except Exception as e:
        print(f"Outlook bağlantı hatası: {e}")
        return 0
    finally:
        pythoncom.CoUninitialize()

@app.route("/api/check_now", methods=["GET"])
def check_now_endpoint():
    """İstenildiği zaman manuel tetiklemek veya servis sağlığını görmek için endpoint."""
    count = process_new_emails()
    if count > 0:
        return jsonify({"status": "success", "message": f"{count} adet yeni mail işlendi ve Excel'e kaydedildi."}), 200
    else:
        return jsonify({"status": "success", "message": "Yeni / okunmamış mail bulunamadı."}), 200

@app.route("/api/parse_manual", methods=["POST"])
def parse_manual_endpoint():
    """Dışarıdan POST isteğiyle gelen bir mail metnini manuel olarak işler."""
    raw_body = ""
    musteri = "API İsteği (Postman)"
    gonderen = "API Kullanıcısı"

    # 1. Eğer düzgün bir JSON gönderildiyse:
    data = request.get_json(silent=True)
    if data and isinstance(data, dict) and "email_content" in data:
        raw_body = data["email_content"]
        musteri = data.get("Musteri", musteri)
        gonderen = data.get("Mail Gonderen", gonderen)
    # 2. Eğer form verisi olarak geldiyse:
    elif "email_content" in request.form:
        raw_body = request.form["email_content"]
    # 3. Eğer JSON bozuksa (alt alta yazılmış hatalı bir string) veya RAW TEXT olarak atıldıysa:
    else:
        raw_body = request.get_data(as_text=True)
    
    if not raw_body or not raw_body.strip():
        return jsonify({"status": "error", "message": "Email içeriği boş olamaz veya format anlaşılamadı."}), 400
    
    try:
        # 1. Metni temizle ve imza vb. ayır
        cleaned_content = clean_email_body(raw_body)
        
        # 2. Modeli çağır ve ayrıştır
        parsed_data = parse_freight_email(cleaned_content)
        
        if parsed_data is not None:
            # 3. İsteğe bağlı olarak Müşteri alanlarını manuel besleyebiliriz
            parsed_data['Musteri'] = musteri
            parsed_data['Mail Gonderen'] = gonderen
            
            # Excel'e logla (orijinal metni loglayalım ki teyit edebilelim)
            log_to_excel(raw_body, parsed_data)
            
            return jsonify({
                "status": "success", 
                "message": "Mail başarıyla işlendi ve Excel'e kaydedildi.",
                "parsed_data": parsed_data
            }), 200
        else:
            return jsonify({"status": "error", "message": "Model yanıtı ayrıştıramadı veya beklendiği gibi bir sonuç dönmedi."}), 500
            
    except Exception as e:
        return jsonify({"status": "error", "message": f"İşlem sırasında hata oluştu: {str(e)}"}), 500

def run_loop():
    """Basit bir sonsuz döngü ile mailleri kontrol eder. Thread içinde çalışacak."""
    print("Sürekli mail kontrol servisi arka planda başlatıldı (60 saniyede bir çalışacak).")
    while True:
        try:
            process_new_emails()
        except Exception as e:
            print(f"Döngü hatası: {e}")
        
        # 60 saniye bekle
        time.sleep(60)

if __name__ == "__main__":
    # 1. Arka planda çalışacak mail dinleme döngüsünü (Thread) başlatıyoruz
    # daemon=True sayesinde ana program kapandığında thread de kapanır
    mail_thread = threading.Thread(target=run_loop, daemon=True)
    mail_thread.start()
    
    # 2. Flask sunucusunu başlatıyoruz.
    # Flask ana thread'i bloke ederek endpointleri dinler, arka plandaki mail_thread ise bağımsız çalışmaya devam eder.
    print("API Sunucusu http://localhost:5000/ üzerinde başlatılıyor...")
    print("Manuel denetim için GET isteği atabilirsiniz: http://localhost:5000/api/check_now")
    app.run(host="0.0.0.0", port=5000, debug=False)