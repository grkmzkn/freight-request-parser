import json
from helpful_functions import parse_freight_email, init_db, insert_freight_request

def process_email(email_content: str):
    """
    Receives incoming email text, parses it using the LLM function from helpful_functions,
    and saves the returned JSON data to the MSSQL database.
    """
    print("\n--- Processing Incoming Email ---")
    print(f"Content Length: {len(email_content)} characters")
    
    # 1. Parse Email with LLM
    extracted_data = parse_freight_email(email_content)
    
    if extracted_data:
        print("\n[SUCCESS] LLM Data Extraction Completed:")
        print(json.dumps(extracted_data, indent=4, ensure_ascii=False))
        
        # 2. Save Extracted Data to Database
        print("\n--- Starting Database Registration Process ---")
        insert_freight_request(extracted_data)
    else:
        print("\n[ERROR] Email could not be parsed. Database registration skipped.")

if __name__ == "__main__":
    # First, make sure the database and tables are properly configured
    print("System Starting...\nChecking database...")
    init_db()
    
    # Sample freight email for manual testing purposes
    sample_email = """
    Konu: Acil Nakliye Talebi / FCA Brugerio (Italy) - Uralsk / FTL +5+10
    Merhaba
    
    Aşağıdaki detaylara uygun araç tedariki rica ederiz:
    
    FCA Brugerio - Uralsk
    Frigo +5+10 derece
    ADR-3 sınıf olacak
    GTİP - 32091000, 32089091
    21 ton
    Yarın yüklemeye hazır.
    Rusya üzerinden geçebilir.

    Saygılarımla / Best regards
    """
    
    # Start the process
    process_email(sample_email)
