import json
from helpful_functions import parse_freight_email

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