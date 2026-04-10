import pandas as pd
import numpy as np

def clean_text(val):
    """Metinleri karşılaştırmak için normalize eder (boşlukları siler, küçük harfe çevirir)."""
    if pd.isna(val) or val is None:
        return ""
    return str(val).strip().lower()

def evaluate_predictions(pred_path, truth_path, merge_col='mail_icerik'):
    print(f"[{pred_path}] ve [{truth_path}] dosyaları yükleniyor...\n")
    
    try:
        # Excel dosyalarını oku
        df_pred = pd.read_excel(pred_path)
        df_truth = pd.read_excel(truth_path)
    except Exception as e:
        print(f"Dosya okuma hatası: {e}")
        return
        
    # Eşleştirme yapacağımız sütunun varlığını kontrol et
    if merge_col not in df_pred.columns or merge_col not in df_truth.columns:
        print(f"HATA: Eşleştirme (anahtar) sütunu '{merge_col}' dosyalardan en az birinde bulunamadı.")
        print("1. Dosya Sütunları:", list(df_pred.columns))
        print("2. Dosya Sütunları:", list(df_truth.columns))
        return
        
    # Eşleştirme hatalarının önüne geçmek için merge sütununu temizle
    df_pred[merge_col] = df_pred[merge_col].astype(str).str.strip()
    df_truth[merge_col] = df_truth[merge_col].astype(str).str.strip()
    
    # İki veri setini mail içeriği bazında birleştir (Çoklu satırlar için özel eşleştirme algoritması)
    matched_rows = []
    
    unique_mails = df_truth[merge_col].dropna().unique()
    
    for mail_id in unique_mails:
        # Bu maile ait gerçek ve tahmin edilen satırları al
        group_truth = df_truth[df_truth[merge_col] == mail_id]
        group_pred = df_pred[df_pred[merge_col] == mail_id]
        
        t_rows = group_truth.to_dict('records')
        p_rows = group_pred.to_dict('records')
        
        # Birebir eşleşme (Tek talep)
        if len(t_rows) == 1 and len(p_rows) == 1:
            row = {**{f"{k}_gercek": v for k,v in t_rows[0].items()}, **{f"{k}_tahmin": v for k,v in p_rows[0].items()}}
            matched_rows.append(row)
        else:
            # ÇOKLU EŞLEŞTİRME (Aynı mailde birden fazla rota varsa)
            # Rota uyuşmazlığının (Cartesian Product) önüne geçmek için 
            # yükleme-boşaltma lokasyonlarını karşılaştırarak "En iyi Yeri" buluyoruz.
            used_preds = set()
            for t_idx, t_row in enumerate(t_rows):
                best_p_idx = -1
                best_score = -1
                
                for p_idx, p_row in enumerate(p_rows):
                    if p_idx in used_preds:
                        continue
                    
                    # Şehir ve Ülke eşleşmelerine göre benzerlik puanı hesapla
                    score = 0
                    for loc_col in ['yukleme_sehri', 'yukleme_ulkesi', 'bosaltma_sehri', 'bosaltma_ulkesi', 'tonaj']:
                        if loc_col in t_row and loc_col in p_row:
                            val_t = clean_text(t_row[loc_col])
                            val_p = clean_text(p_row[loc_col])
                            if val_t == val_p and val_t != "":
                                score += 1
                                
                    if score > best_score:
                        best_score = score
                        best_p_idx = p_idx
                
                # En çok örtüşen tahmini bu gerçek satırla eşleştir
                if best_p_idx != -1:
                    used_preds.add(best_p_idx)
                    row = {**{f"{k}_gercek": v for k,v in t_row.items()}, **{f"{k}_tahmin": v for k,v in p_rows[best_p_idx].items()}}
                    matched_rows.append(row)
                else:
                    # Model bu rota için bir satır çıkaramamış (Eksik Tahmin)
                    row = {**{f"{k}_gercek": v for k,v in t_row.items()}}
                    keys_to_null = p_rows[0].keys() if p_rows else df_pred.columns
                    for k in keys_to_null:
                        row[f"{k}_tahmin"] = None
                    matched_rows.append(row)
                    
    merged_df = pd.DataFrame(matched_rows)
    
    print(f"Toplam değerlendirilen talep/rota sayısı: {len(merged_df)}\n")
    
    if len(merged_df) == 0:
        print("Eşleşen satır bulunamadığı için değerlendirme yapılamıyor. Lütfen eşleştirme sütunundaki (mail_icerik) verilerin birebir aynı olduğundan emin olun.")
        return

    # Değerlendirilecek temel lojistik veri sütunları
    columns_to_evaluate = [
        'romork_cinsi', 'yukleme_tipi', 'yukleme_sehri', 'yukleme_ulkesi',
        'bosaltma_sehri', 'bosaltma_ulkesi','intermodal',
        'is_turu'
    ]

    """
    columns_to_evaluate = [
        'romork_cinsi', 'yukleme_tipi', 'yukleme_sehri', 'yukleme_ulkesi',
        'bosaltma_sehri', 'bosaltma_ulkesi', 'tarih', 'intermodal',
        'is_turu', 'sicaklik_araligi', 'adr_sinifi', 'gtip_kodlari', 'tonaj'
    ]
    """
    
    results = []
    
    # Bütün satırların her bir sütun için tamamen doğru olup olmadığını tutacak seri
    all_correct = pd.Series([True] * len(merged_df), index=merged_df.index)
    
    for col in columns_to_evaluate:
        col_truth = f"{col}_gercek"
        col_pred = f"{col}_tahmin"
        
        if col_truth in merged_df.columns and col_pred in merged_df.columns:
            # Değerleri temizle (küçük/büyük harf veya boşluk duyarlılığını kaldırmak için)
            truth_vals = merged_df[col_truth].apply(clean_text)
            pred_vals = merged_df[col_pred].apply(clean_text)
            
            # Karşılaştırma yap
            is_match_series = (truth_vals == pred_vals)
            all_correct = all_correct & is_match_series
            
            matches = is_match_series.sum()
            total = len(merged_df)
            accuracy = (matches / total) * 100 if total > 0 else 0
            
            results.append({
                'Sütun / Değişken': col,
                'Doğru Tahmin': matches,
                'Toplam Satır': total,
                'Başarı Oranı (%)': round(accuracy, 2)
            })
            
        else:
            print(f"Uyarı: '{col}' sütunu dosyalarda tam olarak bulunamadı. Atlanıyor.")
            
    # Hata Analizi için satır bazında hataları topla (Böylece aynı maile ait tüm hatalar peş peşe gelir)
    diff_records = []
    for _, row in merged_df.iterrows():
        for col in columns_to_evaluate:
            col_truth = f"{col}_gercek"
            col_pred = f"{col}_tahmin"
            if col_truth in merged_df.columns and col_pred in merged_df.columns:
                truth_val = clean_text(row[col_truth])
                pred_val = clean_text(row[col_pred])
                if truth_val != pred_val:
                    diff_records.append({
                        'Mail Referansı': row[f"{merge_col}_gercek"],
                        'Hatalı Sütun': col,
                        'Gerçek (Beklenen) Değer': row[col_truth],
                        'Modelin Tahmini': row[col_pred]
                    })
            
    # Genel Başarı Raporu
    results_df = pd.DataFrame(results)
    print("=== SÜTUN BAZLI MODEL BAŞARI ORANLARI ===\n")
    print(results_df.to_string(index=False))
    print("\n=========================================")
    
    # Tüm detaylı karşılaştırmaları yan yana görmek için tabloyu düzenle
    import numpy as np
    merged_df['Doğru mu?'] = np.where(all_correct, 'X', '')
    comparison_cols = ['Doğru mu?', f"{merge_col}_gercek"]
    
    for col in columns_to_evaluate:
        if f"{col}_gercek" in merged_df.columns and f"{col}_tahmin" in merged_df.columns:
            comparison_cols.extend([f"{col}_gercek", f"{col}_tahmin"])
    
    # Diğer (kalan) sütunları sona ekle
    for col in merged_df.columns:
        if col not in comparison_cols:
            comparison_cols.append(col)
            
    detailed_df = merged_df[comparison_cols]
    
    # Raporları ayrıştır ve excele yaz
    writer = pd.ExcelWriter("degerlendirme_raporu.xlsx", engine='openpyxl')
    results_df.to_excel(writer, sheet_name="Genel Başarı", index=False)
    
    # Tüm karşılaştırmaların olduğu detay sayfasını ekle
    detailed_df.to_excel(writer, sheet_name="Tüm Karşılaştırmalar", index=False)
    
    if diff_records:
        errors_df = pd.DataFrame(diff_records)
        errors_df.to_excel(writer, sheet_name="Hata Analizi", index=False)
        print(f"\nModelin yanlış tahmin ettiği toplam {len(diff_records)} nokta 'Hata Analizi' sayfasına aktarıldı.")
        
    writer.close()
    print("Detaylı rapor 'degerlendirme_raporu.xlsx' dosyasına kaydedildi.")

if __name__ == "__main__":
    print("--- Model Değerlendirme Aracı ---")
    
    # KULLANIM: Buradaki dosya isimlerini kendi excel dosyalarınızın isimleriyle değiştirin.
    
    # 1. Modelin ürettiği günlük log dosyasının adı
    FILE_PREDICTIONS = "log_08-04-2026.xlsx"   # Örnek: "log_03-04-2026.xlsx"
    
    # 2. Sizin manuel hazırladığınız %100 doğru (Ground Truth) dosyasının adı
    FILE_GROUND_TRUTH = "ground_truth.xlsx"    # Kendi dosyanızın ismini yazın
    
    # Sütun bazlı eşleştirme yapacağımız alan 
    # (Ortak anahtar, isteğinize göre 'mail_icerik' veya 'Mail Konusu' kullanılabilir)
    MERGE_COLUMN = "mail_icerik"
    
    # Fonksiyonu çalıştır:
    evaluate_predictions(FILE_PREDICTIONS, FILE_GROUND_TRUTH, merge_col=MERGE_COLUMN)
    print(f"\nDosya isimlerini {__file__} içinde ayarladıktan sonra yoruma alınmış 'evaluate_predictions' satırını açıp çalıştırın.")
