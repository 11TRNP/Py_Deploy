from database import SessionLocal
from sqlalchemy import text


def normalize_date(value):
    if not value or value in ("NOT FOUND", ""):
        return None
    return value

def get_global_value(certificates, field):
    for cert in certificates:
        val = cert.get("extracted_fields", {}).get(field)
        if val and val not in ("NOT FOUND", ""):
            return val
        return None
    
def get_fallback(primary, fallback):
    if primary and primary not in ("NOT FOUND", ""):
        return primary
    return fallback

def save_parsing_result(result: dict):
    db = SessionLocal()

    try:
        certificates = result.get("metadata", {}).get("certificates", [])

        if not certificates:
            raise ValueError("No certificates found")
        
        global_tgl_sert = get_global_value(certificates, "tgl_sert")
        global_tgl_berlaku = get_global_value(certificates, "tgl_berlaku")
        global_tgl_survey1 = get_global_value(certificates, "tgl_survey1")
        global_tgl_survey2 = get_global_value(certificates, "tgl_survey2")


        sql = text("""
            INSERT INTO dbo.ocr_parse_certificate (
                nosert,
                noreg,
                nmkpl,
                jenis_sert1,
                jenis_survey,
                divisi,
                lokasi_survey,
                mem01,
                tgl_sert,
                tgl_berlaku,
                tgl_survey1,
                tgl_survey2
            ) VALUES (
                :nosert,
                :noreg,
                :nmkpl,
                :jenis_sert1,
                :jenis_survey,
                :divisi,
                :lokasi_survey,
                :mem01,
                :tgl_sert,
                :tgl_berlaku,
                :tgl_survey1,
                :tgl_survey2
            )
        """)

        for cert in certificates:
            data = cert.get("extracted_fields", {})

            tgl_sert_final = get_fallback(data.get("tgl_sert"), global_tgl_sert)
            tgl_berlaku_final = get_fallback(data.get("tgl_berlaku"), global_tgl_berlaku)
            tgl_survey1_final = get_fallback(data.get("tgl_survey1"), global_tgl_survey1)
            tgl_survey2_final = get_fallback(data.get("tgl_survey2"), global_tgl_survey2)
            
            
            payload = {
                "nosert": data.get("nosert"),
                "noreg": data.get("noreg"),
                "nmkpl": data.get("nmkpl"),
                "jenis_sert1": data.get("jenis_sert"),
                "jenis_survey": data.get("jenis_survey"),
                "divisi": data.get("divisi"),
                "lokasi_survey": data.get("lokasi_survey"),
                "mem01": data.get("mem01"),
                "tgl_sert": normalize_date(tgl_sert_final),
                "tgl_berlaku": normalize_date(tgl_berlaku_final),
                "tgl_survey1": normalize_date(tgl_survey1_final),
                "tgl_survey2": normalize_date(tgl_survey2_final),
            }

            print("INSERTING:", payload)  

            db.execute(sql, payload)

        db.commit()

    except Exception as e:
        db.rollback()
        raise RuntimeError(f"DB insert failed: {e}")

    finally:
        db.close()