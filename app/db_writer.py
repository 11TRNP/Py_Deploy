"""
Database Writer
Handles writing OCR extraction results to the database
Supports both single and multiple certificates
"""

import logging
import json
import os
import hashlib
from datetime import datetime
from typing import Dict, Any
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create database engine and session
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'ocr_db')
DB_USERNAME = os.getenv('DB_USERNAME', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

DATABASE_URL = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def save_parsing_result(result: Dict[str, Any], nup: str = None, sign_no: str = None, nosert_expected: str = None) -> bool:
    """
    Save OCR parsing result to database
    Handles both single and multiple certificates
    
    Args:
        result: OCR processing result dictionary containing:
            - status: 'success' or 'error'
            - file: path to the processed file
            - metadata: extracted metadata (may contain certificates array)
            - validation: validation results
        nup: The NUP of the user who uploaded the document (optional)
        sign_no: The agenda/sign number for the session (optional)
        nosert_expected: Nosert yang dikirim Laravel saat upload (untuk validasi mismatch)
            
    Returns:
        True if successful, False otherwise
    """
    if result.get('status') != 'success':
        logger.warning("Cannot save result with non-success status")
        return False
    
    try:
        # Extract file information
        file_path = result.get('file', '')
        filename = os.path.basename(file_path) if file_path else None
        
        # Get metadata
        metadata = result.get('metadata', {})
        
        # Check if this is multiple certificates
        certificates = metadata.get('certificates', [])
        multiple_certs = metadata.get('multiple_certificates', False)
        
        if multiple_certs and certificates:
            # Handle multiple certificates - save each one separately
            logger.info(f"Processing {len(certificates)} certificates from {filename}")
            success_count = 0
            
            for idx, cert in enumerate(certificates):
                try:
                    if _save_single_certificate(cert, file_path, filename, idx + 1, len(certificates), nup=nup, sign_no=sign_no, nosert_expected=nosert_expected):
                        success_count += 1
                except Exception as e:
                    logger.error(f"Failed to save certificate {idx + 1}: {e}")
            
            logger.info(f"Successfully saved {success_count}/{len(certificates)} certificates")
            return success_count > 0
        else:
            # Handle single certificate (old format)
            return _save_single_certificate(metadata, file_path, filename, 1, 1, nup=nup, sign_no=sign_no, nosert_expected=nosert_expected)
        
    except Exception as e:
        logger.error(f"Failed to save parsing result: {e}", exc_info=True)
        return False


def _save_single_certificate(cert_data: Dict[str, Any], file_path: str, filename: str, 
                             cert_num: int, total_certs: int, nup: str = None, sign_no: str = None,
                             nosert_expected: str = None) -> bool:
    """
    Save a single certificate to database
    
    Args:
        cert_data: Certificate data dictionary
        file_path: Path to the PDF file
        filename: Name of the PDF file
        cert_num: Certificate number (1-indexed)
        total_certs: Total number of certificates in the file
        nup: The NUP of the user who uploaded the document
        sign_no: The agenda/sign number for the session
        nosert_expected: Nosert yang dikirim Laravel (untuk validasi mismatch)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Generate session_id
        timestamp = datetime.now().isoformat()
        session_data = f"{filename}_{timestamp}_{cert_num}"
        session_id = hashlib.md5(session_data.encode()).hexdigest()
        
        # Extract fields from certificate data
        extracted = cert_data.get('extracted_fields', cert_data)  # Fallback to cert_data for old format
        
        logger.info(f"Saving certificate {cert_num}/{total_certs} - session_id: {session_id}")
        logger.info(f"Extracted fields: {list(extracted.keys())}")
        
        # SQL query to insert data
        sql = text("""
            INSERT INTO public.parsing_results (
                nosert,
                nosert_ocr,
                nosert_expected,
                noreg,
                nmkpl,
                jenis_sert,
                jenis_survey,
                divisi,
                lokasi_survey,
                mem01,
                tgl_sert,
                tgl_berlaku,
                tgl_survey1,
                tgl_survey2,
                raw_result,
                nup,
                sign_no,
                created_at,
                updated_at
            ) VALUES (
                :nosert,
                :nosert_ocr,
                :nosert_expected,
                :noreg,
                :nmkpl,
                :jenis_sert,
                :jenis_survey,
                :divisi,
                :lokasi_survey,
                :mem01,
                :tgl_sert,
                :tgl_berlaku,
                :tgl_survey1,
                :tgl_survey2,
                :raw_result,
                :nup,
                :sign_no,
                NOW(),
                NOW()
            )
        """)
        
        # Prepare data - handle both null strings and actual nulls
        def clean_value(val):
            """Convert 'null', 'NOT FOUND', None to actual None"""
            if val in [None, 'null', 'NOT FOUND', 'MISSING_REQUIRED', 'MISSING_OPTIONAL']:
                return None
            return val
        
        def clean_nosert(val):
            """Strip leading zeros from nosert to match Laravel format (e.g. '032333' -> '32333')"""
            cleaned = clean_value(val)
            if cleaned and isinstance(cleaned, str):
                cleaned = cleaned.lstrip('0') or cleaned
            return cleaned
        
        nosert_ocr_val = clean_nosert(extracted.get("nosert"))
        
        nosert_main = nosert_expected if nosert_expected else nosert_ocr_val
        
        if nosert_expected and nosert_ocr_val and nosert_expected != nosert_ocr_val:
            logger.warning(
                f"[NOSERT MISMATCH] nosert_expected={nosert_expected} "
                f"!= nosert_ocr={nosert_ocr_val} | File: {filename}"
            )
        
        data = {
            "nosert": nosert_main,
            "nosert_ocr": nosert_ocr_val,
            "nosert_expected": nosert_expected,
            "noreg": clean_value(extracted.get("noreg")),
            "nmkpl": clean_value(extracted.get("nmkpl")),
            "jenis_sert": clean_value(extracted.get("jenis_sert")),
            "jenis_survey": clean_value(extracted.get("jenis_survey")),
            "divisi": clean_value(extracted.get("divisi")),
            "lokasi_survey": clean_value(extracted.get("lokasi_survey")),
            "mem01": clean_value(extracted.get("mem01")),
            "tgl_sert": clean_value(extracted.get("tgl_sert")),
            "tgl_berlaku": clean_value(extracted.get("tgl_berlaku")),
            "tgl_survey1": clean_value(extracted.get("tgl_survey1")),
            "tgl_survey2": clean_value(extracted.get("tgl_survey2")),
            "raw_result": json.dumps(cert_data, ensure_ascii=False),
            "nup": nup,
            "sign_no": sign_no
        }
        
        logger.info(f"Data to insert: nosert={data['nosert']}, nosert_ocr={data['nosert_ocr']}, nosert_expected={data['nosert_expected']}, nmkpl={data['nmkpl']}")
        
        # Use engine.connect() with explicit transaction
        with engine.connect() as conn:
            with conn.begin():  # This ensures proper commit
                conn.execute(sql, data)
        
        logger.info(f"✓ Successfully saved certificate {cert_num}/{total_certs} (session_id: {session_id})")
        return True
        
    except Exception as e:
        logger.error(f"✗ Failed to save certificate {cert_num}/{total_certs}: {e}", exc_info=True)
        return False


def get_parsing_result_by_session(session_id: str) -> Dict[str, Any]:
    """
    Retrieve parsing result by session ID
    
    Args:
        session_id: Session identifier
        
    Returns:
        Parsing result dictionary or None if not found
    """
    db = SessionLocal()
    
    try:
        sql = text("""
            SELECT * FROM public.parsing_results 
            WHERE session_id = :session_id
        """)
        
        result = db.execute(sql, {"session_id": session_id}).fetchone()
        
        if result:
            return dict(result._mapping)
        return None
        
    except Exception as e:
        logger.error(f"Failed to retrieve parsing result: {e}")
        return None
        
    finally:
        db.close()


def get_all_parsing_results(limit: int = 100, offset: int = 0) -> list:
    """
    Retrieve all parsing results with pagination
    
    Args:
        limit: Maximum number of records to return
        offset: Number of records to skip
        
    Returns:
        List of parsing result records
    """
    db = SessionLocal()
    
    try:
        sql = text("""
            SELECT * FROM public.parsing_results 
            ORDER BY created_at DESC 
            LIMIT :limit OFFSET :offset
        """)
        
        results = db.execute(sql, {"limit": limit, "offset": offset}).fetchall()
        return [dict(row._mapping) for row in results]
        
    except Exception as e:
        logger.error(f"Failed to retrieve parsing results: {e}")
        return []
        
    finally:
        db.close()


def get_parsing_result_by_nosert(nosert: str) -> dict:
    """
    Retrieve the latest parsing result by certificate number (nosert)
    
    Args:
        nosert: Certificate number to search for
        
    Returns:
        Dictionary with nosert, tgl_sert, tgl_berlaku, nmkpl etc., or None if not found
    """
    db = SessionLocal()
    
    try:
        cleaned_nosert = nosert.lstrip('0') if nosert else nosert
        
        sql = text("""
            SELECT nosert, noreg, nmkpl, jenis_sert, jenis_survey,
                   divisi, lokasi_survey, tgl_sert, tgl_berlaku,
                   tgl_survey1, tgl_survey2, nup, sign_no, created_at
            FROM public.parsing_results
            WHERE nosert = :nosert
            ORDER BY created_at DESC
            LIMIT 1
        """)
        
        result = db.execute(sql, {"nosert": cleaned_nosert}).fetchone()
        
        if result:
            row = dict(result._mapping)
            for key, val in row.items():
                if hasattr(val, 'isoformat'):
                    row[key] = val.isoformat()
            return row
        return None
        
    except Exception as e:
        logger.error(f"Failed to retrieve parsing result by nosert '{nosert}': {e}")
        return None
        
    finally:
        db.close()


if __name__ == "__main__":

    # Test database writer
    logging.basicConfig(level=logging.INFO)
    
    # Test data
    test_result = {
        'status': 'success',
        'file': '/path/to/test.pdf',
        'metadata': {
            'nosert': 'TEST-001',
            'noreg': '1234567',
            'nmkpl': 'TEST VESSEL',
            'jenis_sert': 'Safety Certificate',
            'tgl_sert': '2024-01-01',
            'tgl_berlaku': '2025-01-01'
        },
        'validation': {
            'is_valid': True,
            'errors': []
        }
    }
    
    print("Testing database writer...")
    if save_parsing_result(test_result):
        print("Parsing result saved successfully")
    else:
        print("Failed to save parsing result")