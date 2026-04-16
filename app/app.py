"""
OCR API Service (Flask - JSON only)
Orchestrates OCR extraction and metadata parsing from images and PDFs
"""

import os
import logging
import tempfile
from pathlib import Path
from typing import Union, Optional
from uuid import uuid4
from functools import wraps

from flask import Flask, request, jsonify
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor

from config_loader import CONFIG, BBOX_CONFIG
from ocr_engine import OCREngine
from text_parser import TextParser
from bbox_template_manager import BBoxTemplateManager
from db_writer import save_parsing_result, get_all_parsing_results

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
executor = ThreadPoolExecutor(max_workers=1)

# Session store for OCR results
OCR_SESSION_STORE = {}

UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


# =========================
# JWT PLACEHOLDER (disabled)
# =========================
def jwt_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        return f(*args, **kwargs)
    return decorated


# =========================
# CORE PIPELINE
# =========================
def process_single_document(
    file_path: Union[str, Path],
    ocr_engine: OCREngine,
    text_parser: TextParser,
    bbox_manager: Optional[BBoxTemplateManager] = None,
    use_bbox_template: bool = False
) -> dict:

    file_path = Path(file_path)

    try:
        # 1. OCR
        ocr_result = ocr_engine.process_document(file_path)

        # 2. PARSE
        metadata = text_parser.parse(ocr_result)

        # Optional bbox
        if use_bbox_template and bbox_manager:
            bbox_metadata = bbox_manager.extract_auto(ocr_result)

            if metadata.get('multiple_certificates', False):
                metadata['certificates'][0]['bbox_extraction'] = bbox_metadata
            else:
                metadata['bbox_extraction'] = bbox_metadata

        # 3. VALIDATION
        if metadata.get('multiple_certificates', False):
            for cert in metadata['certificates']:
                is_valid, errors = text_parser.validate_extraction(cert)
                cert['is_valid'] = is_valid
                cert['validation_errors'] = errors
        else:
            is_valid, errors = text_parser.validate_extraction(metadata)
            metadata['is_valid'] = is_valid
            metadata['validation_errors'] = errors

        return {
            "status": "success",
            "file": str(file_path.name),
            "metadata": metadata
        }

    except Exception as e:
        logger.exception("Processing failed")
        return {
            "status": "error",
            "message": str(e)
        }


def run_pipeline(file_path):
    ocr_engine = OCREngine(CONFIG)
    text_parser = TextParser(CONFIG)
    bbox_manager = BBoxTemplateManager(BBOX_CONFIG)
    return process_single_document(
        file_path=file_path,
        ocr_engine=ocr_engine,
        text_parser=text_parser,
        bbox_manager=bbox_manager,
        use_bbox_template=False
    )


# =========================
# API ENDPOINTS
# =========================

@app.route('/api/certificate-ocr/upload', methods=['POST'])
def upload_file():
    """Upload PDF/image dan jalankan OCR pipeline"""
    # Ambil metadata dari form data
    nup = request.form.get('nup')
    sign_no = request.form.get('sign_no')

    nosert_expected = request.form.get('nosert')
    if nosert_expected:
        nosert_expected = nosert_expected.lstrip('0') or nosert_expected

    if not nup:
        logger.warning("Upload attempt without NUP")

    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "No file uploaded"}), 400

    file = request.files['file']
    temp_dir = tempfile.mkdtemp()
    file_path = os.path.join(temp_dir, file.filename)
    file.save(file_path)

    try:
        future = executor.submit(run_pipeline, file_path)
        result = future.result()  # blocking but safer

        logger.info(f"OCR Result Status: {result.get('status')}")

        if result.get("status") == "success":
            logger.info(f"Attempting to save to database (User NUP: {nup}, Sign No: {sign_no}, Nosert Expected: {nosert_expected})...")
            try:
                save_success = save_parsing_result(result, nup=nup, sign_no=sign_no, nosert_expected=nosert_expected)
                if save_success:
                    logger.info("✓ Data successfully saved to database")
                else:
                    logger.error("✗ Failed to save to database (returned False)")
            except Exception as db_error:
                logger.error(f"✗ Exception while saving to database: {db_error}", exc_info=True)
        else:
            logger.warning(f"Skipping database save - OCR status is not success: {result.get('status')}")

        session_id = str(uuid4())
        OCR_SESSION_STORE[session_id] = {
            "metadata": result.get("metadata"),
            "file": result.get("file")
        }
        result["session_id"] = session_id

        return jsonify(result)

    except Exception as e:
        logger.exception("Upload processing failed")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/certificate-ocr/results', methods=['GET'])
def get_results():
    """Get all parsing results from database"""
    limit = request.args.get('limit', default=10, type=int)
    offset = request.args.get('offset', default=0, type=int)

    try:
        results = get_all_parsing_results(limit=limit, offset=offset)
        return jsonify({
            'status': 'success',
            'count': len(results),
            'results': results
        })
    except Exception as e:
        logger.error(f"Failed to fetch results: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/certificate-ocr/sync', methods=['GET'])
def sync_by_nosert():
    """
    Sync endpoint: cari data OCR berdasarkan nosert.
    Laravel bisa hit endpoint ini untuk ambil tgl_berlaku dll.
    Juga melakukan validasi apakah nosert di PDF cocok dengan nosert di Laravel.

    Contoh: GET /api/certificate-ocr/sync?nosert=32333
    """
    nosert = request.args.get('nosert')

    if not nosert:
        return jsonify({'status': 'error', 'message': 'Parameter nosert wajib diisi'}), 400

    nosert_clean = nosert.lstrip('0') or nosert

    try:
        from database import engine
        from sqlalchemy import text as sql_text

        with engine.connect() as conn:
            result = conn.execute(sql_text("""
                SELECT nosert, nosert_ocr, nosert_expected,
                       noreg, nmkpl, jenis_sert, jenis_survey,
                       tgl_sert, tgl_berlaku, tgl_survey1, tgl_survey2,
                       divisi, lokasi_survey, nup, sign_no, created_at
                FROM public.parsing_results
                WHERE nosert_expected = :nosert
                   OR nosert = :nosert
                ORDER BY created_at DESC
                LIMIT 1
            """), {"nosert": nosert_clean}).fetchone()

        if result:
            data = dict(result._mapping)
            for key, val in data.items():
                if hasattr(val, 'isoformat'):
                    data[key] = val.isoformat()

            nosert_ocr_raw      = data.get('nosert_ocr')
            nosert_expected_raw = data.get('nosert_expected')

            nosert_ocr_clean      = nosert_ocr_raw.lstrip('0') if nosert_ocr_raw else None
            nosert_expected_clean = nosert_expected_raw.lstrip('0') if nosert_expected_raw else None

            # Tentukan hasil validasi
            if nosert_expected_clean is None:
                nosert_match = None
                validation_status = "skipped"
                validation_message = (
                    "Validasi tidak dapat dilakukan: parameter 'nosert' tidak dikirim "
                    "saat upload. Pastikan Laravel mengirim field 'nosert' pada form upload."
                )
                logger.warning(f"[SYNC] nosert_expected NULL untuk record nosert_query={nosert_clean}. Validasi dilewati.")
            elif nosert_ocr_clean is None:
                nosert_match = None
                validation_status = "skipped"
                validation_message = (
                    "Validasi tidak dapat dilakukan: data ini diupload sebelum fitur validasi diterapkan. "
                    "Silakan upload ulang PDF untuk mendapatkan validasi."
                )
                logger.warning(f"[SYNC] nosert_ocr NULL untuk record nosert_expected={nosert_expected_clean}. Data lama.")
            elif nosert_ocr_clean == nosert_expected_clean:
                nosert_match = True
                validation_status = "match"
                validation_message = "Nomor sertifikat pada PDF sesuai dengan data di sistem."
            else:
                nosert_match = False
                validation_status = "mismatch"
                validation_message = (
                    f"PERINGATAN! Nomor sertifikat pada PDF yang diupload ({nosert_ocr_clean}) "
                    f"BERBEDA dengan nomor sertifikat di sistem ({nosert_expected_clean}). "
                    f"PDF yang diupload tidak sesuai!"
                )
                logger.warning(
                    f"[SYNC MISMATCH] nosert_expected={nosert_expected_clean} "
                    f"!= nosert_ocr={nosert_ocr_clean}"
                )

            validation = {
                "nosert_match": nosert_match,
                "validation_status": validation_status,
                "nosert_expected": nosert_expected_clean,
                "nosert_ocr": nosert_ocr_clean,
                "message": validation_message,
            }

            return jsonify({
                'status': 'success',
                'found': True,
                'validation': validation,
                'data': data
            })
        else:
            return jsonify({
                'status': 'success',
                'found': False,
                'message': f'Data dengan nosert {nosert_clean} tidak ditemukan'
            })

    except Exception as e:
        logger.error(f"Failed to sync nosert {nosert}: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


# =========================
# HEALTH CHECK
# =========================
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


# =========================
# RUN APP
# =========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)