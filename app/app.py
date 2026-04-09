"""
OCR API Service (Flask - JSON only)
"""

import os
import logging
import tempfile
from pathlib import Path
from typing import Union, Optional
from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor

from config_loader import CONFIG, BBOX_CONFIG
from ocr_engine import OCREngine
from text_parser import TextParser
from bbox_template_manager import BBoxTemplateManager
from db_writer import save_parsing_result

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=1)


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


# =========================
# API ENDPOINT
# =========================
@app.route("/ocr", methods=["POST"])
def ocr_api():
    if "file" not in request.files:
        return jsonify({"status": "error", "message": "No file uploaded"}), 400

    file = request.files["file"]

    # save temp file
    temp_dir = tempfile.mkdtemp()
    file_path = os.path.join(temp_dir, file.filename)
    file.save(file_path)

    try:
        ocr_engine = OCREngine(CONFIG)
        text_parser = TextParser(CONFIG)
        bbox_manager = BBoxTemplateManager(BBOX_CONFIG)

        future = executor.submit(
            process_single_document,
            file_path,
            ocr_engine,
            text_parser,
            bbox_manager,
            False
        )

        result = future.result()

        # SAVE TO DB
        if result.get("status") == "success":
            save_parsing_result(result)

        return jsonify(result)

    except Exception as e:
        logger.exception("API error")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


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
    app.run(host="0.0.0.0", port=5002, debug=True)