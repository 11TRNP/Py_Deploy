"""
BBox Template
Extracts fields from OCR result using bounding box regions and regex patterns.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class BBoxTemplate:
    """
    Template berbasis koordinat bounding box untuk ekstraksi field dari OCR result.

    Setiap template mendefinisikan beberapa 'regions', masing-masing punya:
    - bbox   : {x1, y1, x2, y2} koordinat area di halaman
    - pattern: regex untuk memvalidasi / mengekstrak teks di area tersebut
    """

    def __init__(self, config: dict):
        self.name = config.get('name', 'unknown')
        self.description = config.get('description', '')

        page_size = config.get('page_size', {})
        self.page_width  = page_size.get('width', 1000)
        self.page_height = page_size.get('height', 1400)

        # regions: {field_name: {bbox: {...}, pattern: "...", description: "..."}}
        self.regions: Dict[str, dict] = config.get('regions', {})

        logger.debug(f"BBoxTemplate '{self.name}' loaded with {len(self.regions)} regions")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract_from_ocr(self, ocr_result: dict) -> dict:
        """
        Ekstrak semua field dari ocr_result berdasarkan region bbox + pattern.

        Args:
            ocr_result: dict hasil OCR dengan minimal key 'words'.
                        Setiap word: {text, bbox: {x1, y1, x2, y2}} atau
                                     {text, left, top, width, height}

        Returns:
            {
                "template_name": str,
                "extracted_fields": {field: value},
                "confidence_scores": {field: float 0-1},
            }
        """
        words = ocr_result.get('words', [])
        extracted_fields: Dict[str, Optional[str]] = {}
        confidence_scores: Dict[str, float] = {}

        for field_name, region_cfg in self.regions.items():
            bbox   = region_cfg.get('bbox', {})
            pattern = region_cfg.get('pattern', '')

            # Kumpulkan kata-kata yang jatuh di dalam region
            region_words = self._words_in_region(words, bbox)
            region_text  = ' '.join(w['text'] for w in region_words if w.get('text'))

            # Ekstrak dengan pattern
            value, confidence = self._extract_with_pattern(region_text, pattern)

            extracted_fields[field_name] = value
            confidence_scores[field_name] = confidence

            logger.debug(
                f"[{self.name}] field='{field_name}' "
                f"region_text='{region_text[:60]}' "
                f"value='{value}' conf={confidence:.2f}"
            )

        return {
            "template_name": self.name,
            "extracted_fields": extracted_fields,
            "confidence_scores": confidence_scores,
        }

    def visualize_regions(self, image_path: str, output_path: str):
        """
        Gambar bounding box setiap region di atas gambar dan simpan.
        Membutuhkan pillow (PIL).
        """
        try:
            from PIL import Image, ImageDraw, ImageFont
        except ImportError:
            logger.warning("Pillow tidak terinstall. visualize_regions dilewati.")
            return

        try:
            img = Image.open(image_path).convert("RGB")
            draw = ImageDraw.Draw(img)

            img_w, img_h = img.size
            scale_x = img_w / self.page_width
            scale_y = img_h / self.page_height

            colors = ["#FF5733", "#33FF57", "#3357FF", "#FF33A1",
                      "#A133FF", "#33FFF5", "#FFD433", "#FF8C33"]

            for idx, (field_name, region_cfg) in enumerate(self.regions.items()):
                bbox = region_cfg.get('bbox', {})
                x1 = int(bbox.get('x1', 0) * scale_x)
                y1 = int(bbox.get('y1', 0) * scale_y)
                x2 = int(bbox.get('x2', 0) * scale_x)
                y2 = int(bbox.get('y2', 0) * scale_y)

                color = colors[idx % len(colors)]
                draw.rectangle([x1, y1, x2, y2], outline=color, width=2)
                draw.text((x1 + 4, y1 + 2), field_name, fill=color)

            img.save(output_path)
            logger.info(f"Visualisasi region disimpan ke: {output_path}")

        except Exception as e:
            logger.error(f"visualize_regions gagal: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _words_in_region(self, words: List[dict], bbox: dict) -> List[dict]:
        """Filter kata-kata OCR yang pusat (center) atau sebagian besar berada di dalam bbox."""
        rx1 = bbox.get('x1', 0)
        ry1 = bbox.get('y1', 0)
        rx2 = bbox.get('x2', self.page_width)
        ry2 = bbox.get('y2', self.page_height)

        matched = []
        for word in words:
            wx1, wy1, wx2, wy2 = self._get_word_bbox(word)
            if wx1 is None:
                continue

            # Center point of word
            cx = (wx1 + wx2) / 2
            cy = (wy1 + wy2) / 2

            if rx1 <= cx <= rx2 and ry1 <= cy <= ry2:
                matched.append(word)

        return matched

    @staticmethod
    def _get_word_bbox(word: dict) -> Tuple:
        """
        Normalkan berbagai format bbox dari OCR engine menjadi (x1, y1, x2, y2).
        Format yang didukung:
          - {'bbox': {'x1', 'y1', 'x2', 'y2'}}
          - {'bbox': [x1, y1, x2, y2]}
          - {'left', 'top', 'width', 'height'}
          - {'x', 'y', 'w', 'h'}
        """
        bb = word.get('bbox')
        if isinstance(bb, dict):
            return (
                bb.get('x1') or bb.get('x', 0),
                bb.get('y1') or bb.get('y', 0),
                bb.get('x2') or (bb.get('x', 0) + bb.get('w', 0)),
                bb.get('y2') or (bb.get('y', 0) + bb.get('h', 0)),
            )
        if isinstance(bb, (list, tuple)) and len(bb) >= 4:
            return bb[0], bb[1], bb[2], bb[3]

        # Fallback: left/top/width/height
        left   = word.get('left')
        top    = word.get('top')
        width  = word.get('width')
        height = word.get('height')
        if None not in (left, top, width, height):
            return left, top, left + width, top + height

        return None, None, None, None

    @staticmethod
    def _extract_with_pattern(text: str, pattern: str) -> Tuple[Optional[str], float]:
        """
        Cocokkan `pattern` terhadap `text`.

        Returns:
            (matched_value, confidence)
            confidence = 1.0 jika ada match, 0.0 jika tidak ada match atau pattern kosong.
        """
        if not text.strip():
            return None, 0.0

        if not pattern:
            # Tidak ada pattern — kembalikan seluruh teks
            return text.strip() or None, 0.5

        try:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(0).strip(), 1.0
        except re.error as e:
            logger.warning(f"Pattern regex tidak valid '{pattern}': {e}")

        return None, 0.0
