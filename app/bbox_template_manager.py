"""
BBox Template Manager
Manages multiple bbox templates and auto-selects the best one
"""

import logging
from matplotlib import text
import yaml
import re
from pathlib import Path
from typing import Dict, List, Optional
from bbox_template import BBoxTemplate
from typing import Union

logger = logging.getLogger(__name__)

class BBoxTemplateManager:
    def __init__(self, config: Union[str, Path, dict]):
        self.templates = {}
        self.template_configs = {}

        if isinstance(config, (str, Path)):
            self.config_path = Path(config)
            self._load_templates_from_file()
        elif isinstance(config, dict):
            self.config_path = None
            self._load_templates_from_dict(config)
        else:
            raise TypeError("config must be path or dict")

    def _load_templates_from_file(self):
        if not self.config_path.exists():
            logger.warning(f"BBox templates config not found: {self.config_path}")
            return

        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f) or {}

            self._load_templates_from_dict(config)

        except Exception as e:
            logger.error(f"Error loading bbox templates: {e}")

    def _load_templates_from_dict(self, config: dict):
        for key, value in config.items():
            if key.startswith('templates'):
                template_name = value.get('name', key)
                self.template_configs[template_name] = value
                self.templates[template_name] = BBoxTemplate(value)
                logger.info(f"Loaded bbox templates: {template_name}")

        logger.info(f"Loaded {len(self.templates)} bbox templates")

    def detect_template(self, ocr_result: Dict) -> Optional[str]:
        if not self.templates:
            logger.warning("No bbox templates available")
            return None
        
        full_text = ocr_result.get('full_text', '').lower()
        
        # Strategy 1: Check for specific keywords
        # Template 1 = Nasional format (Indonesian keywords)
        # Template 2 = International format (English keywords)
        
        nasional_keywords = ['nasional', 'divisi', 'oleh', 'di jakarta', 'di surabaya']
        international_keywords = ['international', 'division', 'by', 'at singapore', 'certificate']
        
        nasional_score = sum(1 for kw in nasional_keywords if kw in full_text)
        international_score = sum(1 for kw in international_keywords if kw in full_text)
        
        if nasional_score > international_score:
            logger.info(f"Detected template1 (nasional) - score: {nasional_score}")
            return 'template1'
        elif international_score > nasional_score:
            logger.info(f"Detected template2 (international) - score: {international_score}")
            return 'template2'
        else:
            # Default to template1
            logger.info("No clear templates match, using template1 as default")
            return 'template1'
    
    def extract_with_template(self, template_name: str, ocr_result: Dict) -> Dict:
        if template_name not in self.templates:
            logger.error(f"Template not found: {template_name}")
            return {
                'error': f'Template {template_name} not found',
                'extracted_fields': {},
                'confidence_scores': {}
            }
        
        template = self.templates[template_name]
        result = template.extract_from_ocr(ocr_result)

        extracted_fields = result.get("extracted_fields", {})

        if "nosert" in extracted_fields:
            cleaned = self._clean_nosert(extracted_fields.get("nosert"))
            extracted_fields["nosert"] = cleaned

        result["extracted_fields"] = extracted_fields
        return result
    
    def extract_auto(self, ocr_result: Dict) -> Dict:
        template_name = self.detect_template(ocr_result)
        
        if not template_name:
            return {
                'error': 'No templates detected',
                'template_name': None,
                'extracted_fields': {},
                'confidence_scores': {}
            }
        
        result = self.extract_with_template(template_name, ocr_result)
        result['auto_detected'] = True
        
        return result
    
    def get_available_templates(self) -> List[str]:
        return list(self.templates.keys())
    
    def visualize_template(self, template_name: str, image_path: str, output_path: str):
        if template_name not in self.templates:
            logger.error(f"Template not found: {template_name}")
            return
        
        template = self.templates[template_name]
        template.visualize_regions(image_path, output_path)
    
    def _clean_nosert(self, text: str) -> Optional[str]:
        if not text:
            return None

        text = text.replace("1964", "")

        matches = re.findall(r'\b\d{5}\b', text)

        if matches:
            return matches[0]

        return None