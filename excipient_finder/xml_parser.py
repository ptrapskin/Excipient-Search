"""Parse DailyMed SPL XML into SplRecord objects.

Uses xml.etree.ElementTree.  Structure is modular so lxml can be substituted
later by swapping the _parse_root() helper.

SPL XML uses the HL7 CDA namespace urn:hl7-org:v3.
One SPL XML may describe multiple product subjects (e.g. different strengths);
parse_spl_subjects() returns one SplRecord per product subject found.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import TYPE_CHECKING

from .models import IngredientEntry, SplRecord

NS = {"hl7": "urn:hl7-org:v3"}
_NDC_SYSTEM = "2.16.840.1.113883.6.69"
_UNII_SYSTEM = "2.16.840.1.113883.4.9"
_NDC_RE = re.compile(r"^\d{4,5}-\d{3,4}-\d{1,2}$")
_ACTIVE_CLASS_CODES = {"ACTIB", "ACTIM", "ACTIR"}


def _parse_root(xml_text: str) -> ET.Element | None:
    """Parse XML text and return the root element, or None on failure."""
    try:
        return ET.fromstring(xml_text)
    except ET.ParseError:
        return None


def _text(element: ET.Element | None, default: str = "") -> str:
    if element is None or element.text is None:
        return default
    return element.text.strip()


def _findtext(parent: ET.Element, path: str, default: str = "") -> str:
    el = parent.find(path, NS)
    return _text(el, default)


def _extract_setid(root: ET.Element) -> str:
    el = root.find("hl7:setId", NS)
    if el is not None:
        val = el.get("root", "")
        if val:
            return val
    return "unknown"


def _extract_document_type(root: ET.Element) -> str | None:
    el = root.find("hl7:code", NS)
    if el is not None:
        return el.get("displayName") or None
    return None


def _extract_labeler(root: ET.Element) -> str | None:
    paths = [
        ".//hl7:author/hl7:assignedEntity/hl7:representedOrganization/hl7:name",
        ".//hl7:representedOrganization/hl7:name",
    ]
    for path in paths:
        el = root.find(path, NS)
        if el is not None and el.text:
            return el.text.strip()
    return None


def _extract_ndcs(root: ET.Element) -> list[str]:
    seen: set[str] = set()
    ndcs: list[str] = []
    for code_el in root.findall(".//hl7:code", NS):
        if code_el.get("codeSystem") == _NDC_SYSTEM:
            code = code_el.get("code", "")
            if _NDC_RE.match(code) and code not in seen:
                seen.add(code)
                ndcs.append(code)
    return ndcs


def _extract_route(subject: ET.Element, product: ET.Element) -> str | None:
    """Search product then subject for routeCode displayName."""
    for search_root in (product, subject):
        el = search_root.find(".//hl7:routeCode", NS)
        if el is not None:
            val = el.get("displayName", "")
            if val:
                return val
    return None


def _extract_unii(subst: ET.Element) -> str | None:
    """Return the UNII code from an ingredientSubstance element, or None."""
    for code_el in subst.findall("hl7:code", NS):
        if code_el.get("codeSystem") == _UNII_SYSTEM:
            val = code_el.get("code", "").strip()
            if val:
                return val
    return None


def _extract_strength(ing: ET.Element) -> str | None:
    """Return a formatted strength string from an ingredient's quantity element.

    Returns strings like "10 g/15 mL" (solution) or "500 mg" (solid dose).
    Returns None when no quantity data is present.
    """
    qty = ing.find("hl7:quantity", NS)
    if qty is None:
        return None
    num = qty.find("hl7:numerator", NS)
    if num is None:
        return None
    num_val = num.get("value", "").strip()
    num_unit = num.get("unit", "").strip()
    if not num_val or num_unit in ("1", ""):
        return None
    num_str = f"{num_val} {num_unit}".strip()
    den = qty.find("hl7:denominator", NS)
    if den is not None:
        den_val = den.get("value", "").strip()
        den_unit = den.get("unit", "").strip()
        # Skip uninformative denominators: value=1 AND unit=1 means "per unit" (e.g. tablets)
        # Keep volume denominators. When value=1, omit it: "20 mg/mL" not "20 mg/1 mL"
        if den_val and den_unit and den_unit not in ("1", ""):
            if den_val == "1":
                return f"{num_str}/{den_unit}"   # 20 mg/mL
            return f"{num_str}/{den_val} {den_unit}"  # 10 g/15 mL
    return num_str


def _extract_ingredients(
    product: ET.Element,
    class_codes: set[str],
) -> list[tuple[str, str | None, str | None]]:
    """Return (raw_name, strength, unii) triples for ingredients matching the given classCodes."""
    result: list[tuple[str, str | None, str | None]] = []
    for ing in product.findall("hl7:ingredient", NS):
        if ing.get("classCode") in class_codes:
            subst = ing.find("hl7:ingredientSubstance", NS)
            if subst is not None:
                name = _findtext(subst, "hl7:name")
                if name:
                    result.append((name, _extract_strength(ing), _extract_unii(subst)))
    return result


def parse_spl_subjects(
    xml_text: str,
    setid: str,
    source_file: str,
) -> list[SplRecord]:
    """Parse one SPL XML string and return one SplRecord per product subject.

    Returns an empty list if the XML cannot be parsed.
    """
    root = _parse_root(xml_text)
    if root is None:
        return []

    doc_type = _extract_document_type(root)
    labeler = _extract_labeler(root)
    ndcs = _extract_ndcs(root)

    subjects = root.findall(".//hl7:subject/hl7:manufacturedProduct", NS)
    if not subjects:
        return []

    records: list[SplRecord] = []
    for subject in subjects:
        # The inner manufacturedProduct element holds the actual product data.
        product = subject.find("hl7:manufacturedProduct", NS)
        if product is None:
            product = subject  # some SPLs skip the nesting

        product_name = _findtext(product, "hl7:name") or "Unknown"

        form_el = product.find("hl7:formCode", NS)
        dosage_form = form_el.get("displayName", "").strip() if form_el is not None else None
        dosage_form = dosage_form or None

        route = _extract_route(subject, product)

        active_pairs = _extract_ingredients(product, _ACTIVE_CLASS_CODES)
        inactive_pairs = _extract_ingredients(product, {"IACT"})

        active_names = [name for name, _, _ in active_pairs]
        inactive_names = [name for name, _, _ in inactive_pairs]

        # Strength: "10 g/15 mL" or "500 mg; 250 mg" for combination products.
        active_strengths = [s for _, s, _ in active_pairs if s]
        active_strength = "; ".join(active_strengths) or None

        # UNII: semicolon-joined for active ingredients (aligned with names).
        active_uniis = [u for _, _, u in active_pairs if u]
        active_ingredients_unii = "; ".join(active_uniis) or None

        inactive_entries = [
            IngredientEntry(
                raw_name=name,
                normalized_name=_normalize_simple(name),
                unii=unii,
            )
            for name, _, unii in inactive_pairs
        ]

        records.append(SplRecord(
            setid=setid,
            product_name=product_name,
            dosage_form=dosage_form,
            route=route,
            labeler=labeler,
            ndcs=ndcs,
            active_ingredients_raw="; ".join(active_names) or None,
            active_strength=active_strength,
            active_ingredients_unii=active_ingredients_unii,
            inactive_ingredients_raw="; ".join(inactive_names) or None,
            inactive_ingredient_entries=inactive_entries,
            product_type=doc_type,
            source_file=source_file,
        ))

    return records


def _normalize_simple(text: str) -> str:
    """Basic normalization: lowercase, collapse whitespace."""
    import re as _re
    return _re.sub(r"\s+", " ", text.lower().strip())
