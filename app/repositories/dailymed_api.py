"""Live DailyMed API repository and shared SPL parsing helpers."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

import httpx

from app.config import Settings
from app.domain.models import (
    DrugQuery,
    IngredientEntry,
    ProductCandidate,
    ProductDetail,
    ProductSearchResult,
)
from app.repositories.http_client import get_with_retry
from app.services.normalize_query import DOSE_FORM_HINTS, ROUTE_HINTS
from app.services.parsing_service import ParsingService

logger = logging.getLogger(__name__)

NAMESPACES = {"hl7": "urn:hl7-org:v3"}

# Terms to strip from the DailyMed drug_name param so we always search by ingredient only
_QUALIFIER_TERMS = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in {**DOSE_FORM_HINTS, **ROUTE_HINTS}) + r")\b",
    re.IGNORECASE,
)


class DailyMedAPIError(RuntimeError):
    """Raised when DailyMed requests fail."""


class DailyMedApiRepository:
    """Repository that maps live DailyMed responses into domain models."""

    def __init__(
        self,
        settings: Settings,
        client: httpx.AsyncClient,
        parsing_service: ParsingService,
    ) -> None:
        self._settings = settings
        self._client = client
        self._parsing_service = parsing_service

    async def search_spls(self, query: DrugQuery) -> list[ProductSearchResult]:
        """Return all SPL results for a query directly from DailyMed — SPL is the primary entity."""

        results = await self.search_products(query)
        return dedupe_search_results(results)

    async def get_spl(self, setid: str) -> dict:
        """Return raw SPL XML for a SETID."""

        xml_text = await self._fetch_text(f"/spls/{setid}.xml")
        return {"setid": setid, "xml_text": xml_text, "source": "dailymed_api"}

    async def get_inactive_ingredients(self, setid: str) -> list[dict]:
        """Return parsed inactive ingredient dictionaries for a SETID."""

        spl = await self.get_spl(setid)
        return build_product_inactive_ingredient_dicts(spl["xml_text"], self._parsing_service)

    async def get_product_detail(self, setid: str) -> ProductDetail:
        """Retrieve a single SPL document and extract product details."""

        spl = await self.get_spl(setid)
        return build_product_detail_from_xml(
            xml_text=spl["xml_text"],
            setid=setid,
            base_url=self._settings.dailymed_base_url,
            parsing_service=self._parsing_service,
        )

    def _parse_product_detail(self, xml_text: str, setid: str) -> ProductDetail:
        """Compatibility wrapper used by parsing-focused tests."""

        return build_product_detail_from_xml(
            xml_text=xml_text,
            setid=setid,
            base_url=self._settings.dailymed_base_url,
            parsing_service=self._parsing_service,
        )

    async def find_products(self, query: DrugQuery, concepts: list) -> list[ProductCandidate]:
        """Search DailyMed and return product candidates for the expander."""

        results = await self.search_products(query)
        return [
            ProductCandidate(
                rxcui=result.rxcui,
                setid=result.setid,
                ndc=result.ndcs[0] if result.ndcs else None,
                product_name=result.product_name,
                labeler=result.labeler,
                dosage_form=result.dosage_form,
                route=result.route,
                source="dailymed_api",
            )
            for result in results
        ]

    async def search_products(self, query: DrugQuery) -> list[ProductSearchResult]:
        """Search DailyMed by base drug name only, then post-filter by dose form/route."""

        base_params: dict = {"pagesize": 100}
        if query.ndc:
            base_params["ndc"] = query.ndc
        else:
            # Strip dose form and route terms so DailyMed searches the ingredient broadly.
            # e.g. "metformin tablet" → "metformin"  (541 results vs 7)
            base_name = _QUALIFIER_TERMS.sub(" ", query.normalized_text)
            base_name = " ".join(base_name.split()).strip()
            base_params["drug_name"] = base_name or query.normalized_text

        results = await self._fetch_all_pages("/spls.json", base_params)

        # Post-filter by requested dose form / route using the product title text
        if query.requested_dose_form:
            form = query.requested_dose_form.casefold()
            results = [r for r in results if form in r.product_name.casefold()]
        if query.requested_route:
            route = query.requested_route.casefold()
            results = [r for r in results if route in r.product_name.casefold()]

        return results

    async def search_products_by_rxcui(self, rxcui: str) -> list[ProductSearchResult]:
        """Search DailyMed SPL records linked to a product-level RxCUI, fetching all pages."""

        return await self._fetch_all_pages(
            "/spls.json",
            {"rxcui": rxcui, "pagesize": 100},
            default_rxcui=rxcui,
        )

    async def _fetch_all_pages(
        self,
        path: str,
        base_params: dict,
        default_rxcui: str | None = None,
    ) -> list[ProductSearchResult]:
        """Paginate through all DailyMed result pages and return every result."""

        results: list[ProductSearchResult] = []
        page = 1
        while True:
            payload = await self._fetch_json(path, params={**base_params, "page": page})
            results.extend(self._build_search_results(payload, default_rxcui=default_rxcui))
            metadata = payload.get("metadata", {})
            total_pages = int(metadata.get("total_pages", 1) or 1)
            if page >= total_pages:
                break
            page += 1
        return results

    async def _fetch_json(self, path: str, params: dict | None = None) -> dict:
        """Fetch JSON from DailyMed with retry handling."""

        payload = await self._request(path=path, params=params)
        try:
            return payload.json()
        except ValueError as exc:
            raise DailyMedAPIError("DailyMed returned invalid JSON") from exc

    async def _fetch_text(self, path: str, params: dict | None = None) -> str:
        """Fetch text content from DailyMed with retry handling."""

        return (await self._request(path=path, params=params)).text

    async def _request(self, path: str, params: dict | None = None) -> httpx.Response:
        """Issue a GET request with retries."""

        return await get_with_retry(
            self._client,
            url=f"{self._settings.dailymed_base_url}{path}",
            params=params,
            retries=self._settings.http_retries,
            error_cls=DailyMedAPIError,
            label="DailyMed",
        )

    def _build_search_results(
        self,
        payload: dict,
        default_rxcui: str | None = None,
    ) -> list[ProductSearchResult]:
        """Build internal search result models from a DailyMed JSON payload."""

        data = payload.get("data", [])
        if isinstance(data, dict):
            data = [data]

        results: list[ProductSearchResult] = []
        for item in data:
            title = str(item.get("title") or "").strip()
            product_name, labeler = split_title(title)
            results.append(
                ProductSearchResult(
                    product_name=product_name or title or "Unknown product",
                    setid=str(item.get("setid") or "").strip() or None,
                    labeler=labeler,
                    rxcui=str(item.get("rxcui") or default_rxcui or "").strip() or None,
                )
            )
        return results

    async def get_all_product_details(self, setid: str) -> list[ProductDetail]:
        """Return one ProductDetail per product subject found in the SPL."""

        spl = await self.get_spl(setid)
        return build_all_product_details_from_xml(
            xml_text=spl["xml_text"],
            setid=setid,
            base_url=self._settings.dailymed_base_url,
            parsing_service=self._parsing_service,
        )


def build_all_product_details_from_xml(
    xml_text: str,
    setid: str,
    base_url: str,
    parsing_service: ParsingService,
) -> list[ProductDetail]:
    """Parse a DailyMed SPL XML into one ProductDetail per product subject.

    For SPLs with a single product (or no structured subjects), falls back to
    the standard single-product parser so the return value is always non-empty.
    """

    root = parse_spl_root(xml_text)
    labeler = extract_labeler(root)
    doc_route = extract_first_display_name(root, ".//hl7:routeCode")
    spl_source = f"{base_url}/spls/{setid}.xml"
    fetched_at = datetime.now(timezone.utc)

    subjects = find_product_subjects(root)
    if not subjects:
        return [build_product_detail_from_xml(xml_text, setid, base_url, parsing_service)]

    details: list[ProductDetail] = []
    for subject in subjects:
        name = clean_text(subject.findtext("hl7:name", default="", namespaces=NAMESPACES))
        form_el = subject.find("hl7:formCode", NAMESPACES)
        form = clean_text(form_el.attrib.get("displayName", "")) if form_el is not None else ""

        active_entries = extract_product_data_ingredients(subject, parsing_service, role="active")
        inactive_entries = extract_product_data_ingredients(subject, parsing_service, role="inactive")

        # Build descriptive name: "Keppra 250 mg TABLET, FILM COATED"
        product_name = name or "Unknown product"
        if active_entries and active_entries[0].strength:
            strength = active_entries[0].strength
            if strength.casefold() not in product_name.casefold():
                product_name = f"{product_name} {strength}"
        if form and form.casefold() not in product_name.casefold():
            product_name = f"{product_name} {form}"

        ndcs = extract_subject_ndcs(subject)
        route_el = subject.find(".//hl7:routeCode", NAMESPACES)
        route = (
            clean_text(route_el.attrib.get("displayName", ""))
            if route_el is not None
            else doc_route
        )
        inactive_raw = (
            ", ".join(e.display_name or e.raw_name for e in inactive_entries) or None
        )

        details.append(ProductDetail(
            product_name=product_name,
            setid=setid,
            ndcs=ndcs,
            route=route or doc_route,
            dosage_form=form or None,
            labeler=labeler,
            active_ingredients=active_entries,
            inactive_ingredients_raw=inactive_raw,
            inactive_ingredients=inactive_entries,
            spl_source=spl_source,
            fetched_at=fetched_at,
        ))

    return details


def extract_subject_ndcs(subject: ET.Element) -> list[str]:
    """Extract NDC-formatted codes from a single product subject element."""

    ndcs: list[str] = []
    seen: set[str] = set()
    for code_el in subject.findall(".//hl7:code", NAMESPACES):
        code = clean_text(code_el.attrib.get("code", ""))
        if re.match(r"^\d{4,5}-\d{3,4}-\d{1,2}$", code) and code not in seen:
            seen.add(code)
            ndcs.append(code)
    return ndcs


def build_product_detail_from_xml(
    xml_text: str,
    setid: str,
    base_url: str,
    parsing_service: ParsingService,
) -> ProductDetail:
    """Parse a DailyMed SPL XML document into the internal detail model."""

    root = parse_spl_root(xml_text)
    title = extract_product_name(root)
    labeler = extract_labeler(root)
    route = extract_first_display_name(root, ".//hl7:routeCode")
    dosage_form = extract_first_display_name(root, ".//hl7:formCode")
    ndcs = extract_ndcs(xml_text)
    doc_code = root.find("hl7:code", NAMESPACES)
    product_type = doc_code.attrib.get("displayName") if doc_code is not None else None

    inactive_entries, inactive_raw = extract_ingredient_entries_from_sections(
        root,
        match_terms=["inactive ingredient", "inactive ingredients"],
        parsing_service=parsing_service,
    )
    active_entries, _active_raw = extract_ingredient_entries_from_sections(
        root,
        match_terms=["active ingredient", "active ingredients", "active moiety"],
        parsing_service=parsing_service,
    )

    return ProductDetail(
        product_name=title or "Unknown product",
        setid=setid,
        ndcs=ndcs,
        route=route,
        dosage_form=dosage_form,
        labeler=labeler,
        product_type=product_type,
        active_ingredients=active_entries,
        inactive_ingredients_raw=inactive_raw,
        inactive_ingredients=inactive_entries,
        spl_source=f"{base_url}/spls/{setid}.xml",
        fetched_at=datetime.now(timezone.utc),
    )


def build_product_inactive_ingredient_dicts(
    xml_text: str,
    parsing_service: ParsingService,
) -> list[dict]:
    """Parse inactive ingredients from SPL XML and return dictionaries."""

    root = parse_spl_root(xml_text)
    ingredients, _inactive_raw = extract_ingredient_entries_from_sections(
        root,
        match_terms=["inactive ingredient", "inactive ingredients"],
        parsing_service=parsing_service,
    )
    return [ingredient.model_dump() for ingredient in ingredients]


def parse_spl_root(xml_text: str) -> ET.Element:
    """Parse SPL XML into an ElementTree root node."""

    try:
        return ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise DailyMedAPIError("DailyMed returned malformed XML") from exc


def extract_ndcs(xml_text: str) -> list[str]:
    """Extract NDC-like identifiers from SPL XML text."""

    matches = re.findall(r"\b\d{4,5}-\d{3,4}-\d{1,2}\b", xml_text)
    unique: list[str] = []
    for match in matches:
        if match not in unique:
            unique.append(match)
    return unique


def split_title(title: str) -> tuple[str, str | None]:
    """Split a DailyMed title into product name and labeler components."""

    match = re.match(r"^(?P<name>.+?)\s*\[(?P<labeler>.+)\]\s*$", title)
    if not match:
        return title, None
    return match.group("name").strip(), match.group("labeler").strip()


def extract_labeler(root: ET.Element) -> str | None:
    """Extract the first represented organization name as the labeler."""

    for path in (
        ".//hl7:representedOrganization/hl7:name",
        ".//hl7:author/hl7:assignedEntity/hl7:representedOrganization/hl7:name",
    ):
        value = clean_text(root.findtext(path, default="", namespaces=NAMESPACES))
        if value:
            return value
    return None


def extract_product_name(root: ET.Element) -> str:
    """Extract the product name, preferring manufactured product metadata over document title."""

    manufactured_name = None
    for path in (
        ".//hl7:subject/hl7:manufacturedProduct/hl7:manufacturedProduct/hl7:name",
        ".//hl7:subject/hl7:manufacturedProduct/hl7:manufacturedMedicine/hl7:name",
        ".//hl7:manufacturedProduct/hl7:manufacturedProduct/hl7:name",
        ".//hl7:manufacturedProduct/hl7:manufacturedMedicine/hl7:name",
    ):
        manufactured_name = clean_text(root.findtext(path, default="", namespaces=NAMESPACES))
        if manufactured_name:
            break

    manufactured_form = None
    for path in (
        ".//hl7:subject/hl7:manufacturedProduct/hl7:manufacturedProduct/hl7:formCode",
        ".//hl7:subject/hl7:manufacturedProduct/hl7:manufacturedMedicine/hl7:formCode",
        ".//hl7:manufacturedProduct/hl7:manufacturedProduct/hl7:formCode",
        ".//hl7:manufacturedProduct/hl7:manufacturedMedicine/hl7:formCode",
    ):
        element = root.find(path, NAMESPACES)
        if element is not None:
            manufactured_form = clean_text(element.attrib.get("displayName", ""))
            if manufactured_form:
                break

    if manufactured_name:
        if manufactured_form and manufactured_form.casefold() not in manufactured_name.casefold():
            return f"{manufactured_name} {manufactured_form}"
        return manufactured_name

    title_element = root.find("hl7:title", NAMESPACES)
    if title_element is not None:
        flattened_title = flatten_text(title_element)
        if flattened_title:
            return flattened_title
    return clean_text(root.findtext("hl7:title", default="", namespaces=NAMESPACES)) or "Unknown product"


def extract_first_display_name(root: ET.Element, xpath: str) -> str | None:
    """Extract the first displayName attribute for a given XPath."""

    element = root.find(xpath, NAMESPACES)
    if element is None:
        return None
    return clean_text(element.attrib.get("displayName", ""))


def extract_section_text(root: ET.Element, match_terms: list[str]) -> str | None:
    """Return the flattened text of the first matching SPL section."""

    normalized_terms = [term.casefold() for term in match_terms]
    for section in root.findall(".//hl7:section", NAMESPACES):
        title = clean_text(section.findtext("hl7:title", default="", namespaces=NAMESPACES))
        code = section.find("hl7:code", NAMESPACES)
        display_name = clean_text(code.attrib.get("displayName", "")) if code is not None else ""
        if not matches_section_terms(
            values=[title.casefold(), display_name.casefold()],
            terms=normalized_terms,
        ):
            continue
        text_element = section.find("hl7:text", NAMESPACES)
        if text_element is None:
            continue
        flattened = flatten_text(text_element)
        if flattened:
            return flattened
    return None


def extract_ingredient_entries_from_sections(
    root: ET.Element,
    match_terms: list[str],
    parsing_service: ParsingService,
) -> tuple[list[IngredientEntry], str | None]:
    """Extract ingredient entries using table, structured XML, then narrative fallback."""

    normalized_terms = [term.casefold() for term in match_terms]
    if any("inactive" in term.casefold() for term in match_terms):
        role = "inactive"
    else:
        role = "active"
    for section in root.findall(".//hl7:section", NAMESPACES):
        title = clean_text(section.findtext("hl7:title", default="", namespaces=NAMESPACES))
        code = section.find("hl7:code", NAMESPACES)
        display_name = clean_text(code.attrib.get("displayName", "")) if code is not None else ""
        if not matches_section_terms(
            values=[title.casefold(), display_name.casefold()],
            terms=normalized_terms,
        ):
            continue

        text_element = section.find("hl7:text", NAMESPACES)
        if text_element is None:
            continue

        raw_text = flatten_text(text_element) or None

        table_entries = extract_table_ingredients(text_element, parsing_service, role=role)
        if table_entries:
            return table_entries, raw_text

        structured_chunks = extract_structured_chunks(text_element)
        structured_entries = parsing_service.parse_structured_ingredients(
            structured_chunks,
            source_type="xml_structured",
            role=role,
        )
        if structured_entries:
            return structured_entries, raw_text

        product_data_entries = extract_product_data_ingredients(root, parsing_service, role=role)
        if product_data_entries:
            return product_data_entries, raw_text

        return parsing_service.parse_narrative_ingredients(
            raw_text,
            source_type="narrative_text",
            role=role,
        ), raw_text

    product_data_entries = extract_product_data_ingredients(root, parsing_service, role=role)
    if product_data_entries:
        return product_data_entries, None

    return [], None


def extract_table_ingredients(
    text_element: ET.Element,
    parsing_service: ParsingService,
    role: str,
) -> list[IngredientEntry]:
    """Extract ingredient rows from structured SPL tables when available."""

    tables = [element for element in text_element.iter() if local_name(element.tag) == "table"]
    entries: list[IngredientEntry] = []
    seen: set[str] = set()

    for table in tables:
        rows = [element for element in table.iter() if local_name(element.tag) == "tr"]
        if not rows:
            continue

        headers: list[str] | None = None
        for row in rows:
            cells = [element for element in row if local_name(element.tag) in {"th", "td"}]
            values = [flatten_text(cell) for cell in cells]
            values = [value for value in values if value]
            if not values:
                continue

            if headers is None and (any(local_name(cell.tag) == "th" for cell in cells) or looks_like_header_row(values)):
                headers = [normalize_header(value) for value in values]
                continue

            ingredient_name = None
            unii = None
            strength = None

            if headers:
                for index, value in enumerate(values):
                    header = headers[index] if index < len(headers) else ""
                    if is_ingredient_header(header):
                        ingredient_name = value
                    elif "unii" in header:
                        unii = normalize_unii_cell(value)
                    elif is_strength_header(header):
                        strength = value
                if ingredient_name is None and values:
                    ingredient_name = values[0]
            elif values:
                ingredient_name = values[0]
                if len(values) > 1:
                    strength = values[1]
                if len(values) > 2:
                    unii = normalize_unii_cell(values[2])

            if not ingredient_name:
                continue

            raw_row_text = " | ".join(values)
            entry = parsing_service.build_table_entry(
                ingredient_name,
                raw_row_text=raw_row_text,
                role=role,
                unii=unii,
                strength=strength,
            )
            if entry is None:
                continue

            key = f"{(entry.display_name or entry.raw_name).casefold()}|{entry.unii or ''}|{entry.role or ''}"
            if key in seen:
                continue
            seen.add(key)
            entries.append(entry)

    return entries


def extract_structured_chunks(text_element: ET.Element) -> list[str]:
    """Extract paragraph and list-item text outside tables for structured XML parsing."""

    chunks: list[str] = []
    seen: set[str] = set()
    parent_map = {child: parent for parent in text_element.iter() for child in parent}
    for element in text_element.iter():
        name = local_name(element.tag)
        if name not in {"paragraph", "item", "content"}:
            continue
        if name == "content" and list(element):
            continue
        if has_table_ancestor(element, parent_map):
            continue
        value = flatten_text(element)
        if not value or value.casefold() in seen:
            continue
        seen.add(value.casefold())
        chunks.append(value)
    return chunks


def extract_product_data_ingredients(
    root: ET.Element,
    parsing_service: ParsingService,
    role: str,
) -> list[IngredientEntry]:
    """Extract ingredients from structured product-data nodes when section tables are unavailable."""

    entries: list[IngredientEntry] = []
    seen: set[str] = set()

    for ingredient, substance_path in iter_product_data_ingredient_nodes(root, role):
        substance = ingredient.find(substance_path, NAMESPACES)
        if substance is None:
            continue

        name = clean_text(substance.findtext("hl7:name", default="", namespaces=NAMESPACES))
        if not name:
            continue

        code_element = substance.find("hl7:code", NAMESPACES)
        unii = clean_text(code_element.attrib.get("code", "")) if code_element is not None else None
        strength = extract_ingredient_strength(ingredient)
        entry = parsing_service.build_entry(
            name,
            source_type="xml_structured",
            role=role,
            unii_override=unii or None,
            strength_override=strength,
            raw_name_override=name,
        )
        if entry is None:
            continue

        key = f"{(entry.display_name or entry.raw_name).casefold()}|{entry.unii or ''}|{entry.role or ''}"
        if key in seen:
            continue
        seen.add(key)
        entries.append(entry)

    return entries


def find_product_subjects(root: ET.Element) -> list[ET.Element]:
    """Return per-product subject nodes across modern and legacy SPL layouts."""

    subjects: list[ET.Element] = []
    for container in root.findall(".//hl7:subject/hl7:manufacturedProduct", NAMESPACES):
        for child in list(container):
            if local_name(child.tag) in {"manufacturedProduct", "manufacturedMedicine"}:
                subjects.append(child)
    return subjects


def iter_product_data_ingredient_nodes(
    root: ET.Element,
    role: str,
) -> list[tuple[ET.Element, str]]:
    """Return ingredient nodes and substance paths for supported SPL ingredient layouts."""

    nodes: list[tuple[ET.Element, str]] = []

    # DailyMed SPLs use several classCode values for active ingredients:
    # ACTIM (moiety), ACTIB (basis of strength), ACTIR (active moiety)
    active_codes = {"ACTIM", "ACTIB", "ACTIR", "ACTI"}
    for ingredient in root.findall(".//hl7:ingredient", NAMESPACES):
        ingredient_class = clean_text(ingredient.attrib.get("classCode", "")).upper()
        if role == "active":
            if ingredient_class not in active_codes:
                continue
        elif ingredient_class != "IACT":
            continue
        nodes.append((ingredient, "hl7:ingredientSubstance"))

    legacy_tag = "activeIngredient" if role == "active" else "inactiveIngredient"
    legacy_substance = "hl7:activeIngredientSubstance" if role == "active" else "hl7:inactiveIngredientSubstance"
    for ingredient in root.findall(f".//hl7:{legacy_tag}", NAMESPACES):
        nodes.append((ingredient, legacy_substance))

    return nodes


def flatten_text(element: ET.Element) -> str:
    """Flatten a mixed-content XML node into readable text."""

    raw_lines = []
    for chunk in element.itertext():
        normalized = clean_text(chunk)
        if normalized:
            raw_lines.append(normalized)
    return "\n".join(raw_lines).strip()


def extract_ingredient_strength(ingredient: ET.Element) -> str | None:
    """Extract a displayable strength from a structured ingredient quantity."""

    quantity = ingredient.find("hl7:quantity", NAMESPACES)
    if quantity is None:
        return None

    numerator = quantity.find("hl7:numerator", NAMESPACES)
    denominator = quantity.find("hl7:denominator", NAMESPACES)
    numerator_text = format_quantity_part(numerator)
    denominator_text = format_quantity_part(denominator)

    if numerator_text and denominator_text:
        if denominator_text == "1":
            return numerator_text
        return f"{numerator_text}/{denominator_text}"
    return numerator_text


def format_quantity_part(element: ET.Element | None) -> str | None:
    """Format one SPL quantity part such as 20 mg or 5 mL."""

    if element is None:
        return None
    value = clean_text(element.attrib.get("value", ""))
    unit = clean_text(element.attrib.get("unit", ""))
    if not value:
        return None
    if not unit or unit == "1":
        return value
    return f"{value} {unit}"


def local_name(tag: str) -> str:
    """Return an XML tag's local name without namespace."""

    return tag.split("}", 1)[-1] if "}" in tag else tag


def has_table_ancestor(target: ET.Element, parent_map: dict[ET.Element, ET.Element]) -> bool:
    """Return whether an element is nested beneath a table."""

    current = parent_map.get(target)
    while current is not None:
        if local_name(current.tag) == "table":
            return True
        current = parent_map.get(current)
    return False


def clean_text(value: str | None) -> str:
    """Collapse repeated whitespace while preserving words."""

    return re.sub(r"\s+", " ", value or "").strip()


def matches_section_terms(values: list[str], terms: list[str]) -> bool:
    """Check whether section metadata matches a target term with word boundaries."""

    for value in values:
        for term in terms:
            pattern = rf"(?:^|\b){re.escape(term)}(?:$|\b)"
            if re.search(pattern, value):
                return True
    return False


def looks_like_header_row(values: list[str]) -> bool:
    """Return whether a row looks like a table header."""

    normalized = {normalize_header(value) for value in values}
    return any(
        is_ingredient_header(value) or is_strength_header(value) or "unii" in value
        for value in normalized
    )


def normalize_header(value: str) -> str:
    """Normalize a table header to a comparable form."""

    return re.sub(r"\s+", " ", value.casefold()).strip(" :")


def is_ingredient_header(value: str) -> bool:
    """Return whether a normalized header points to an ingredient name column."""

    return value in {
        "ingredient",
        "ingredient name",
        "active ingredient",
        "active moiety",
        "inactive ingredient",
        "name",
    }


def is_strength_header(value: str) -> bool:
    """Return whether a normalized header points to a strength column."""

    return "strength" in value or value in {"basis of strength", "amount"}


def normalize_unii_cell(value: str) -> str | None:
    """Normalize a UNII table cell value."""

    cleaned = clean_text(value).strip()
    if not cleaned:
        return None
    match = re.search(r"[A-Z0-9]{10}", cleaned.upper())
    return match.group(0) if match else cleaned.upper()


def dedupe_search_results(results: list[ProductSearchResult]) -> list[ProductSearchResult]:
    """Deduplicate search results by setid, rxcui, or product name, preserving order."""

    deduped: list[ProductSearchResult] = []
    seen: set[str] = set()
    for result in results:
        key = result.setid or result.rxcui or result.product_name.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(result)
    return deduped
