from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class IngredientEntry:
    raw_name: str
    normalized_name: str
    unii: str | None = None


@dataclass
class SplRecord:
    setid: str
    product_name: str
    dosage_form: str | None
    route: str | None
    labeler: str | None
    ndcs: list[str]
    active_ingredients_raw: str | None       # semicolon-joined raw names
    active_strength: str | None              # semicolon-joined strength strings e.g. "10 g/15 mL"
    active_ingredients_unii: str | None      # semicolon-joined UNII codes aligned with names
    inactive_ingredients_raw: str | None     # semicolon-joined raw names
    inactive_ingredient_entries: list[IngredientEntry]
    product_type: str | None                 # from document code displayName
    source_file: str                         # outer zip filename


@dataclass
class FilterDecision:
    form_class: str          # "strong" | "ambiguous" | "excluded" | "non_liquid"
    route_class: str         # "oral" | "excluded" | "blank"
    included_form_match: str | None
    excluded_form_match: str | None
    included_route_match: str | None
    excluded_route_match: str | None
    should_process: bool     # False = hard-excluded, skip excipient matching


@dataclass
class MatchedExcipient:
    raw_name: str
    normalized_name: str
    canonical_name: str
    category: str            # "high" | "moderate"
    unii: str | None = None


@dataclass
class ProductOutputRow:
    spl_setid: str
    product_name: str
    labeler: str | None
    dosage_form: str | None
    normalized_form: str
    form_class: str
    route: str | None
    normalized_route: str
    route_class: str
    ndcs: str                            # semicolon-joined
    active_ingredients_raw: str | None
    active_strength: str | None          # semicolon-joined strength strings
    active_ingredients_unii: str | None  # semicolon-joined UNII codes
    concern_tier: str                    # "high" | "moderate" | "review" | "excluded"
    inclusion_decision: str             # "included" | "excluded"
    review_reason: str | None
    included_form_match: str | None
    excluded_form_match: str | None
    included_route_match: str | None
    excluded_route_match: str | None
    inactive_ingredients_raw: str | None
    inactive_ingredients_unii: str       # semicolon-joined UNIIs aligned with inactive_ingredients_raw
    matched_sugar_alcohols: str          # semicolon-joined canonical names
    matched_sugar_alcohol_terms: str     # semicolon-joined raw matched terms
    matched_sugar_alcohol_uniis: str     # semicolon-joined UNIIs (empty string when absent, aligned)
    source_file: str
    processed_at: str
    matched_excipient_list: list[MatchedExcipient] = field(default_factory=list)  # not stored in products table; used for matched_excipients table


@dataclass
class ParseFailure:
    source_file: str
    xml_member_name: str     # inner zip entry name
    error_type: str          # "parse_error" | "read_error" | "xml_error"
    error_message: str
    processed_at: str
