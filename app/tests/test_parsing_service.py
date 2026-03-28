import httpx

from app.config import Settings
from app.repositories.dailymed_api import (
    DailyMedAPIRepository,
    build_all_product_details_from_xml,
    build_product_detail_from_xml,
)
from app.services.parsing_service import ParsingService


def test_parsing_service_extracts_generic_inactive_ingredients():
    service = ParsingService()

    results = service.parse_inactive_ingredients(
        "Inactive ingredients: lactose monohydrate; magnesium stearate; colloidal silicon dioxide"
    )

    assert [entry.display_name for entry in results] == [
        "lactose monohydrate",
        "magnesium stearate",
        "colloidal silicon dioxide",
    ]
    assert all(entry.source_type == "narrative_text" for entry in results)
    assert results[0].raw_name == "Inactive ingredients: lactose monohydrate"


def test_dailymed_product_detail_prefers_inactive_ingredient_table():
    xml = """
    <document xmlns="urn:hl7-org:v3">
        <title>Example Product</title>
        <section>
            <title>Inactive Ingredients</title>
            <text>
                <table>
                    <thead>
                        <tr>
                            <th>Ingredient Name</th>
                            <th>Strength</th>
                            <th>UNII</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>lactose monohydrate</td>
                            <td>15 mg</td>
                            <td>EWQ57Q8I5X</td>
                        </tr>
                    </tbody>
                </table>
                <paragraph>red dye 40</paragraph>
            </text>
        </section>
    </document>
    """

    repository = DailyMedAPIRepository(Settings(), httpx.AsyncClient(), ParsingService())
    detail = repository._parse_product_detail(xml_text=xml, setid="set-123")

    assert len(detail.inactive_ingredients) == 1
    assert detail.inactive_ingredients[0].display_name == "lactose monohydrate"
    assert detail.inactive_ingredients[0].raw_name == "lactose monohydrate | 15 mg | EWQ57Q8I5X"
    assert detail.inactive_ingredients[0].strength == "15 mg"
    assert detail.inactive_ingredients[0].unii == "EWQ57Q8I5X"
    assert detail.inactive_ingredients[0].source_type == "table"
    assert detail.inactive_ingredients[0].confidence == "high"
    assert detail.inactive_ingredients[0].role == "inactive"


def test_dailymed_product_detail_uses_structured_xml_before_narrative():
    xml = """
    <document xmlns="urn:hl7-org:v3">
        <title>Example Product</title>
        <section>
            <title>Inactive Ingredients</title>
            <text>
                <paragraph>lactose monohydrate</paragraph>
                <paragraph>magnesium stearate (UNII: 70097M6I30)</paragraph>
            </text>
        </section>
    </document>
    """

    repository = DailyMedAPIRepository(Settings(), httpx.AsyncClient(), ParsingService())
    detail = repository._parse_product_detail(xml_text=xml, setid="set-123")

    assert [entry.display_name for entry in detail.inactive_ingredients] == [
        "lactose monohydrate",
        "magnesium stearate",
    ]
    assert all(entry.source_type == "xml_structured" for entry in detail.inactive_ingredients)
    assert detail.inactive_ingredients[1].raw_name == "magnesium stearate (UNII: 70097M6I30)"


def test_dailymed_product_detail_parses_inactive_section():
    xml = """
    <document xmlns="urn:hl7-org:v3">
        <title>Example Product</title>
        <author>
            <assignedEntity>
                <representedOrganization>
                    <name>Example Labeler</name>
                </representedOrganization>
            </assignedEntity>
        </author>
        <manufacturedProduct>
            <manufacturedMedicine>
                <formCode displayName="TABLET" />
            </manufacturedMedicine>
        </manufacturedProduct>
        <routeCode displayName="ORAL" />
        <section>
            <title>Inactive Ingredients</title>
            <text>
                <paragraph>lactose monohydrate</paragraph>
                <paragraph>magnesium stearate (UNII: 70097M6I30)</paragraph>
            </text>
        </section>
        <section>
            <title>Active Ingredients</title>
            <text>
                <paragraph>metformin hydrochloride 500 mg</paragraph>
            </text>
        </section>
        <subject>
            <manufacturedProduct>
                <code code="12345-678-90" />
            </manufacturedProduct>
        </subject>
    </document>
    """

    repository = DailyMedAPIRepository(Settings(), httpx.AsyncClient(), ParsingService())
    detail = repository._parse_product_detail(xml_text=xml, setid="set-123")

    assert detail.product_name == "Example Product"
    assert detail.labeler == "Example Labeler"
    assert detail.route == "ORAL"
    assert detail.dosage_form == "TABLET"
    assert detail.ndcs == ["12345-678-90"]
    assert [entry.display_name for entry in detail.active_ingredients] == ["metformin hydrochloride"]
    assert detail.active_ingredients[0].role == "active"
    assert [entry.display_name for entry in detail.inactive_ingredients] == [
        "lactose monohydrate",
        "magnesium stearate",
    ]
    assert [entry.source_type for entry in detail.inactive_ingredients] == [
        "xml_structured",
        "xml_structured",
    ]


def test_dailymed_product_detail_prefers_structured_tables_for_childrens_pain_reliever_sample():
    xml = """
    <document xmlns="urn:hl7-org:v3">
        <title>Children's Pain Reliever</title>
        <section>
            <title>Active Ingredient/Active Moiety</title>
            <text>
                <table>
                    <tr>
                        <th>Active Ingredient</th>
                        <th>Strength</th>
                        <th>UNII</th>
                    </tr>
                    <tr>
                        <td>Acetaminophen (UNII: 362O9ITL9D)</td>
                        <td>160 mg/5 mL</td>
                        <td>362O9ITL9D</td>
                    </tr>
                </table>
            </text>
        </section>
        <section>
            <title>Inactive Ingredients</title>
            <text>
                <table>
                    <tr>
                        <th>Ingredient Name</th>
                        <th>UNII</th>
                    </tr>
                    <tr>
                        <td>Mannitol</td>
                        <td>3OWL53L36A</td>
                    </tr>
                    <tr>
                        <td>FD&amp;C Red No. 40</td>
                        <td>WZB9127XOA</td>
                    </tr>
                    <tr>
                        <td>D&amp;C Red No. 33</td>
                        <td>9DBA0SBB0L</td>
                    </tr>
                </table>
                <paragraph>inactive ingredients narrative should not win</paragraph>
            </text>
        </section>
    </document>
    """

    repository = DailyMedAPIRepository(Settings(), httpx.AsyncClient(), ParsingService())
    detail = repository._parse_product_detail(xml_text=xml, setid="set-children")

    assert len(detail.active_ingredients) == 1
    active = detail.active_ingredients[0]
    assert active.display_name == "Acetaminophen"
    assert active.unii == "362O9ITL9D"
    assert active.strength == "160 mg/5 mL"
    assert active.role == "active"
    assert active.source_type == "table"
    assert active.confidence == "high"
    assert active.raw_name == "Acetaminophen (UNII: 362O9ITL9D) | 160 mg/5 mL | 362O9ITL9D"

    assert [entry.display_name for entry in detail.inactive_ingredients] == [
        "Mannitol",
        "FD&C Red No. 40",
        "D&C Red No. 33",
    ]
    assert [entry.unii for entry in detail.inactive_ingredients] == [
        "3OWL53L36A",
        "WZB9127XOA",
        "9DBA0SBB0L",
    ]
    assert all(entry.role == "inactive" for entry in detail.inactive_ingredients)
    assert all(entry.source_type == "table" for entry in detail.inactive_ingredients)


def test_dailymed_product_detail_uses_structured_product_data_when_document_title_is_highlights():
    xml = """
    <document xmlns="urn:hl7-org:v3">
        <title>These highlights do not include all the information needed to use FLUOXETINE ORAL SOLUTION safely and effectively.</title>
        <author>
            <assignedEntity>
                <representedOrganization>
                    <name>Upsher-Smith Laboratories, LLC</name>
                </representedOrganization>
            </assignedEntity>
        </author>
        <routeCode displayName="ORAL" />
        <component>
            <structuredBody>
                <component>
                    <section>
                        <code displayName="SPL PRODUCT DATA ELEMENTS SECTION" />
                        <subject>
                            <manufacturedProduct>
                                <manufacturedProduct>
                                    <code code="0832-6032" />
                                    <name>Fluoxetine hydrochloride</name>
                                    <formCode displayName="FOR SOLUTION" />
                                    <ingredient classCode="ACTIM">
                                        <quantity>
                                            <numerator value="20" unit="mg" />
                                            <denominator value="5" unit="mL" />
                                        </quantity>
                                        <ingredientSubstance>
                                            <code code="I9W7N6B1KJ" />
                                            <name>FLUOXETINE HYDROCHLORIDE</name>
                                        </ingredientSubstance>
                                    </ingredient>
                                    <ingredient classCode="IACT">
                                        <ingredientSubstance>
                                            <code code="3K9958V90M" />
                                            <name>alcohol</name>
                                        </ingredientSubstance>
                                    </ingredient>
                                    <ingredient classCode="IACT">
                                        <ingredientSubstance>
                                            <code code="8SKN0B0MIM" />
                                            <name>benzoic acid</name>
                                        </ingredientSubstance>
                                    </ingredient>
                                    <ingredient classCode="IACT">
                                        <ingredientSubstance>
                                            <code code="PDC6A3C0OX" />
                                            <name>GLYCERIN</name>
                                        </ingredientSubstance>
                                    </ingredient>
                                    <asContent>
                                        <containerPackagedProduct>
                                            <asContent>
                                                <containerPackagedProduct>
                                                    <code code="0832-6032-12" />
                                                </containerPackagedProduct>
                                            </asContent>
                                        </containerPackagedProduct>
                                    </asContent>
                                </manufacturedProduct>
                            </manufacturedProduct>
                        </subject>
                    </section>
                </component>
            </structuredBody>
        </component>
    </document>
    """

    repository = DailyMedAPIRepository(Settings(), httpx.AsyncClient(), ParsingService())
    detail = repository._parse_product_detail(xml_text=xml, setid="set-fluoxetine")

    assert detail.product_name == "Fluoxetine hydrochloride FOR SOLUTION"
    assert detail.labeler == "Upsher-Smith Laboratories, LLC"
    assert detail.route == "ORAL"
    assert detail.dosage_form == "FOR SOLUTION"
    assert detail.ndcs == ["0832-6032-12"]
    assert [entry.display_name for entry in detail.active_ingredients] == ["FLUOXETINE HYDROCHLORIDE"]
    assert detail.active_ingredients[0].unii == "I9W7N6B1KJ"
    assert detail.active_ingredients[0].strength == "20 mg/5 mL"
    assert [entry.display_name for entry in detail.inactive_ingredients] == [
        "alcohol",
        "benzoic acid",
        "GLYCERIN",
    ]
    assert [entry.unii for entry in detail.inactive_ingredients] == [
        "3K9958V90M",
        "8SKN0B0MIM",
        "PDC6A3C0OX",
    ]
    assert all(entry.source_type == "xml_structured" for entry in detail.inactive_ingredients)


def test_build_all_product_details_expands_multi_strength_spl():
    """An SPL with multiple subjects yields one ProductDetail per subject."""
    xml = """
    <document xmlns="urn:hl7-org:v3">
        <title>KEPPRA (LEVETIRACETAM) TABLET, FILM COATED KEPPRA (LEVETIRACETAM) SOLUTION</title>
        <author>
            <assignedEntity>
                <representedOrganization><name>UCB, INC.</name></representedOrganization>
            </assignedEntity>
        </author>
        <routeCode displayName="ORAL" />
        <component>
            <structuredBody>
                <component>
                    <section>
                        <code displayName="SPL PRODUCT DATA ELEMENTS SECTION" />
                        <subject>
                            <manufacturedProduct>
                                <manufacturedProduct>
                                    <name>Keppra</name>
                                    <formCode displayName="TABLET, FILM COATED" />
                                    <ingredient classCode="ACTIM">
                                        <quantity>
                                            <numerator value="250" unit="mg" />
                                            <denominator value="1" unit="1" />
                                        </quantity>
                                        <ingredientSubstance>
                                            <code code="44YRR34555" />
                                            <name>LEVETIRACETAM</name>
                                        </ingredientSubstance>
                                    </ingredient>
                                    <ingredient classCode="IACT">
                                        <ingredientSubstance>
                                            <code code="FDB7HW3TYE" />
                                            <name>INDIGOTINDISULFONATE SODIUM</name>
                                        </ingredientSubstance>
                                    </ingredient>
                                    <ingredient classCode="IACT">
                                        <ingredientSubstance>
                                            <code code="3K9958V90M" />
                                            <name>MAGNESIUM STEARATE</name>
                                        </ingredientSubstance>
                                    </ingredient>
                                    <asContent>
                                        <containerPackagedProduct>
                                            <code code="50474-594-40" />
                                        </containerPackagedProduct>
                                    </asContent>
                                </manufacturedProduct>
                            </manufacturedProduct>
                        </subject>
                        <subject>
                            <manufacturedProduct>
                                <manufacturedProduct>
                                    <name>Keppra</name>
                                    <formCode displayName="SOLUTION" />
                                    <ingredient classCode="ACTIM">
                                        <quantity>
                                            <numerator value="100" unit="mg" />
                                            <denominator value="1" unit="mL" />
                                        </quantity>
                                        <ingredientSubstance>
                                            <code code="44YRR34555" />
                                            <name>LEVETIRACETAM</name>
                                        </ingredientSubstance>
                                    </ingredient>
                                    <ingredient classCode="IACT">
                                        <ingredientSubstance>
                                            <code code="PDC6A3C0OX" />
                                            <name>GLYCERIN</name>
                                        </ingredientSubstance>
                                    </ingredient>
                                    <asContent>
                                        <containerPackagedProduct>
                                            <code code="50474-001-48" />
                                        </containerPackagedProduct>
                                    </asContent>
                                </manufacturedProduct>
                            </manufacturedProduct>
                        </subject>
                    </section>
                </component>
            </structuredBody>
        </component>
    </document>
    """

    details = build_all_product_details_from_xml(
        xml_text=xml,
        setid="keppra-setid",
        base_url="https://dailymed.nlm.nih.gov",
        parsing_service=ParsingService(),
    )

    assert len(details) == 2

    tablet = details[0]
    assert "250 mg" in tablet.product_name
    assert "TABLET" in tablet.product_name
    assert tablet.dosage_form == "TABLET, FILM COATED"
    assert tablet.ndcs == ["50474-594-40"]
    assert tablet.labeler == "UCB, INC."
    assert tablet.route == "ORAL"
    assert [e.display_name for e in tablet.active_ingredients] == ["LEVETIRACETAM"]
    assert tablet.active_ingredients[0].strength == "250 mg"
    assert [e.display_name for e in tablet.inactive_ingredients] == [
        "INDIGOTINDISULFONATE SODIUM",
        "MAGNESIUM STEARATE",
    ]

    solution = details[1]
    assert "100 mg" in solution.product_name
    assert "SOLUTION" in solution.product_name
    assert solution.dosage_form == "SOLUTION"
    assert solution.ndcs == ["50474-001-48"]
    assert [e.display_name for e in solution.inactive_ingredients] == ["GLYCERIN"]
    assert solution.active_ingredients[0].strength == "100 mg/1 mL"


def test_legacy_manufactured_medicine_spl_parses_name_and_ingredients():
    xml = """
    <document xmlns="urn:hl7-org:v3">
        <title mediaType="text/x-hl7-title+xml">
            <content styleCode="bold">METFORMIN HYDROCHLORIDE TABLETS</content>
        </title>
        <author>
            <assignedEntity>
                <representedOrganization><name>Actavis Elizabeth LLC</name></representedOrganization>
            </assignedEntity>
        </author>
        <component>
            <structuredBody>
                <component>
                    <section>
                        <subject>
                            <manufacturedProduct>
                                <manufacturedMedicine>
                                    <code code="0228-2657" />
                                    <name>metformin hydrochloride</name>
                                    <formCode displayName="TABLET" />
                                    <activeIngredient>
                                        <quantity>
                                            <numerator value="500" unit="mg" />
                                            <denominator value="1" unit="1" />
                                        </quantity>
                                        <activeIngredientSubstance>
                                            <code code="786Z46389E" />
                                            <name>metformin hydrochloride</name>
                                        </activeIngredientSubstance>
                                    </activeIngredient>
                                    <inactiveIngredient>
                                        <inactiveIngredientSubstance>
                                            <name>crospovidone</name>
                                        </inactiveIngredientSubstance>
                                    </inactiveIngredient>
                                    <inactiveIngredient>
                                        <inactiveIngredientSubstance>
                                            <name>povidone</name>
                                        </inactiveIngredientSubstance>
                                    </inactiveIngredient>
                                </manufacturedMedicine>
                                <routeCode displayName="ORAL" />
                            </manufacturedProduct>
                        </subject>
                    </section>
                </component>
            </structuredBody>
        </component>
    </document>
    """

    detail = build_product_detail_from_xml(
        xml_text=xml,
        setid="legacy-setid",
        base_url="https://dailymed.nlm.nih.gov",
        parsing_service=ParsingService(),
    )
    assert detail.product_name == "metformin hydrochloride TABLET"
    assert detail.labeler == "Actavis Elizabeth LLC"
    assert detail.route == "ORAL"
    assert detail.dosage_form == "TABLET"
    assert [entry.display_name for entry in detail.active_ingredients] == ["metformin hydrochloride"]
    assert detail.active_ingredients[0].strength == "500 mg"
    assert [entry.display_name for entry in detail.inactive_ingredients] == ["crospovidone", "povidone"]

    details = build_all_product_details_from_xml(
        xml_text=xml,
        setid="legacy-setid",
        base_url="https://dailymed.nlm.nih.gov",
        parsing_service=ParsingService(),
    )
    assert len(details) == 1
    assert details[0].product_name == "metformin hydrochloride 500 mg TABLET"
    assert [entry.display_name for entry in details[0].inactive_ingredients] == ["crospovidone", "povidone"]
