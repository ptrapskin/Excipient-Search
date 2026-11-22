import asyncio

from app.domain.enums import DataSource
from app.domain.models import DrugQuery
from app.repositories.product_index import ProductIndexRepository
from app.services.excipient_filter import ExcipientFilterService
from app.services.search_service import SearchService
from excipient_finder.models import SplRecord
from excipient_finder.product_index import init_index_db, insert_index_record


def _record(setid, name, active, strength, inactive, form="TABLET", ndcs=("11111-222-33",)):
    return SplRecord(
        setid=setid,
        product_name=name,
        dosage_form=form,
        route="ORAL",
        labeler="Labeler",
        ndcs=list(ndcs),
        active_ingredients_raw=active,
        active_strength=strength,
        active_ingredients_unii=None,
        inactive_ingredients_raw=inactive,
        inactive_ingredient_entries=[],
        product_type="HUMAN PRESCRIPTION DRUG LABEL",
        source_file="sample.zip",
    )


def _build_index(tmp_path):
    db_path = tmp_path / "product_index.db"
    conn = init_index_db(db_path)
    insert_index_record(conn, _record("set-1", "Metformin Hydrochloride", "METFORMIN HYDROCHLORIDE", "500 mg", "STARCH, CORN; MAGNESIUM STEARATE"))
    insert_index_record(conn, _record("set-2", "Metformin Hydrochloride", "METFORMIN HYDROCHLORIDE", "1000 mg", "STARCH; POVIDONE", ndcs=("44444-555-66",)))
    insert_index_record(conn, _record("set-3", "Keppra", "LEVETIRACETAM", "500 mg", "STARCH, WHEAT"))
    conn.commit()
    conn.close()
    return ProductIndexRepository(db_path)


def test_index_search_matches_all_query_terms(tmp_path):
    index = _build_index(tmp_path)

    names = lambda q: sorted(d.setid for _, d in index.search(DrugQuery(raw_text=q, normalized_text=q)))

    assert names("metformin") == ["set-1", "set-2"]
    assert names("metformin 1000 mg") == ["set-2"]
    assert names("lisinopril") == []


def test_index_search_by_ndc_and_alias(tmp_path):
    index = _build_index(tmp_path)

    by_ndc = index.search(DrugQuery(raw_text="44444-555-66", normalized_text="44444-555-66", ndc="44444-555-66"))
    by_alias = index.search(DrugQuery(raw_text="brandname", normalized_text="brandname"), ["levetiracetam"])

    assert [d.setid for _, d in by_ndc] == ["set-2"]
    assert [d.setid for _, d in by_alias] == ["set-3"]


def test_index_rows_carry_parsed_ingredients(tmp_path):
    result, detail = _build_index(tmp_path).search(DrugQuery(raw_text="keppra", normalized_text="keppra"))[0]

    assert result.source == DataSource.LOCAL_INDEX
    assert [i.display_name for i in detail.inactive_ingredients] == ["STARCH, WHEAT"]
    assert detail.active_ingredients[0].strength == "500 mg"


def test_missing_index_is_unavailable(tmp_path):
    assert ProductIndexRepository(tmp_path / "missing.db").available is False


class _NoAliases:
    async def resolve(self, query):
        return []


class _NoLiveCalls:
    def __getattr__(self, name):
        raise AssertionError(f"live DailyMed call {name} made despite index hit")


def test_search_service_uses_index_without_live_calls(tmp_path):
    service = SearchService(None, _NoLiveCalls(), _NoAliases(), ExcipientFilterService(), product_index=_build_index(tmp_path))

    query, results, rows, _, matching, cached, unavailable = asyncio.run(
        service.search_with_excipients("metformin", exclude_terms="povidone")
    )

    assert {r.setid for r in rows} == {"set-1", "set-2"}
    assert [r.setid for r in matching] == ["set-1"]
    assert unavailable == 0


def test_compressed_export_round_trips_to_search(tmp_path):
    from app.repositories.product_index import resolve_index_path
    from excipient_finder.product_index import export_compressed

    build = _build_index(tmp_path)
    deploy_dir = tmp_path / "deploy"
    deploy_dir.mkdir()
    gz_path = export_compressed(tmp_path / "product_index.db", deploy_dir / "product_index.db.gz")

    db_path = deploy_dir / "product_index.db"
    unpacked = resolve_index_path(db_path, unpack_dir=tmp_path / "unpacked")

    assert not db_path.exists()
    assert unpacked.exists() and unpacked != db_path
    assert resolve_index_path(db_path, unpack_dir=tmp_path / "unpacked") == unpacked
    repo = ProductIndexRepository(unpacked)
    assert repo.available
    assert sorted(d.setid for _, d in repo.search(DrugQuery(raw_text="metformin", normalized_text="metformin"))) == ["set-1", "set-2"]


def test_resolve_index_path_prefers_plain_db(tmp_path):
    from app.repositories.product_index import resolve_index_path

    _build_index(tmp_path)
    db_path = tmp_path / "product_index.db"

    assert resolve_index_path(db_path) == db_path
    assert resolve_index_path(tmp_path / "missing.db") == tmp_path / "missing.db"


def test_parser_uses_document_set_id_not_file_name():
    from excipient_finder.xml_parser import parse_spl_subjects

    xml = """<?xml version="1.0"?>
<document xmlns="urn:hl7-org:v3">
  <id root="version-id-1"/>
  <setId root="real-set-id"/>
  <component><structuredBody><component><section><subject><manufacturedProduct>
    <manufacturedProduct><name>Example</name><formCode displayName="TABLET"/></manufacturedProduct>
  </manufacturedProduct></subject></section></component></structuredBody></component>
</document>"""

    records = parse_spl_subjects(xml, "version-id-1", "sample.zip")

    assert [r.setid for r in records] == ["real-set-id"]
