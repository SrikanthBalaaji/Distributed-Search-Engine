from dse.textproc import clean_wikitext, document_terms, query_terms, stem, tokenize


def test_stemmer_plurals():
    assert stem("plants") == "plant"
    assert stem("cities") == "city"
    assert stem("trees") == "tree"
    assert stem("glass") == "glass"
    assert stem("virus") == "virus"
    assert stem("gas") == "gas"


def test_tokenize_drops_stopwords_and_short_tokens():
    assert tokenize("A seed becomes a plant!") == ["seed", "become", "plant"]
    assert tokenize("The of and") == []


def test_query_and_document_terms_agree():
    doc = document_terms("Plants", "Many '''plants''' grow in [[soil|soils]].")
    for t in query_terms("plant soil"):
        assert t in doc


def test_clean_wikitext_links_templates_refs():
    raw = ("{{Infobox|x={{nested}}}}'''April''' is a [[month]] of the [[Gregorian calendar|calendar]]."
           "<ref>cite</ref> [[File:x.jpg|thumb|a [[flower]] picture]] [[Category:Months]]"
           "\n== History ==\n[https://example.org Example site]")
    t = clean_wikitext(raw)
    assert "April is a month of the calendar." in t
    assert "Infobox" not in t and "cite" not in t and "File:" not in t
    assert "Category" not in t and "History" in t and "Example site" in t
    assert "[[" not in t and "{{" not in t


def test_link_suffix_plural():
    assert clean_wikitext("It has 30 [[day]]s.") == "It has 30 days."


def test_query_terms_dedup():
    assert query_terms("plant plants PLANT") == ["plant"]
