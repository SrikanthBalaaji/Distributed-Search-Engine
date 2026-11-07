"""Text processing shared by the Spark indexer (Nodes 2/3) and the query side (Node 4).

The SAME functions must be used on both sides, otherwise query terms will not match
index terms. Pure Python + `re` only, so it runs on any Spark executor.

Pipeline:  raw wikitext -> clean_wikitext() -> tokenize() -> [terms]
           tokenize = lowercase -> split on non-alphanumerics -> drop stopwords /
                      1-char tokens -> light plural stemming (Harman S-stemmer)
"""
import html
import re

# --------------------------------------------------------------------------
# Wikitext cleaning
# --------------------------------------------------------------------------
_COMMENT = re.compile(r"<!--.*?-->", re.S)
_REF = re.compile(r"<ref[^>]*?/>|<ref[^>]*?>.*?</ref>", re.S | re.I)
_TEMPLATE = re.compile(r"\{\{[^{}]*\}\}")
_TABLE = re.compile(r"\{\|(?:(?!\{\|).)*?\|\}", re.S)
_LINK = re.compile(r"\[\[([^\[\]]*)\]\]")
_EXT_LINK = re.compile(r"\[(?:https?|ftp)://[^\s\]]+(?:\s+([^\]]*))?\]")
_BARE_URL = re.compile(r"https?://\S+")
_TAG = re.compile(r"<[^>]+>")
_HEADING = re.compile(r"^=+\s*(.*?)\s*=+\s*$", re.M)
_QUOTES = re.compile(r"'{2,}")
_LIST_MARK = re.compile(r"^[*#:;]+\s*", re.M)
_SPACES = re.compile(r"[ \t\r\f\v]+")
_BLANKS = re.compile(r"\n\s*\n+")
_NON_ARTICLE_NS = ("file:", "image:", "category:", "media:", "wikipedia:", "template:")


def _link_text(m):
    inner = m.group(1).strip()
    low = inner.lower().lstrip(":")
    if low.startswith(_NON_ARTICLE_NS):
        return " "
    # [[target|label]] -> label, [[target]] -> target ; no padding so "[[day]]s" -> "days"
    return inner.split("|")[-1]


def _repeat(pattern, repl, text, limit=10):
    for _ in range(limit):
        new = pattern.sub(repl, text)
        if new == text:
            break
        text = new
    return text


def clean_wikitext(text):
    """Turn MediaWiki markup into readable plain text."""
    if not text:
        return ""
    t = _COMMENT.sub(" ", text)
    t = _REF.sub(" ", t)
    t = _repeat(_TEMPLATE, " ", t)  # nested {{...}} removed innermost-first
    t = _repeat(_TABLE, " ", t)
    t = _repeat(_LINK, _link_text, t)  # nested [[File:..|[[x]]]] resolved innermost-first
    t = _EXT_LINK.sub(lambda m: m.group(1) or "", t)
    t = _BARE_URL.sub(" ", t)
    t = _TAG.sub(" ", t)
    t = _HEADING.sub(r"\1", t)
    t = _QUOTES.sub("", t)
    t = html.unescape(t)
    t = t.replace("{{", " ").replace("}}", " ").replace("[[", " ").replace("]]", " ")
    t = _LIST_MARK.sub("", t)
    t = _SPACES.sub(" ", t)
    t = _BLANKS.sub("\n", t)
    return t.strip()


# --------------------------------------------------------------------------
# Tokenizer
# --------------------------------------------------------------------------
TOKEN_RE = re.compile(r"[^\W_]+", re.UNICODE)

STOPWORDS = frozenset(
    """
    a about above after again against all also am an and any are as at be because been
    before being below between both but by can could did do does doing down during each
    few for from further had has have having he her here hers herself him himself his how
    i if in into is it its itself just me more most my myself no nor not now of off on
    once only or other our ours ourselves out over own same she should so some such than
    that the their theirs them themselves then there these they this those through to too
    under until up very was we were what when where which while who whom why will with
    would you your yours yourself yourselves
    """.split()
)

MIN_TOKEN_LEN = 2
MAX_TOKEN_LEN = 40


def stem(token):
    """Harman 'S' stemmer: conservative plural removal (plants->plant, cities->city)."""
    if len(token) <= 3 or token.isdigit():
        return token
    if token.endswith("ies") and not token.endswith(("eies", "aies")):
        return token[:-3] + "y"
    if token.endswith("es") and not token.endswith(("aes", "ees", "oes")):
        return token[:-1]
    if token.endswith("s") and not token.endswith(("us", "ss")):
        return token[:-1]
    return token


def normalize_token(raw):
    """Map one raw token to its index term, or None if it should be dropped."""
    tok = raw.lower()
    if len(tok) < MIN_TOKEN_LEN or len(tok) > MAX_TOKEN_LEN or tok in STOPWORDS:
        return None
    tok = stem(tok)
    if tok in STOPWORDS:
        return None
    return tok


def tokenize(text):
    """Plain text -> list of index terms (order preserved, duplicates kept)."""
    if not text:
        return []
    out = []
    for raw in TOKEN_RE.findall(text):
        t = normalize_token(raw)
        if t:
            out.append(t)
    return out


def document_terms(title, wikitext):
    """Terms indexed for a document: title + cleaned body."""
    return tokenize((title or "") + "\n" + clean_wikitext(wikitext or ""))


def query_terms(query):
    """Terms for a user query (deduplicated, order kept)."""
    seen, out = set(), []
    for t in tokenize(query):
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out
