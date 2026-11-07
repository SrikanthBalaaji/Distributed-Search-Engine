import math

def compute_bm25(query_terms: list[str], docs_df, index_df, k1: float = 1.2, b: float = 0.75) -> dict[str, float]:
    """
    Computes BM25 relevance scores for all documents matching the query terms.
    
    docs_df:  Contains ['doc_id', 'length'] (total word count per doc)
    index_df: Inverted index containing ['term', 'doc_id', 'tf', 'df']
    """
    n_docs = len(docs_df)
    avgdl = max(float(docs_df["length"].mean()), 1.0)
    scores = {str(doc_id): 0.0 for doc_id in docs_df["doc_id"]}

    for term in query_terms:
        # 1. Fetch matching rows from inverted index for this query term
        term_rows = index_df[index_df["term"] == term]
        if term_rows.empty:
            continue

        # 2. Compute IDF for the term across the corpus
        df = int(term_rows["df"].iloc[0])
        idf = math.log(1.0 + (n_docs - df + 0.5) / (df + 0.5))

        # 3. Calculate BM25 contribution for each document that contains the term
        for _, row in term_rows.iterrows():
            doc_id = str(row["doc_id"])
            tf = float(row["tf"])
            doc_len = float(row["length"])

            # Term frequency saturation & length normalization
            numerator = tf * (k1 + 1.0)
            denominator = tf + k1 * (1.0 - b + b * (doc_len / avgdl))

            scores[doc_id] += idf * (numerator / denominator)

    return scores