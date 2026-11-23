from backend.ingestion.pdf_processor import extract_pdf_text

def search_pdf(path, keywords, max_results=8):
    terms = [t.lower() for t in keywords if t.strip()]
    matches = []
    for page in extract_pdf_text(path):
        text = page["text"]
        lowered = text.lower()
        if any(term in lowered for term in terms):
            matches.append({"page": page["page"], "excerpt": text[:1000]})
        if len(matches) >= max_results:
            break
    return matches
