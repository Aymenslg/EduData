import fitz

def extract_pdf_text(path):
    doc = fitz.open(path)
    return [{"page": i + 1, "text": page.get_text()} for i, page in enumerate(doc)]
