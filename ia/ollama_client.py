import requests
from config import OLLAMA_BASE_URL, OLLAMA_MODEL

class OllamaClient:
    def status(self):
        try:
            response = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=3)
            response.raise_for_status()
            models = [m.get("name", "") for m in response.json().get("models", [])]
            return {"available": True, "model": OLLAMA_MODEL, "installed_models": models}
        except Exception as exc:
            return {"available": False, "model": OLLAMA_MODEL, "error": str(exc)}

    def generate(self, prompt):
        response = requests.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            timeout=90,
        )
        response.raise_for_status()
        return response.json().get("response", "")

    def generate_json(self, prompt):
        response = requests.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "format": "json",
                  "stream": False, "options": {"temperature": 0}},
            timeout=240,
        )
        response.raise_for_status()
        return response.json().get("response", "")
