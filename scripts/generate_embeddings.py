"""
generate_embeddings.py

Futurology card data -> multilingual-e5-small embedding -> UMAP 2D reduction
Output: data/futurology/embeddings_2d.json

Usage:
    python3 scripts/generate_embeddings.py
"""

import json
import numpy as np
from pathlib import Path

# --- Paths ---
ROOT = Path(__file__).parent.parent
DATA_PATH = ROOT / "data" / "futurology" / "futurology_data.json"
OUT_PATH = ROOT / "data" / "futurology" / "embeddings_2d.json"

# --- Load data ---
print("Loading futurology data...")
with open(DATA_PATH, encoding="utf-8") as f:
    data = json.load(f)

cards = []
for item in data["dai_items"]:
    dai_name = item["name"]
    map_theme = item.get("mapTheme", "")
    for chu_name, card_list in item["cardsByChu"].items():
        for card in card_list:
            cards.append({
                "summary": card.get("summary", ""),
                "dai": card.get("dai", dai_name),
                "theme": card.get("theme", map_theme),
                "nen": card.get("nen", ""),
                "dai_no": item["no"],
                "chu": chu_name,
                "sources": [s.get("book", "") for s in card.get("sources", [])],
            })

print(f"Total cards loaded: {len(cards)}")

# --- Embedding ---
from sentence_transformers import SentenceTransformer

print("Loading multilingual-e5-small model (first run downloads ~400MB)...")
model = SentenceTransformer("intfloat/multilingual-e5-small")

# e5 models expect "query: " or "passage: " prefix
# For search/retrieval use "passage: " for documents
texts = ["passage: " + c["summary"] for c in cards]

print("Generating embeddings (this may take 1-3 minutes)...")
embeddings = model.encode(texts, batch_size=64, show_progress_bar=True, normalize_embeddings=True)
print(f"Embedding shape: {embeddings.shape}")

# --- UMAP ---
import umap

print("Running UMAP dimensionality reduction...")
reducer = umap.UMAP(
    n_components=2,
    n_neighbors=15,   # balanced for 553 points: local vs global structure
    min_dist=0.1,     # moderate clustering density
    metric="cosine",  # optimal for normalized text embeddings
    random_state=42,  # reproducibility
    low_memory=False,
)
coords_2d = reducer.fit_transform(embeddings)
print(f"UMAP output shape: {coords_2d.shape}")

# --- Build output JSON ---
print("Building output JSON...")

# Collect unique values for color mapping
all_dais = list(dict.fromkeys(c["dai"] for c in cards))   # preserve insertion order
all_themes = list(dict.fromkeys(c["theme"] for c in cards))

output = {
    "meta": {
        "model": "intfloat/multilingual-e5-small",
        "umap_params": {
            "n_neighbors": 15,
            "min_dist": 0.1,
            "metric": "cosine",
            "random_state": 42,
        },
        "total_cards": len(cards),
        "dai_list": all_dais,
        "theme_list": all_themes,
    },
    "points": []
}

for i, card in enumerate(cards):
    output["points"].append({
        "x": float(coords_2d[i, 0]),
        "y": float(coords_2d[i, 1]),
        "summary": card["summary"],
        "dai": card["dai"],
        "theme": card["theme"],
        "nen": card["nen"],
        "chu": card["chu"],
        "dai_no": card["dai_no"],
        "books": card["sources"],
    })

# --- Save ---
with open(OUT_PATH, "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, separators=(",", ":"))

size_kb = OUT_PATH.stat().st_size / 1024
print(f"\nSaved: {OUT_PATH}")
print(f"File size: {size_kb:.1f} KB")
print(f"Points: {len(output['points'])}")
print("Done.")
