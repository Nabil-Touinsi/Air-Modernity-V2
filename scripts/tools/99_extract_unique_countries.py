from pathlib import Path
import pandas as pd

IN_CSV = Path("data/interim/flightradar24_clean.csv")
OUT = Path("data/ref/countries_from_dataset.csv")

df = pd.read_csv(IN_CSV)

if "country" not in df.columns:
    raise ValueError("Colonne 'country' introuvable dans le dataset clean.")

countries = (
    df["country"]
    .dropna()
    .astype(str)
    .sort_values()
    .unique()
)

pd.DataFrame({"country": countries}).to_csv(OUT, index=False, encoding="utf-8")

print(f"{len(countries)} pays extraits")
print(f"Sortie : {OUT}")
