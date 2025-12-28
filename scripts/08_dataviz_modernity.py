from __future__ import annotations

from pathlib import Path
from textwrap import fill

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from matplotlib.ticker import FuncFormatter
import pandas as pd
from sqlalchemy import create_engine

# ============================================================
# Étape 8 — Dataviz (extraction MySQL + graphiques)
# ============================================================

OUT_DIR = Path("data/exports")

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3307
MYSQL_USER = "root"
MYSQL_PASSWORD = "0000"
MYSQL_DB = "air_modernity"


# ----------------------------
# "Style pro" centralisé
# ----------------------------
def set_plot_style() -> None:
    plt.rcParams.update({
        "figure.dpi": 120,
        "savefig.dpi": 220,
        "font.size": 12,
        "axes.titlesize": 16,
        "axes.labelsize": 12,
        "xtick.labelsize": 11,
        "ytick.labelsize": 12,
        "axes.grid": True,
        "grid.alpha": 0.22,
        "grid.linestyle": "-",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.titlepad": 10,
    })


def ensure_dirs() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def get_engine():
    url = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    )
    return create_engine(url)


def read_sql(query: str) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def _safe_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _has_any_value(s: pd.Series) -> bool:
    return s is not None and s.dropna().shape[0] > 0


def _all_zero_or_na(s: pd.Series) -> bool:
    s2 = s.dropna()
    return s2.empty or (s2.abs().sum() == 0)


# ----------------------------
# Formatters pro
# ----------------------------
def fmt_int(x: float) -> str:
    try:
        return f"{int(round(x)):,}".replace(",", " ")
    except Exception:
        return ""


def fmt_k(x: float) -> str:
    # 12357 -> 12.4k
    try:
        if x >= 1_000_000:
            return f"{x/1_000_000:.1f}M"
        if x >= 1_000:
            return f"{x/1_000:.1f}k"
        return f"{int(round(x))}"
    except Exception:
        return ""


def fmt_pct(x: float) -> str:
    try:
        return f"{x:.2f}%"
    except Exception:
        return ""


def wrap_labels(series: pd.Series, width: int = 18) -> pd.Series:
    return series.astype(str).apply(lambda s: fill(s, width=width))


def _filter_non_empty_labels(df: pd.DataFrame, label_col: str) -> pd.DataFrame:
    """Retire les labels NULL / vides / espaces (pro + évite les barres sans nom)."""
    if df is None or df.empty or label_col not in df.columns:
        return df
    d = df.copy()
    d[label_col] = d[label_col].astype(str)
    d = d[d[label_col].notna()]
    d = d[d[label_col].str.strip() != ""]
    return d


# ----------------------------
# Plots pro
# ----------------------------
def barh_pro(
    df: pd.DataFrame,
    label_col: str,
    value_col: str,
    title: str,
    xlabel: str,
    out_name: str,
    *,
    sort_desc: bool = True,
    logx: bool = False,
    label_wrap: int = 18,
    value_kind: str = "int",   # "int" | "pct" | "k"
) -> None:
    if df is None or df.empty:
        return
    if label_col not in df.columns or value_col not in df.columns:
        return
    if not _has_any_value(df[value_col]) or _all_zero_or_na(df[value_col]):
        return

    d = df[[label_col, value_col]].copy()
    d = d.dropna(subset=[label_col, value_col])
    d = _filter_non_empty_labels(d, label_col)
    if d.empty:
        return

    d[label_col] = wrap_labels(d[label_col], width=label_wrap)
    d = d.sort_values(value_col, ascending=not sort_desc)

    vals = d[value_col].astype(float).tolist()

    # Taille figure dynamique
    h = max(4.2, 0.72 * len(d))
    fig, ax = plt.subplots(figsize=(11.2, h))

    ax.barh(d[label_col], d[value_col])
    ax.invert_yaxis()

    ax.set_title(title)
    ax.set_xlabel(xlabel)

    if logx:
        ax.set_xscale("log")
        ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos: fmt_k(x)))

    if value_kind == "pct" and not logx:
        ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=100, decimals=0))
    elif value_kind in ("int", "k") and not logx:
        ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos: fmt_int(x)))

    xmax = max(v for v in vals if pd.notna(v))
    for i, v in enumerate(vals):
        if pd.isna(v):
            continue

        if value_kind == "pct":
            txt = fmt_pct(v)
        elif value_kind == "k":
            txt = fmt_k(v)
        else:
            txt = fmt_int(v)

        x = (v * 1.10) if logx else (v + (xmax * 0.015))
        ax.text(x, i, txt, va="center", ha="left")

    ax.grid(axis="x", alpha=0.22)
    ax.grid(axis="y", visible=False)

    fig.tight_layout()
    fig.savefig(OUT_DIR / out_name, bbox_inches="tight")
    plt.close(fig)


def scatter_pro(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    xlabel: str,
    ylabel: str,
    out_name: str,
    *,
    logx: bool = True,
) -> None:
    if df is None or df.empty:
        return
    if x not in df.columns or y not in df.columns:
        return
    d = df[[x, y]].copy().dropna()
    if d.empty:
        return

    fig, ax = plt.subplots(figsize=(9.2, 5.6))
    ax.scatter(d[x], d[y], alpha=0.75)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)

    ax.grid(True, alpha=0.22)
    if logx:
        ax.set_xscale("log")
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, pos: fmt_k(v)))

    fig.tight_layout()
    fig.savefig(OUT_DIR / out_name, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    ensure_dirs()
    set_plot_style()

    # 1) Extraction des vues
    df_region = read_sql("SELECT * FROM v_modernity_by_region;")
    df_cat = read_sql("SELECT * FROM v_modernity_by_aircraft_category;")
    df_oem = read_sql("SELECT * FROM v_modernity_by_oem_commercial;")
    df_airline = read_sql("SELECT * FROM v_modernity_score_by_airline;")

    # 2) Nettoyage types
    df_region = _safe_numeric(df_region, ["aircraft_count", "avg_entry_year", "pct_modern_2015", "pct_modern_2010"])
    df_cat = _safe_numeric(df_cat, ["aircraft_count", "avg_entry_year", "pct_modern_2015", "pct_modern_2010"])
    df_oem = _safe_numeric(df_oem, ["aircraft_count_total", "aircraft_with_year", "avg_entry_year", "pct_modern_2015", "pct_modern_2010"])
    df_airline = _safe_numeric(df_airline, ["aircraft_count", "avg_entry_year", "pct_modern_2015", "pct_modern_2010", "modernity_score"])

    # IMPORTANT : éviter les barres sans label (region vide)
    df_region = _filter_non_empty_labels(df_region, "region")
    df_cat = _filter_non_empty_labels(df_cat, "aircraft_category")
    df_oem = _filter_non_empty_labels(df_oem, "oem")

    # 3) Export CSV
    df_region.to_csv(OUT_DIR / "v_modernity_by_region.csv", index=False, encoding="utf-8")
    df_cat.to_csv(OUT_DIR / "v_modernity_by_aircraft_category.csv", index=False, encoding="utf-8")
    df_oem.to_csv(OUT_DIR / "v_modernity_by_oem_commercial.csv", index=False, encoding="utf-8")
    df_airline.to_csv(OUT_DIR / "v_modernity_score_by_airline.csv", index=False, encoding="utf-8")

    # 4) coverage_pct OEM
    if not df_oem.empty and {"aircraft_count_total", "aircraft_with_year"}.issubset(df_oem.columns):
        df_oem = df_oem.copy()
        df_oem["coverage_pct"] = (100 * df_oem["aircraft_with_year"] / df_oem["aircraft_count_total"]).round(2)

    # ------------------------------------------------------------
    # GRAPHS NECESSAIRES + LISIBLES (on retire OEM modern share)
    # ------------------------------------------------------------

    # Région: % modern >=2015
    barh_pro(
        df_region,
        label_col="region",
        value_col="pct_modern_2015",
        title="Modern fleet share by region (>=2015)",
        xlabel="Share of aircraft entered into service since 2015",
        out_name="fig_region_pct_modern_2015_pro.png",
        value_kind="pct",
        label_wrap=22,
    )

    # Catégorie: counts
    barh_pro(
        df_cat,
        label_col="aircraft_category",
        value_col="aircraft_count",
        title="Dataset composition by aircraft category",
        xlabel="Number of aircraft (with usable entry_year)",
        out_name="fig_category_counts_pro.png",
        value_kind="int",
        label_wrap=26,
    )

    # OEM total (log) — indispensable
    barh_pro(
        df_oem,
        label_col="oem",
        value_col="aircraft_count_total",
        title="Commercial OEM presence (total aircraft) — log scale",
        xlabel="Total aircraft (log scale)",
        out_name="fig_oem_total_count_pro.png",
        value_kind="k",
        logx=True,
        label_wrap=18,
    )

    # OEM coverage %
    if "coverage_pct" in df_oem.columns:
        barh_pro(
            df_oem,
            label_col="oem",
            value_col="coverage_pct",
            title="OEM coverage of usable entry_year",
            xlabel="Coverage (aircraft with entry_year / total)",
            out_name="fig_oem_coverage_pct_pro.png",
            value_kind="pct",
            label_wrap=18,
        )

    # Airlines scatter (log x) — modernity_score vs fleet size
    scatter_pro(
        df_airline,
        x="aircraft_count",
        y="modernity_score",
        title="Modernity score vs fleet size (airlines)",
        xlabel="Fleet size (log scale)",
        ylabel="Modernity score",
        out_name="fig_airline_score_vs_size_pro.png",
        logx=True,
    )

    print("OK ✅ Exports générés dans :", OUT_DIR.resolve())


if __name__ == "__main__":
    main()
