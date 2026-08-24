"""
Dashboard generation.

Reads the gold CSV tables (small, aggregated) straight from the gold bucket
in MinIO with pandas, builds the charts with matplotlib and renders a single
self-contained HTML file (charts embedded as base64 PNGs, no external
dependency / no internet needed to view it) answering the four business
questions.
"""
import base64
import io
import os

import boto3
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from botocore.client import Config
from matplotlib.patches import Patch
from matplotlib.ticker import FuncFormatter

GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "gold")
GOLD_CSV_PREFIX = os.environ.get("GOLD_CSV_PREFIX", "toy_store/csv")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/opt/output")


def read_gold_csv(name):
    """Download one aggregated gold CSV object from MinIO into a DataFrame."""
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    obj = s3.get_object(Bucket=GOLD_BUCKET, Key=f"{GOLD_CSV_PREFIX}/{name}.csv")
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

COLORS = {
    "sessions": "#2FB8AC",
    "orders": "#FF6B4A",
    "revenue": "#2FB8AC",
    "conv": "#FF6B4A",
    "aov": "#FFC145",
    "bar": "#FF6B4A",
    "bar2": "#2FB8AC",
    "text": "#1B1F3B",
    "muted": "#8A8FA3",
    "grid": "#E4E6F1",
}

plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "axes.edgecolor": COLORS["grid"],
    "axes.grid": True,
    "grid.color": COLORS["grid"],
    "grid.linewidth": 0.8,
    "font.size": 11,
    "font.family": "sans-serif",
    "text.color": COLORS["text"],
    "axes.labelcolor": COLORS["text"],
    "xtick.color": COLORS["muted"],
    "ytick.color": COLORS["muted"],
    "axes.titlecolor": COLORS["text"],
    "axes.spines.top": False,
    "axes.spines.right": False,
    # Keep the grid behind the bars instead of slicing through them.
    "axes.axisbelow": True,
})


def fig_to_base64(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=140, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("ascii")


def chart_sessions_orders(monthly):
    fig, ax1 = plt.subplots(figsize=(9, 4))
    ax1.plot(monthly["year_month"], monthly["sessions"], color=COLORS["sessions"], marker="o", markersize=3, label="Sessions")
    ax1.set_ylabel("Sessions", color=COLORS["sessions"])
    ax1.tick_params(axis="y", labelcolor=COLORS["sessions"])
    ax1.tick_params(axis="x", rotation=90)

    ax2 = ax1.twinx()
    ax2.plot(monthly["year_month"], monthly["orders"], color=COLORS["orders"], marker="o", markersize=3, label="Commandes")
    ax2.set_ylabel("Commandes", color=COLORS["orders"])
    ax2.tick_params(axis="y", labelcolor=COLORS["orders"])
    ax2.grid(False)

    fig.suptitle("Sessions vs Commandes par mois")
    fig.tight_layout()
    return fig_to_base64(fig)


def chart_conversion(monthly):
    fig, ax = plt.subplots(figsize=(9, 3.5))
    ax.plot(monthly["year_month"], monthly["conversion_rate_pct"], color=COLORS["conv"], marker="o", markersize=3)
    ax.set_ylabel("Taux de conversion (%)")
    ax.tick_params(axis="x", rotation=90)
    ax.set_title("Taux de conversion session -> commande, par mois")
    fig.tight_layout()
    return fig_to_base64(fig)


def chart_aov(monthly):
    fig, ax = plt.subplots(figsize=(9, 3.5))
    ax.plot(monthly["year_month"], monthly["aov_usd"], color=COLORS["aov"], marker="o", markersize=3)
    ax.set_ylabel("AOV ($)")
    ax.tick_params(axis="x", rotation=90)
    ax.set_title("Panier moyen (Average Order Value) par mois")
    fig.tight_layout()
    return fig_to_base64(fig)


TYPE_COLORS = {
    "Payant": "#FF6B4A",
    "Naturel": "#2FB8AC",
    "Direct": "#FFC145",
}


def _type_legend(ax, types_present):
    handles = [
        Patch(facecolor=TYPE_COLORS[t], label=t)
        for t in ("Payant", "Naturel", "Direct")
        if t in types_present
    ]
    ax.legend(handles=handles, loc="lower right", frameon=False, fontsize=9)


def chart_channels(channels):
    top = channels.sort_values("revenue", ascending=False).iloc[::-1]
    colors = [TYPE_COLORS.get(t, COLORS["bar"]) for t in top["channel_type"]]
    fig, ax = plt.subplots(figsize=(9, 4.5))
    bars = ax.barh(top["channel"], top["revenue"], color=colors)
    ax.set_xlabel("Chiffre d'affaires ($)")
    ax.set_title("Chiffre d'affaires par canal d'acquisition")
    # Plain "$120k" ticks instead of matplotlib's 1e6 offset notation.
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"${v/1000:,.0f}k"))
    ax.bar_label(bars, labels=[f"${v/1000:,.0f}k" for v in top["revenue"]],
                 padding=4, fontsize=9, color=COLORS["muted"])
    ax.set_xlim(0, top["revenue"].max() * 1.18)
    _type_legend(ax, set(top["channel_type"]))
    fig.tight_layout()
    return fig_to_base64(fig)


def chart_channel_conversion(channels):
    top = channels[channels["sessions"] >= 100].sort_values("conversion_rate_pct").copy()
    colors = [TYPE_COLORS.get(t, COLORS["bar2"]) for t in top["channel_type"]]
    fig, ax = plt.subplots(figsize=(9, 4.5))
    bars = ax.barh(top["channel"], top["conversion_rate_pct"], color=colors)
    ax.set_xlabel("Taux de conversion (%)")
    ax.set_title("Taux de conversion par canal (>= 100 sessions)")
    ax.bar_label(bars, labels=[f"{v:.2f}%" for v in top["conversion_rate_pct"]],
                 padding=4, fontsize=9, color=COLORS["muted"])
    ax.set_xlim(0, top["conversion_rate_pct"].max() * 1.18)
    _type_legend(ax, set(top["channel_type"]))
    fig.tight_layout()
    return fig_to_base64(fig)


def build_table(df, columns=None, max_rows=15):
    if columns:
        df = df[columns]
    df = df.head(max_rows)
    return df.to_html(index=False, classes="data-table", border=0, float_format=lambda x: f"{x:,.2f}")


def main():
    monthly = read_gold_csv("gold_monthly_trend")
    channels = read_gold_csv("gold_channel_performance")

    monthly = monthly.sort_values("year_month")
    channels = channels.sort_values("revenue", ascending=False)

    # ---- narrative numbers -------------------------------------------------
    total_sessions = int(monthly["sessions"].sum())
    total_orders = int(monthly["orders"].sum())
    total_revenue = float(monthly["revenue"].sum())
    overall_conv = round(total_orders / total_sessions * 100, 2)

    first_months = monthly.head(3)
    last_months = monthly.tail(3)
    sessions_growth = (last_months["sessions"].mean() / first_months["sessions"].mean() - 1) * 100
    orders_growth = (last_months["orders"].mean() / first_months["orders"].mean() - 1) * 100
    conv_first = first_months["conversion_rate_pct"].mean()
    conv_last = last_months["conversion_rate_pct"].mean()
    aov_first = first_months["aov_usd"].mean()
    aov_last = last_months["aov_usd"].mean()

    best_month = monthly.loc[monthly["orders"].idxmax()]
    top_channel = channels.iloc[0]
    significant = channels[channels["sessions"] >= 100].sort_values("conversion_rate_pct", ascending=False)
    top_conv_channel = significant.iloc[0]
    worst_channel = significant.iloc[-1]

    # ---- charts --------------------------------------------------------
    img_sessions_orders = chart_sessions_orders(monthly)
    img_conversion = chart_conversion(monthly)
    img_aov = chart_aov(monthly)
    img_channels = chart_channels(channels)
    img_channel_conv = chart_channel_conversion(channels)

    # ---- channel mix by acquisition type -------------------------------
    by_type = (
        channels.groupby("channel_type")[["sessions", "orders", "revenue"]]
        .sum()
        .reindex(["Payant", "Naturel", "Direct"])
        .dropna(how="all")
    )
    by_type["share_revenue_pct"] = by_type["revenue"] / by_type["revenue"].sum() * 100
    by_type["conversion_rate_pct"] = by_type["orders"] / by_type["sessions"] * 100

    paid_share = by_type.loc["Payant", "share_revenue_pct"] if "Payant" in by_type.index else 0.0
    free_share = 100 - paid_share

    table_channels = build_table(
        channels.rename(columns={
            "channel": "Canal",
            "channel_type": "Type",
            "sessions": "Sessions",
            "orders": "Commandes",
            "conversion_rate_pct": "Conv. %",
            "revenue": "CA ($)",
            "revenue_per_session_usd": "CA / session ($)",
        }),
        columns=["Canal", "Type", "Sessions", "Commandes", "Conv. %", "CA ($)", "CA / session ($)"],
        max_rows=20,
    )

    table_types = build_table(
        by_type.reset_index().rename(columns={
            "channel_type": "Type",
            "sessions": "Sessions",
            "orders": "Commandes",
            "revenue": "CA ($)",
            "share_revenue_pct": "Part du CA (%)",
            "conversion_rate_pct": "Conv. %",
        }),
        columns=["Type", "Sessions", "Commandes", "Conv. %", "CA ($)", "Part du CA (%)"],
        max_rows=5,
    )

    trend_direction_sessions = "en hausse" if sessions_growth >= 0 else "en baisse"
    trend_direction_orders = "en hausse" if orders_growth >= 0 else "en baisse"
    conv_direction = "en progression" if conv_last >= conv_first else "en recul"
    aov_direction = "en hausse" if aov_last >= aov_first else "en baisse"

    html = f"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Rapport Maven Fuzzy Factory</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Fredoka:wght@500;600;700&family=Karla:ital,wght@0,400;0,500;0,700;1,400&family=IBM+Plex+Mono:wght@500;600&display=swap" rel="stylesheet">
<style>
  :root {{
    --bg: #F4F5FA;
    --surface: #FFFFFF;
    --surface-raised: #FFFFFF;
    --text: #1B1F3B;
    --muted: #5B6178;
    --border: #E4E6F1;
    --accent: #FF6B4A;
    --accent-ink: #B8391F;
    --accent-soft: #FFE7E0;
    --teal: #2FB8AC;
    --teal-soft: #DDF4F1;
    --teal-ink: #10756C;
    --amber: #FFC145;
    --amber-soft: #FFF2D9;
    --amber-ink: #8A5D00;
    --shadow: 0 1px 2px rgba(27, 31, 59, 0.05), 0 8px 24px -16px rgba(27, 31, 59, 0.18);
  }}
  @media (prefers-color-scheme: dark) {{
    :root:not([data-theme="light"]) {{
      --bg: #14162A;
      --surface: #1C1F38;
      --surface-raised: #22254A;
      --text: #EEF0FB;
      --muted: #9EA3C4;
      --border: #2F3357;
      --accent: #FF8266;
      --accent-ink: #FFB6A0;
      --accent-soft: #3A2620;
      --teal: #4ED0C3;
      --teal-soft: #17332F;
      --amber: #FFCE6B;
      --amber-soft: #362B14;
      --shadow: 0 1px 2px rgba(0,0,0,0.3), 0 12px 28px -18px rgba(0,0,0,0.6);
    }}
  }}
  :root[data-theme="dark"] {{
    --bg: #14162A;
    --surface: #1C1F38;
    --surface-raised: #22254A;
    --text: #EEF0FB;
    --muted: #9EA3C4;
    --border: #2F3357;
    --accent: #FF8266;
    --accent-ink: #FFB6A0;
    --accent-soft: #3A2620;
    --teal: #4ED0C3;
    --teal-soft: #17332F;
    --teal-ink: #7EE0D5;
    --amber: #FFCE6B;
    --amber-soft: #362B14;
    --amber-ink: #FFD98A;
    --shadow: 0 1px 2px rgba(0,0,0,0.3), 0 12px 28px -18px rgba(0,0,0,0.6);
  }}

  * {{ box-sizing: border-box; }}
  html {{ background: var(--bg); }}
  body {{
    margin: 0;
    font-family: "Karla", -apple-system, "Segoe UI", sans-serif;
    background: var(--bg);
    color: var(--text);
    -webkit-font-smoothing: antialiased;
  }}
  a {{ color: var(--accent-ink); }}

  header {{
    padding: 56px 24px 76px;
    background:
      radial-gradient(760px 320px at 12% -10%, var(--accent-soft), transparent 60%),
      radial-gradient(620px 280px at 88% 0%, var(--teal-soft), transparent 55%);
    border-bottom: 1px solid var(--border);
  }}
  .eyebrow {{
    display: inline-flex;
    align-items: center;
    gap: 8px;
    font-family: "IBM Plex Mono", monospace;
    font-size: 12px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--muted);
    margin: 0 0 14px;
  }}
  .eyebrow::before {{
    content: "";
    width: 7px; height: 7px; border-radius: 50%;
    background: var(--teal);
    box-shadow: 0 0 0 3px var(--teal-soft);
  }}
  header h1 {{
    font-family: "Fredoka", "Karla", sans-serif;
    font-weight: 600;
    font-size: clamp(28px, 4vw, 40px);
    line-height: 1.15;
    margin: 0 0 12px;
    text-wrap: balance;
    max-width: 20ch;
  }}
  header p.lede {{
    margin: 0;
    max-width: 62ch;
    color: var(--muted);
    font-size: 16px;
    line-height: 1.6;
  }}
  header p.lede strong {{ color: var(--text); }}

  main {{
    max-width: 980px;
    margin: -40px auto 72px;
    padding: 0 24px;
    display: flex;
    flex-direction: column;
    gap: 20px;
  }}

  .kpis {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 14px;
  }}
  .kpi {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 18px 20px;
    box-shadow: var(--shadow);
  }}
  .kpi .label {{
    font-family: "IBM Plex Mono", monospace;
    font-size: 11px;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }}
  .kpi .value {{
    font-family: "IBM Plex Mono", monospace;
    font-variant-numeric: tabular-nums;
    font-size: 26px;
    font-weight: 600;
    margin-top: 6px;
    color: var(--text);
  }}

  section.card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 18px;
    padding: 30px 32px;
    box-shadow: var(--shadow);
  }}
  section.card .kicker {{
    display: flex;
    align-items: baseline;
    gap: 10px;
    margin-bottom: 4px;
  }}
  section.card .kicker .num {{
    font-family: "IBM Plex Mono", monospace;
    font-size: 13px;
    font-weight: 600;
    color: var(--accent-ink);
    background: var(--accent-soft);
    border-radius: 999px;
    padding: 3px 10px;
  }}
  section.card h2 {{
    font-family: "Fredoka", "Karla", sans-serif;
    font-weight: 600;
    margin: 0 0 6px;
    font-size: 21px;
    text-wrap: balance;
  }}
  section.card .question {{
    color: var(--muted);
    font-style: italic;
    margin: 0 0 18px;
    font-size: 14.5px;
  }}
  section.card .chart {{
    background: var(--surface-raised);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 12px;
    margin-bottom: 18px;
  }}
  section.card img {{ max-width: 100%; height: auto; display: block; margin: 0 auto; border-radius: 4px; }}
  section.card p.answer {{ line-height: 1.65; font-size: 15px; margin: 0; }}
  section.card p.answer + p.answer {{ margin-top: 10px; }}
  .charts-row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
  @media (max-width: 760px) {{ .charts-row {{ grid-template-columns: 1fr; }} }}

  .tag {{
    display: inline-block;
    font-family: "IBM Plex Mono", monospace;
    font-size: 11px;
    font-weight: 600;
    padding: 2px 8px;
    border-radius: 999px;
    vertical-align: 1px;
  }}
  .tag-paid {{ background: var(--accent-soft); color: var(--accent-ink); }}
  .tag-organic {{ background: var(--teal-soft); color: var(--teal-ink); }}
  .tag-direct {{ background: var(--amber-soft); color: var(--amber-ink); }}
  code {{
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.88em;
    background: var(--surface-raised);
    border: 1px solid var(--border);
    border-radius: 5px;
    padding: 1px 5px;
  }}

  .table-wrap {{ overflow-x: auto; margin-top: 18px; border: 1px solid var(--border); border-radius: 12px; }}
  table.data-table {{ width: 100%; border-collapse: collapse; font-size: 13.5px; min-width: 560px; }}
  table.data-table th, table.data-table td {{
    padding: 10px 14px;
    border-bottom: 1px solid var(--border);
    text-align: right;
    font-variant-numeric: tabular-nums;
    font-family: "IBM Plex Mono", monospace;
  }}
  table.data-table td:first-child, table.data-table th:first-child {{
    text-align: left;
    font-family: "Karla", sans-serif;
  }}
  table.data-table tbody tr:last-child td {{ border-bottom: none; }}
  table.data-table tbody tr:hover {{ background: var(--surface-raised); }}
  table.data-table th {{
    color: var(--muted);
    font-weight: 600;
    font-size: 10.5px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    background: var(--surface-raised);
  }}

  footer {{
    text-align: center;
    color: var(--muted);
    font-size: 12.5px;
    font-family: "IBM Plex Mono", monospace;
    padding: 8px 24px 40px;
  }}
</style>
</head>
<body>
<header>
  <p class="eyebrow">Maven Fuzzy Factory &middot; rapport genere automatiquement</p>
  <h1>Sessions, conversion et revenu du e-commerce jouets</h1>
  <p class="lede">
    Pipeline medaillon (bronze &rarr; silver &rarr; gold) execute sur un cluster
    <strong>Spark</strong> reel, donnees brutes persistees dans <strong>MinIO</strong>.
    Ce rapport repond aux 4 questions metier sur <strong>{total_sessions:,.0f}</strong>
    sessions et <strong>{total_orders:,.0f}</strong> commandes.
  </p>
</header>
<main>

  <div class="kpis">
    <div class="kpi"><div class="label">Sessions totales</div><div class="value">{total_sessions:,.0f}</div></div>
    <div class="kpi"><div class="label">Commandes totales</div><div class="value">{total_orders:,.0f}</div></div>
    <div class="kpi"><div class="label">Chiffre d'affaires total</div><div class="value">${total_revenue:,.0f}</div></div>
    <div class="kpi"><div class="label">Conversion globale</div><div class="value">{overall_conv}%</div></div>
  </div>

  <section class="card">
    <div class="kicker"><span class="num">01</span></div>
    <h2>Tendance du trafic et du volume de commandes</h2>
    <p class="question">Quelle est la tendance du nombre de sessions sur le site et du volume de commandes ?</p>
    <div class="chart"><img src="data:image/png;base64,{img_sessions_orders}" alt="Sessions vs commandes par mois"></div>
    <p class="answer">
      Entre les 3 premiers et les 3 derniers mois observes, les sessions sont
      <strong>{trend_direction_sessions}</strong> ({sessions_growth:+.0f}%) et les commandes sont
      <strong>{trend_direction_orders}</strong> ({orders_growth:+.0f}%).
      Le mois le plus actif en volume de commandes est <strong>{best_month['year_month']}</strong>
      avec {int(best_month['orders']):,} commandes pour {int(best_month['sessions']):,} sessions.
    </p>
    <p class="answer">
      La croissance des commandes suit globalement celle des sessions : le trafic reste le
      principal moteur du volume de ventes, plutot qu'une amelioration ponctuelle du taux de conversion.
    </p>
  </section>

  <section class="card">
    <div class="kicker"><span class="num">02</span></div>
    <h2>Taux de conversion session &rarr; commande</h2>
    <p class="question">Quel est le taux de conversion session-commande ? Comment a-t-il evolue ?</p>
    <div class="chart"><img src="data:image/png;base64,{img_conversion}" alt="Taux de conversion par mois"></div>
    <p class="answer">
      Le taux de conversion global sur toute la periode est de <strong>{overall_conv}%</strong>.
      Il est passe d'une moyenne de <strong>{conv_first:.2f}%</strong> sur les 3 premiers mois a
      <strong>{conv_last:.2f}%</strong> sur les 3 derniers mois : {conv_direction}
      de {abs(conv_last - conv_first):.2f} points.
    </p>
    <p class="answer">Cette evolution reflete les optimisations successives du site et du tunnel d'achat au fil du temps.</p>
  </section>

  <section class="card">
    <div class="kicker"><span class="num">03</span></div>
    <h2>Performance des canaux marketing</h2>
    <p class="question">Quels canaux marketing ont ete les plus performants ?</p>

    <p class="answer">
      Chaque session est rattachee a un seul type d'acquisition :
      <span class="tag tag-paid">Payant</span> une campagne balisee (parametres <code>utm</code>),
      <span class="tag tag-organic">Naturel</span> une arrivee depuis un moteur de recherche sans campagne,
      <span class="tag tag-direct">Direct</span> une saisie directe de l'adresse, sans referent.
    </p>

    <div class="table-wrap">
      {table_types}
    </div>

    <div class="charts-row">
      <div class="chart"><img src="data:image/png;base64,{img_channels}" alt="Chiffre d'affaires par canal d'acquisition"></div>
      <div class="chart"><img src="data:image/png;base64,{img_channel_conv}" alt="Taux de conversion par canal"></div>
    </div>

    <p class="answer">
      <strong>En volume</strong>, le canal dominant est <strong>{top_channel['channel']}</strong> :
      ${top_channel['revenue']:,.0f} de chiffre d'affaires ({int(top_channel['orders']):,} commandes,
      {top_channel['conversion_rate_pct']}% de conversion). Le trafic paye pese
      <strong>{paid_share:.0f}%</strong> du chiffre d'affaires, contre <strong>{free_share:.0f}%</strong>
      pour le trafic gratuit (naturel + direct).
    </p>
    <p class="answer">
      <strong>En efficacite</strong>, le classement change : le meilleur taux de conversion revient a
      <strong>{top_conv_channel['channel']}</strong> ({top_conv_channel['conversion_rate_pct']}%),
      soit {top_conv_channel['conversion_rate_pct'] / top_channel['conversion_rate_pct']:.1f}x celui du canal
      le plus gros. Le canal qui rapporte le plus n'est donc pas celui qui convertit le mieux : les volumes
      viennent de campagnes d'acquisition sur mots-cles generiques, peu qualifiees, alors que les visiteurs
      qui connaissent deja la boutique (naturel, direct, campagnes de marque) achetent nettement plus souvent.
    </p>
    <p class="answer">
      Point d'attention : <strong>{worst_channel['channel']}</strong> ne convertit qu'a
      <strong>{worst_channel['conversion_rate_pct']}%</strong> pour
      ${worst_channel['revenue_per_session_usd']:.2f} de chiffre d'affaires par session, loin derriere
      tous les autres canaux. Ce budget rapporte peu et gagnerait a etre reoriente.
    </p>

    <div class="table-wrap">
      {table_channels}
    </div>
  </section>

  <section class="card">
    <div class="kicker"><span class="num">04</span></div>
    <h2>Evolution du panier moyen (AOV)</h2>
    <p class="question">Comment le chiffre d'affaires par commande a-t-il evolue ?</p>
    <div class="chart"><img src="data:image/png;base64,{img_aov}" alt="AOV dans le temps"></div>
    <p class="answer">
      Le panier moyen est passe de <strong>${aov_first:,.2f}</strong> en moyenne sur les 3 premiers mois a
      <strong>${aov_last:,.2f}</strong> sur les 3 derniers mois : {aov_direction} de ${abs(aov_last - aov_first):,.2f}.
    </p>
    <p class="answer">
      {"Cette hausse peut s'expliquer par l'ajout de nouveaux produits au catalogue et/ou par du cross-sell." if aov_last >= aov_first else "Cette baisse peut s'expliquer par une part croissante de produits d'entree de gamme dans le mix de vente."}
    </p>
  </section>

</main>
<footer>
  genere automatiquement par le pipeline Spark &middot; bronze &rarr; silver &rarr; gold &rarr; dashboard
</footer>
</body>
</html>
"""

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "index.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Dashboard written to {out_path}")


if __name__ == "__main__":
    main()
