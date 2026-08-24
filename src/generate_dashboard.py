"""
Dashboard generation.

Reads the gold CSV tables (small, aggregated) with pandas, builds the
charts with matplotlib and renders a single self-contained HTML file
(charts embedded as base64 PNGs, no external dependency / no internet
needed to view it) answering the four business questions.
"""
import base64
import io
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

GOLD_DIR = os.environ.get("GOLD_DIR", "/opt/spark-data/processed/gold")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/opt/output")

COLORS = {
    "sessions": "#4C6EF5",
    "orders": "#F76707",
    "revenue": "#2F9E44",
    "conv": "#E64980",
    "aov": "#7048E8",
    "bar": "#4C6EF5",
}

plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "axes.edgecolor": "#D0D5DD",
    "axes.grid": True,
    "grid.color": "#EAECF0",
    "grid.linewidth": 0.8,
    "font.size": 11,
    "axes.spines.top": False,
    "axes.spines.right": False,
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


def chart_channels(channels):
    top = channels.sort_values("revenue", ascending=False).head(10).iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.barh(top["channel"], top["revenue"], color=COLORS["bar"])
    ax.set_xlabel("Chiffre d'affaires ($)")
    ax.set_title("Top 10 canaux marketing par chiffre d'affaires")
    fig.tight_layout()
    return fig_to_base64(fig)


def chart_channel_conversion(channels):
    top = channels[channels["sessions"] >= 100].sort_values("conversion_rate_pct", ascending=False).head(10).iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.barh(top["channel"], top["conversion_rate_pct"], color=COLORS["conv"])
    ax.set_xlabel("Taux de conversion (%)")
    ax.set_title("Top 10 canaux par taux de conversion (>= 100 sessions)")
    fig.tight_layout()
    return fig_to_base64(fig)


def build_table(df, columns=None, max_rows=15):
    if columns:
        df = df[columns]
    df = df.head(max_rows)
    return df.to_html(index=False, classes="data-table", border=0, float_format=lambda x: f"{x:,.2f}")


def main():
    monthly = pd.read_csv(os.path.join(GOLD_DIR, "gold_monthly_trend.csv"))
    channels = pd.read_csv(os.path.join(GOLD_DIR, "gold_channel_performance.csv"))

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
    top_conv_channel = channels[channels["sessions"] >= 100].sort_values("conversion_rate_pct", ascending=False).iloc[0]

    # ---- charts --------------------------------------------------------
    img_sessions_orders = chart_sessions_orders(monthly)
    img_conversion = chart_conversion(monthly)
    img_aov = chart_aov(monthly)
    img_channels = chart_channels(channels)
    img_channel_conv = chart_channel_conversion(channels)

    table_channels = build_table(
        channels,
        columns=["channel", "sessions", "orders", "conversion_rate_pct", "revenue", "revenue_per_session_usd"],
        max_rows=15,
    )

    html = f"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<title>Toy Store E-Commerce - Dashboard</title>
<style>
  :root {{
    --bg: #F8F9FB;
    --card: #FFFFFF;
    --text: #1D2939;
    --muted: #667085;
    --border: #E4E7EC;
    --accent: #4C6EF5;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0;
    font-family: -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    background: var(--bg);
    color: var(--text);
  }}
  header {{
    background: linear-gradient(135deg, #364FC7, #4C6EF5);
    color: white;
    padding: 40px 24px;
  }}
  header h1 {{ margin: 0 0 6px 0; font-size: 28px; }}
  header p {{ margin: 0; opacity: 0.9; }}
  main {{
    max-width: 1100px;
    margin: -28px auto 60px auto;
    padding: 0 24px;
  }}
  .kpis {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
    margin-bottom: 28px;
  }}
  .kpi {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 18px 20px;
    box-shadow: 0 1px 2px rgba(16,24,40,0.04);
  }}
  .kpi .label {{ font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; }}
  .kpi .value {{ font-size: 24px; font-weight: 700; margin-top: 4px; }}
  section.card {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 24px 28px;
    margin-bottom: 24px;
    box-shadow: 0 1px 2px rgba(16,24,40,0.04);
  }}
  section.card h2 {{ margin-top: 0; font-size: 18px; }}
  section.card .question {{ color: var(--muted); font-style: italic; margin-bottom: 14px; }}
  section.card img {{ max-width: 100%; height: auto; display: block; margin: 12px auto; }}
  .charts-row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
  @media (max-width: 800px) {{ .charts-row {{ grid-template-columns: 1fr; }} }}
  table.data-table {{ width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 10px; }}
  table.data-table th, table.data-table td {{ padding: 8px 10px; border-bottom: 1px solid var(--border); text-align: right; }}
  table.data-table th:first-child, table.data-table td:first-child {{ text-align: left; }}
  table.data-table th {{ color: var(--muted); font-weight: 600; font-size: 11px; text-transform: uppercase; }}
  footer {{ text-align: center; color: var(--muted); font-size: 12px; padding-bottom: 30px; }}
</style>
</head>
<body>
<header>
  <h1>Toy Store E-Commerce - Dashboard</h1>
  <p>Pipeline medaillon (bronze / silver / gold) - ingestion, MinIO, Spark cluster, restitution</p>
</header>
<main>

  <div class="kpis">
    <div class="kpi"><div class="label">Sessions totales</div><div class="value">{total_sessions:,.0f}</div></div>
    <div class="kpi"><div class="label">Commandes totales</div><div class="value">{total_orders:,.0f}</div></div>
    <div class="kpi"><div class="label">Chiffre d'affaires total</div><div class="value">${total_revenue:,.0f}</div></div>
    <div class="kpi"><div class="label">Taux de conversion global</div><div class="value">{overall_conv}%</div></div>
  </div>

  <section class="card">
    <h2>1. Tendance du nombre de sessions et du volume de commandes</h2>
    <p class="question">Quelle est la tendance du nombre de sessions sur le site et du volume de commandes ?</p>
    <img src="data:image/png;base64,{img_sessions_orders}" alt="Sessions vs commandes">
    <p>
      Entre les 3 premiers et les 3 derniers mois observés, les sessions ont
      {"progressé de " + f"{sessions_growth:,.0f}%" if sessions_growth >= 0 else "reculé de " + f"{abs(sessions_growth):,.0f}%"}
      et les commandes ont {"progressé de " + f"{orders_growth:,.0f}%" if orders_growth >= 0 else "reculé de " + f"{abs(orders_growth):,.0f}%"}.
      Le mois le plus actif en volume de commandes est <strong>{best_month['year_month']}</strong>
      avec {int(best_month['orders']):,} commandes pour {int(best_month['sessions']):,} sessions.
      La croissance des commandes suit globalement celle des sessions, ce qui indique que le
      trafic est le principal moteur du volume de ventes plutot qu'une amelioration ponctuelle
      du taux de conversion.
    </p>
  </section>

  <section class="card">
    <h2>2. Taux de conversion session -> commande</h2>
    <p class="question">Quel est le taux de conversion session-commande ? Comment a-t-il evolue ?</p>
    <img src="data:image/png;base64,{img_conversion}" alt="Taux de conversion">
    <p>
      Le taux de conversion global sur toute la periode est de <strong>{overall_conv}%</strong>.
      Il est passe d'une moyenne de {conv_first:.2f}% sur les 3 premiers mois a
      {conv_last:.2f}% sur les 3 derniers mois
      ({"amelioration" if conv_last >= conv_first else "baisse"} de {abs(conv_last - conv_first):.2f} points).
      Cette evolution reflete les optimisations successives du site et du tunnel d'achat au fil du temps.
    </p>
  </section>

  <section class="card">
    <h2>3. Performance des canaux marketing</h2>
    <p class="question">Quels canaux marketing ont ete les plus performants ?</p>
    <div class="charts-row">
      <img src="data:image/png;base64,{img_channels}" alt="CA par canal">
      <img src="data:image/png;base64,{img_channel_conv}" alt="Conversion par canal">
    </div>
    <p>
      Le canal generant le plus de chiffre d'affaires est <strong>{top_channel['channel']}</strong>
      (${top_channel['revenue']:,.0f}, {int(top_channel['orders']):,} commandes,
      taux de conversion de {top_channel['conversion_rate_pct']}%).
      En termes d'efficacite (taux de conversion, canaux avec au moins 100 sessions), le meilleur est
      <strong>{top_conv_channel['channel']}</strong> avec {top_conv_channel['conversion_rate_pct']}% de conversion.
      Le detail des {min(15, len(channels))} principaux canaux est disponible ci-dessous.
    </p>
    {table_channels}
  </section>

  <section class="card">
    <h2>4. Evolution du chiffre d'affaires par commande (AOV)</h2>
    <p class="question">Comment le chiffre d'affaires par commande a-t-il evolue ?</p>
    <img src="data:image/png;base64,{img_aov}" alt="AOV dans le temps">
    <p>
      Le panier moyen (AOV) est passe de ${aov_first:,.2f} en moyenne sur les 3 premiers mois
      a ${aov_last:,.2f} sur les 3 derniers mois
      ({"hausse" if aov_last >= aov_first else "baisse"} de ${abs(aov_last - aov_first):,.2f}).
      {"Cette hausse peut s'expliquer par l'ajout de nouveaux produits au catalogue et/ou par du cross-sell." if aov_last >= aov_first else "Cette baisse peut s'expliquer par une part croissante de produits d'entree de gamme dans le mix de vente."}
    </p>
  </section>

</main>
<footer>
  Genere automatiquement par le pipeline Spark (bronze -> silver -> gold -> dashboard).
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
