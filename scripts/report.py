"""Draw the README charts from the measured results in docs/*.json."""

from __future__ import annotations

import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from deltalake import DeltaTable  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS = os.path.join(ROOT, "docs")
SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
RULE_COLOR = {"card_testing": BLUE, "impossible_travel": ORANGE, "account_takeover": AQUA}
RULE_NAME = {"card_testing": "Card testing", "impossible_travel": "Impossible travel", "account_takeover": "Account takeover"}


def style(ax, title):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", fontsize=13, color=INK, pad=12, fontweight="bold")
    ax.tick_params(colors=INK2, labelsize=9, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color(GRID)


def timeline() -> None:
    """Hero chart: transactions per minute with every fraud alert placed on it."""
    lake = os.path.join(ROOT, "data", "live", "lake")
    vol = DeltaTable(os.path.join(lake, "metrics_minute")).to_pandas().groupby("minute")["txns"].sum().sort_index()
    fig, ax = plt.subplots(figsize=(10, 4.2), dpi=150, facecolor=SURFACE)
    x0 = vol.index.min()
    mins = (vol.index - x0).total_seconds() / 60
    ax.fill_between(mins, vol.values, color="#d9d8d3", linewidth=0)
    ax.plot(mins, vol.values, color="#8a8984", linewidth=1.2, label="Transactions per minute")
    top = vol.max() * 1.18
    xmax = mins.max()
    for i, rule in enumerate(RULE_COLOR):
        a = DeltaTable(os.path.join(lake, f"alerts_{rule}")).to_pandas()
        x = (pd.to_datetime(a["window_end"]) - x0).dt.total_seconds() / 60
        xmax = max(xmax, x.max())
        ax.scatter(x, [top * (0.86 + 0.05 * i)] * len(x), s=34, color=RULE_COLOR[rule], zorder=3,
                   edgecolors=SURFACE, linewidths=1.2, label=f"{RULE_NAME[rule]} alert ({len(a)})")
    style(ax, "30 minutes of card traffic, every fraud alert raised in real time")
    ax.set_xlabel("Minutes into the stream (event time)", color=INK2, fontsize=10)
    ax.set_ylabel("Transactions per minute", color=INK2, fontsize=10)
    ax.set_ylim(0, top * 1.08)
    ax.set_xlim(0, xmax + 0.5)  # the last minutes' volume is still open (watermark) when the run ends
    ax.legend(frameon=False, fontsize=9, labelcolor=INK, loc="lower left", ncol=2)
    fig.tight_layout()
    fig.savefig(os.path.join(DOCS, "img", "stream_timeline.png"), facecolor=SURFACE)
    plt.close(fig)


def latency() -> None:
    lake = os.path.join(ROOT, "data", "live", "lake")
    frames = [DeltaTable(os.path.join(lake, f"alerts_{r}")).to_pandas() for r in RULE_COLOR]
    a = pd.concat(frames)
    s = (pd.to_datetime(a["detected_at"]) - pd.to_datetime(a["trigger_sent_at"])).dt.total_seconds()
    fig, ax = plt.subplots(figsize=(8, 3.6), dpi=150, facecolor=SURFACE)
    ax.hist(s, bins=range(0, int(s.max()) + 2), color=BLUE, edgecolor=SURFACE, linewidth=2)
    style(ax, "Seconds from fraudulent transaction to alert (live run)")
    ax.axvline(s.median(), color=INK, linewidth=1, linestyle="--")
    ax.text(s.median() + 0.2, ax.get_ylim()[1] * 0.9, f"median {s.median():.1f} s", color=INK, fontsize=9)
    ax.set_xlabel("Seconds", color=INK2, fontsize=10)
    ax.set_ylabel("Alerts", color=INK2, fontsize=10)
    fig.tight_layout()
    fig.savefig(os.path.join(DOCS, "img", "latency.png"), facecolor=SURFACE)
    plt.close(fig)


def main() -> None:
    os.makedirs(os.path.join(DOCS, "img"), exist_ok=True)
    timeline()
    latency()
    print("wrote docs/img/stream_timeline.png and docs/img/latency.png")


if __name__ == "__main__":
    main()
