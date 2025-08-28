from pathlib import Path
import matplotlib as mpl
import matplotlib.pyplot as plt

def use_paper_style():
    mpl.rcParams.update({
        "font.family": "DejaVu Sans",
        "font.size": 10, "axes.titlesize": 11, "axes.labelsize": 10,
        "legend.fontsize": 9, "xtick.labelsize": 9, "ytick.labelsize": 9,
        "lines.linewidth": 2.0, "lines.markersize": 4,
        "axes.grid": True, "grid.color": "#c7c7c7", "grid.linestyle": "-", "grid.linewidth": 0.6, "grid.alpha": 0.6,
        "axes.spines.top": False, "axes.spines.right": False, "axes.edgecolor": "#444444",
        "xtick.major.size": 3, "ytick.major.size": 3, "xtick.major.width": 0.8, "ytick.major.width": 0.8,
        "savefig.dpi": 300, "savefig.bbox": "tight",
        "pdf.fonttype": 42, "ps.fonttype": 42,
    })

PALETTE = {
    "blue":"#0072B2","orange":"#E69F00","green":"#009E73","sky":"#56B4E9",
    "verm":"#D55E00","purple":"#CC79A7","yellow":"#F0E442","black":"#000000",
}

MODEL_COLOR = {"QRF": PALETTE["blue"], "LightGBM": PALETTE["orange"], "LQR": PALETTE["green"]}

def quantile_linestyle(tau: float):
    if abs(tau-0.10)<1e-6: return "--"
    if abs(tau-0.50)<1e-6: return "-"
    if abs(tau-0.90)<1e-6: return ":"
    return "-."

def tidy_axes(ax, xlabel=None, ylabel=None, title=None):
    if xlabel: ax.set_xlabel(xlabel)
    if ylabel: ax.set_ylabel(ylabel)
    if title:  ax.set_title(title, pad=8)
    ax.tick_params(axis="both", which="both", direction="out")
    return ax

DEF_OUTDIR = Path("/Users/james/OneDrive/Documents/GitHub/solana-qrf-interval-forecasting/paper/figures/final")

def savefig_pdf(fig, filename_slug: str, outdir: Path = DEF_OUTDIR):
    outdir.mkdir(parents=True, exist_ok=True)
    fig.savefig(outdir / f"{filename_slug}.pdf", format="pdf", bbox_inches="tight")