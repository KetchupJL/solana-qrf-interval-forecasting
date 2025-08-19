# utils/theme.py
import matplotlib as mpl
import matplotlib.pyplot as plt

def apply():
    mpl.rcParams.update({
        "figure.figsize": (7, 3.5),
        "axes.grid": True,
        "axes.spines.right": False,
        "axes.spines.top": False,
        "font.size": 10,
        "legend.frameon": False,
        "savefig.dpi": 180
    })
