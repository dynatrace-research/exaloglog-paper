#
# Copyright (c) 2024-2025 Dynatrace LLC. All rights reserved.
#
# This software and associated documentation files (the "Software")
# are being made available by Dynatrace LLC for the sole purpose of
# illustrating the implementation of certain algorithms which have
# been published by Dynatrace LLC. Permission is hereby granted,
# free of charge, to any person obtaining a copy of the Software,
# to view and use the Software for internal, non-production,
# non-commercial purposes only – the Software may not be used to
# process live data or distributed, sublicensed, modified and/or
# sold either alone or as part of or in combination with any other
# software.
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
#
import preamble
from math import sqrt
import matplotlib.pyplot as plt
import numpy
from matplotlib.ticker import MultipleLocator
import mvp
import matplotlib.ticker as mtick
import matplotlib.patheffects as PathEffects
from labellines import labelLine, labelLines

d_max = 60
help_line_color = "black"
help_linestyle = "dotted"
help_linewidth = 1


def plot_improvement(ax, ull, hll, d_pos, y_pos):
    ax.plot(
        [hll.d, d_max],
        [hll.mvp, hll.mvp],
        color=help_line_color,
        linestyle=help_linestyle,
        linewidth=help_linewidth,
    )
    ax.plot(
        [ull.d, d_max],
        [ull.mvp, ull.mvp],
        color=help_line_color,
        linestyle=help_linestyle,
        linewidth=help_linewidth,
    )
    improvement = round(100 * (ull.mvp / hll.mvp - 1))
    txt = ax.text(
        d_pos - 5.5,
        ull.mvp + y_pos * (hll.mvp - ull.mvp),
        "$" + str(improvement) + r"\%$",
        zorder=1.6,
    )
    txt.set_path_effects([PathEffects.withStroke(linewidth=5, foreground="w")])
    ax.annotate(
        "",
        xy=(d_pos, ull.mvp),
        xytext=(d_pos, hll.mvp),
        arrowprops=dict(arrowstyle="->", linewidth=help_linewidth, shrinkB=0.5),
        zorder=4,
    )


colors = ["C" + str(i) for i in range(0, 65)]
linestyles = ["solid"] * 66
bbox = None
outline_width = 0

marker = "."
marker_size = 4


def make_chart(
    mvp_func,
    y_limits,
    file_name,
    mark_minima,
    labels,
    mark_improvement=False,
    label_pos=[],
):
    delta_y = y_limits[1] - y_limits[0]

    t_values = range(4)
    d_values = range(d_max + 1)

    fig, ax = plt.subplots(1, 1, sharex=True, sharey=True)
    fig.set_size_inches(5, 2.5)

    if mark_improvement:
        hll_mvp = mvp_func(q=6, b=2, d=0)
        opt_mvp = mvp_func(q=8, b=pow(2, 0.25), d=None)
        plot_improvement(ax, opt_mvp, hll_mvp, 57.5, 0.37)

    plotted_lines = []
    for t in reversed(t_values):
        b = pow(2, 1.0 / pow(2.0, t))
        q = 6 + t
        plotted_lines += ax.plot(
            d_values,
            [mvp_func(q=q, d=d, b=b).mvp for d in d_values],
            label=r"$\symExtraBits=" + str(t) + "$",
            color=colors[t],
            linestyle=linestyles[t],
            marker=marker,
            markersize=marker_size,
        )
        if mark_minima:
            opt_mvp = mvp_func(q=q, b=b, d=None)
            ax.annotate(
                "",
                xy=(opt_mvp.d, opt_mvp.mvp),
                xytext=(
                    opt_mvp.d,
                    opt_mvp.mvp - delta_y * 0.085,
                ),
                arrowprops=dict(arrowstyle="->", color=colors[t], linewidth=1.5),
            )

    ax.set_ylim(y_limits)
    ax.set_xlim([-5, 60])
    ax.set_xlabel(r"$\symNumExtraBits$")
    ax.set_ylabel(r"memory-variance product (MVP)")
    # ax.set_xticks(numpy.arange(0, 65, 5.0))
    ax.grid(visible=True)

    hll_t = 0
    hll_d = 0
    hll_mvp = mvp_func(q=6 + hll_t, d=hll_d, b=pow(2, 1.0 / pow(2.0, hll_t)))
    hll_mvp_const = hll_mvp.mvp

    secax = ax.secondary_yaxis(
        "right",
        functions=(
            lambda x: (x - hll_mvp_const) / hll_mvp_const * 100,
            lambda x: (100 + x) * hll_mvp_const / 100,
        ),
    )
    secax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))

    for l in labels:
        t = l[1]
        d = l[2]
        mvp = mvp_func(q=(6 + t), d=d, b=pow(2, 1.0 / pow(2.0, t)))
        ax.annotate(
            l[0],
            horizontalalignment="center",
            verticalalignment="center",
            xytext=(
                mvp.d + l[3],
                mvp.mvp + l[4] * delta_y,
            ),
            xy=(mvp.d, mvp.mvp),
            arrowprops=dict(arrowstyle="-", shrinkA=0, shrinkB=3),
        )

    for pos, line in zip(label_pos, reversed(plotted_lines)):
        labelLine(line, pos, bbox=None, outline_width=6)

    fig.subplots_adjust(top=0.98, bottom=0.16, left=0.105, right=0.91, hspace=0.08)

    fig.savefig(
        file_name,
        format="pdf",
        dpi=1200,
        metadata={"CreationDate": None, "ModDate": None},
    )
    plt.close(fig)


make_chart(
    lambda q, d, b: mvp.mvp_ml(q=q, d=d, b=b),
    [3.4, 6.6],
    "paper/mvp_ml.pdf",
    True,
    labels=[
        ("HLL", 0, 0, -2.88, -0.1),
        ("EHLL", 0, 1, -3.2, -0.1),
        ("ULL", 0, 2, -4.8, -0.02),
        ("ELL(1,9)", 1, 9, 3.5, -0.1),
        ("ELL(2,16)", 2, 16, 2.3, 0.1),
        ("ELL(2,20)", 2, 20, 5, 0.21),
        ("ELL(2,24)", 2, 24, 8, 0.03),
    ],
    mark_improvement=True,
    label_pos=[8.7, 19, 49, 24],
)
make_chart(
    lambda q, d, b: mvp.mvp_ml_compressed(d=d, b=b),
    [1.9, 3.1],
    "paper/mvp_compressed_ml.pdf",
    False,
    labels=[
        ("HLL", 0, 0, -2.9, -0.1),
        ("EHLL", 0, 1, -3.2, -0.09),
        ("ULL", 0, 2, -4, -0.05),
        ("ELL(1,9)", 1, 9, 6, 0.03),
        ("ELL(2,16)", 2, 16, 7, 0.03),
        ("ELL(2,20)", 2, 20, 5.7, 0.08),
        ("ELL(2,24)", 2, 24, 7, 0.03),
    ],
    label_pos=[0.6, 3, 10.5, 29],
)
make_chart(
    lambda q, d, b: mvp.mvp_martingale(q=q, d=d, b=b),
    [2.6, 4.3],
    "paper/mvp_martingale.pdf",
    True,
    labels=[
        ("HLL", 0, 0, -2.88, -0.13),
        ("EHLL", 0, 1, -3.2, 0.09),
        ("ULL", 0, 2, -4.6, -0.09),
        ("ELL(1,9)", 1, 9, 6.3, 0.03),
        ("ELL(2,16)", 2, 16, 0, 0.1),
        ("ELL(2,20)", 2, 20, 4.5, -0.1),
        ("ELL(2,24)", 2, 24, 5, -0.08),
    ],
    mark_improvement=True,
    label_pos=[5.7, 15, 39, 22],
)
make_chart(
    lambda q, d, b: mvp.mvp_martingale_compressed(d=d, b=b),
    [1.6, 2.0],
    "paper/mvp_compressed_martingale.pdf",
    False,
    labels=[
        ("HLL", 0, 0, -2.9, -0.09),
        ("EHLL", 0, 1, -3.2, -0.09),
        ("ULL", 0, 2, -4.6, -0.05),
        ("ELL(1,9)", 1, 9, 6, 0.02),
        ("ELL(2,16)", 2, 16, 7, 0.04),
        ("ELL(2,20)", 2, 20, 5.6, 0.08),
        ("ELL(2,24)", 2, 24, 7, 0.05),
    ],
    label_pos=[0.6, 2.8, 10, 28],
)
