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
import csv
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import mvp
from math import sqrt


def read_data(data_file):
    info = {}

    with open(data_file, "r") as file:
        reader = csv.reader(file, skipinitialspace=True, delimiter=";")
        row_counter = 0
        headers = []
        values = []
        for r in reader:
            if row_counter == 0:
                for i in r:
                    if i != "":
                        g = i.split("=")
                        info[g[0]] = g[1]

            elif row_counter == 1:
                for i in r:
                    if i != "":
                        headers.append(i)
                        values.append([])
            elif row_counter >= 2:
                k = 0
                for i in r:
                    if i != "":
                        values[k].append(float(i))
                        k += 1
            row_counter += 1

    data = {h: v for h, v in zip(headers, values)}
    size = row_counter - 2
    return info, data, size


def to_percent(values):
    return [100.0 * v for v in values]


colors = ["C0", "C1", "C2"]

tdvals = [(1, 9), (2, 16), (2, 20), (2, 24)]
pvals = [4, 6, 8, 10]


# num_simulation_runs_unit = ""
# num_simulation_runs = int(headers["num_cycles"])
# if num_simulation_runs % 1000 == 0:
#     num_simulation_runs //= 1000
#     num_simulation_runs_unit = "k"
# if num_simulation_runs % 1000 == 0:
#     num_simulation_runs //= 1000
#     num_simulation_runs_unit = "M"

fig, axs = plt.subplots(4, 4, sharex=True)
fig.set_size_inches(11, 5.5)

for td_idx, td in enumerate(tdvals):

    t, d = td

    for pidx, p in enumerate(pvals):
        ax = axs[pidx][td_idx]

        data = read_data(
            "results/error/exaloglog-estimation-error"
            + "-t"
            + str(t).zfill(2)
            + "-d"
            + str(d).zfill(2)
            + "-p"
            + str(p).zfill(2)
            + ".csv"
        )

        values = data[1]
        headers = data[0]

        large_scale_simulation_mode_distinct_count_limit = int(
            headers["large_scale_simulation_mode_distinct_count_limit"]
        )

        ax.set_xscale("log", base=10)
        theory = to_percent(
            values["theoretical relative standard error maximum likelihood"]
        )[0]

        ax.set_ylim([-theory * 0.1, theory * 1.35])
        ax.set_xlim([1, values["distinct count"][-1]])
        ax.set_xticks([pow(10.0, 3 * i) for i in range(0, 8)])
        # ax.xaxis.grid(True)
        if pidx == len(pvals) - 1:
            ax.set_xlabel(r"distinct count $\symCardinality$")
        # ax.yaxis.grid(True)
        if td_idx == 0:
            ax.set_ylabel(r"relative error (\%)")

        # # draw transition
        # ax.plot(
        #     [
        #         large_scale_simulation_mode_distinct_count_limit,
        #         large_scale_simulation_mode_distinct_count_limit,
        #     ],
        #     [-theory * 2, theory * 2],
        #     color="red",
        #     linestyle="dashed",
        #     linewidth=0.8,
        # )

        b = pow(2, pow(2, -t))
        q = 6 + t
        rel_error_martingale_theory = sqrt(
            mvp.mvp_martingale(b=b, d=d, q=q).mvp / ((q + d) * pow(2, p))
        )
        rel_error_ml_theory = sqrt(
            mvp.mvp_ml(b=b, d=d, q=q).mvp / ((q + d) * pow(2, p))
        )

        ax.plot(
            values["distinct count"],
            to_percent([rel_error_martingale_theory] * len(values["distinct count"])),
            label="martingale theory",
            color=colors[2],
            linestyle="dotted",
        )
        ax.plot(
            values["distinct count"],
            to_percent([rel_error_ml_theory] * len(values["distinct count"])),
            label="ML theory",
            color=colors[2],
            linestyle="solid",
        )

        ax.plot(
            values["distinct count"],
            to_percent(values["relative rmse martingale"]),
            label="martingale rmse",
            color=colors[1],
            linestyle="dotted",
        )
        ax.plot(
            values["distinct count"],
            to_percent(values["relative rmse maximum likelihood"]),
            label="ML rmse",
            color=colors[1],
            linestyle="solid",
        )

        ax.plot(
            values["distinct count"],
            to_percent(values["relative bias martingale"]),
            label="martingale bias",
            color=colors[0],
            linestyle="dotted",
        )
        ax.plot(
            values["distinct count"],
            to_percent(values["relative bias maximum likelihood"]),
            label="ML bias",
            color=colors[0],
            linestyle="solid",
        )

        ax.text(
            0.03,
            0.945,
            r"$\symExtraBits="
            + str(t)
            + r", \symNumExtraBits="
            + str(d)
            + r", \symPrecision="
            + str(p)
            + r"$ ("
            + str((2**p * (t + 6 + d) + 7) // 8)
            + r" bytes)",
            transform=ax.transAxes,
            verticalalignment="top",
            horizontalalignment="left",
            bbox=dict(facecolor="wheat", boxstyle="square,pad=0.2"),
        )

handles, labels = ax.get_legend_handles_labels()
legend_order = [1, 3, 5, 0, 2, 4]
fig.legend(
    [handles[i] for i in legend_order],
    [labels[i] for i in legend_order],
    loc="lower center",
    ncol=6,
    columnspacing=1,
    labelspacing=0.2,
    bbox_to_anchor=(0.5, 0.0),
    borderpad=0.2,
    handletextpad=0.4,
    fancybox=False,
    framealpha=1,
)
fig.subplots_adjust(
    top=0.99, bottom=0.13, left=0.045, right=0.989, wspace=0.13, hspace=0.07
)

fig.savefig(
    "paper/estimation_error.pdf",
    format="pdf",
    dpi=1200,
    metadata={"CreationDate": None, "ModDate": None},
)
plt.close(fig)
