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


vals = [
    (6, 0, 0, 12),
    (8, 0, 1, 6),
    (10, 1, 0, 3),
    (12, 1, 1, 1.5),
    (18, 2, 0, 0.2),
    (26, 2, 1, 0.01),
]

fig, axs = plt.subplots(3, 2, sharex=True)
fig.set_size_inches(5, 3.5)

for v, row_idx, col_idx, ymax in vals:

    ax = axs[row_idx][col_idx]

    data = read_data("results/error/token-estimation-error-" + str(v).zfill(2) + ".csv")

    values = data[1]
    headers = data[0]

    ax.set_xscale("log", base=10)

    ax.set_xlim([1, values["distinct count"][-1]])
    ax.set_xticks([pow(10.0, i) for i in range(0, 6)])
    if row_idx == 2:
        ax.set_xlabel(r"distinct count $\symCardinality$")
    if col_idx == 0:
        ax.set_ylabel(r"relative error (\%)")

    ax.plot(
        values["distinct count"],
        to_percent(values["relative rmse"]),
        label="rmse",
        color="C1",
        linestyle="solid",
    )

    ax.plot(
        values["distinct count"],
        to_percent(values["relative bias"]),
        label="bias",
        color="C0",
        linestyle="solid",
    )

    ax.set_ylim([ax.get_ylim()[0], ymax])

    ax.text(
        0.03,
        0.945,
        r"$\symHashTokenParameter="
        + str(v)
        + r"$ (token size $="
        + str(6 + v)
        + r"\,\text{bits})$",
        transform=ax.transAxes,
        verticalalignment="top",
        horizontalalignment="left",
        bbox=dict(facecolor="wheat", boxstyle="square,pad=0.2"),
    )

# axs[2][1].set_yticks([i * 0.004 for i in range(0, 3)])

handles, labels = ax.get_legend_handles_labels()
legend_order = [1, 0]
fig.legend(
    [handles[i] for i in legend_order],
    [labels[i] for i in legend_order],
    ncol=1,
    columnspacing=1,
    labelspacing=0.2,
    # bbox_to_anchor=(0.52, 0.09),
    borderpad=0.2,
    handletextpad=0.4,
    fancybox=False,
    framealpha=1,
    bbox_to_anchor=[0.9, 0.19],
    loc="center",
)
fig.subplots_adjust(
    top=0.985, bottom=0.12, left=0.095, right=0.98, wspace=0.23, hspace=0.11
)

fig.savefig(
    "paper/token_estimation_error.pdf",
    format="pdf",
    dpi=1200,
    metadata={"CreationDate": None, "ModDate": None},
)
plt.close(fig)
