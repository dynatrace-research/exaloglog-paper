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
import matplotlib.pyplot as plt


def pdf_geometric(b, k):
    return (b - 1) * pow(b, -k)


def pdf_exaloglog(t, k):
    return pow(2.0, -t - 1 - (k - 1) // (2**t))


fig, ax = plt.subplots(1, 2)
fig.set_size_inches(6, 2)


def plot_x(ax, t):
    kVals = range(1, 21)
    label = r"geometric $\symBase="
    if t == 0:
        label += "2$"
    elif t == 1:
        label += r"\sqrt{2}$"
    else:
        label += r"\sqrt[" + str(pow(2, t)) + "]{2}$"

    ax.bar(
        [kVal - 0.2 for kVal in kVals],
        [pdf_geometric(b=pow(2, pow(2.0, -t)), k=kVal) for kVal in kVals],
        width=0.4,
        label=label,
        color="#ffa600",
    )
    ax.bar(
        [kVal + 0.2 for kVal in kVals],
        [pdf_exaloglog(t, kVal) for kVal in kVals],
        width=0.4,
        label=r"approximate $\symExtraBits=" + str(t) + "$",
        color="#377eb8",
    )
    ax.legend(
        loc="upper right",
        bbox_to_anchor=(1.01, 1.02),
        columnspacing=1,
        labelspacing=0.2,
        borderpad=0.2,
        handletextpad=0.4,
        fancybox=False,
        framealpha=1,
    )
    ax.set_xlabel(r"\symUpdateVal")
    ax.set_yscale("log", base=2)
    ax.set_yscale("log", base=2)


plot_x(ax[0], t=1)
plot_x(ax[1], t=2)

ax[0].set_ylabel("probability")
ax[0].set_yticks([2 ** (-i) for i in range(2, 14, 2)])
ax[1].set_yticks([2 ** (-i) for i in range(3, 9, 1)])

fig.subplots_adjust(left=0.087, bottom=0.20, right=0.992, top=0.985, wspace=0.15)
fig.savefig(
    "paper/probability_densities.pdf",
    format="pdf",
    dpi=1200,
    metadata={"CreationDate": None, "ModDate": None},
)
plt.close(fig)
