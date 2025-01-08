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
import numpy


def mem_func(mvp, error_in_percent):
    return mvp / (error_in_percent / 100) ** 2 / 8


def plot_error_over_memory():

    fig, ax = plt.subplots(1, 1, sharex=True, sharey=True)
    fig.set_size_inches(5, 2)

    min_error = 1
    max_error = 5
    mvp_vals = [8, 6, 5, 4, 3, 2]
    colors = ["#003f5c", "#444e86", "#955196", "#dd5182", "#ff6e54", "#ffa600"]
    linestyles = ["dashdot", "dashed", "solid", "dashdot", "dashed", "solid"]
    error_values = numpy.linspace(min_error, max_error, 100)

    for mvp, color, linestyle in zip(mvp_vals, colors, linestyles):
        ax.plot(
            error_values,
            [mem_func(mvp, error) for error in error_values],
            label=r"$\symMVP=" + str(mvp) + "$",
            color=color,
            linestyle=linestyle,
        )

    ax.set_xlim([min_error, max_error])
    ax.set_yscale("log", base=2)
    ax.set_xlabel(r"relative error (\%)")
    ax.set_ylabel(r"memory (bytes)")
    ax.grid(visible=True)
    ax.set_yticks([2**i for i in range(7, 14)], labels=[2**i for i in range(7, 14)])
    ax.legend(
        loc="upper right",
        ncols=2,
        columnspacing=1,
        labelspacing=0.2,
        borderpad=0.2,
        handletextpad=0.4,
        fancybox=False,
        framealpha=1,
    )

    fig.subplots_adjust(top=0.98, bottom=0.21, left=0.11, right=0.98, hspace=0.08)

    fig.savefig(
        "paper/memory_over_error.pdf",
        format="pdf",
        dpi=1200,
        metadata={"CreationDate": None, "ModDate": None},
    )
    plt.close(fig)


plot_error_over_memory()
