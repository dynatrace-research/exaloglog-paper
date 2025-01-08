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
from preamble import algorithm_styles
import json
import matplotlib.pyplot as plt
import numpy
import csv


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
                        values[k].append(i)
                        k += 1
            row_counter += 1

    data = {h: v for h, v in zip(headers, values)}
    size = row_counter - 2
    return info, data, size


def load_data(algorithm_result_files):

    result = {}

    for algorithm in algorithm_result_files:
        info, data, size = read_data(algorithm_result_files[algorithm])
        result[algorithm] = (info, data, size)

    return result


algorithm_blacklist = {
    "APACHE_DATA_SKETCHES_HLL8_11_MARTINGALE",
    "APACHE_DATA_SKETCHES_HLL4_11_MARTINGALE",
    "APACHE_DATA_SKETCHES_HLL6_11_MARTINGALE",
    "APACHE_DATA_SKETCHES_HLL8_11_MARTINGALE",
}

algorithm_result_files = {
    "ULTRALOGLOG_10": "results/comparison-empirical-mvp/Hash4j UltraLogLog (p = 10).csv",
    "HYPERLOGLOG_11": "results/comparison-empirical-mvp/Hash4j HyperLogLog (p = 11).csv",
    "EXALOGLOG_2_20_8": "results/comparison-empirical-mvp/ExaLogLog (t = 2, d = 20, p = 8).csv",
    "EXALOGLOG_2_24_8": "results/comparison-empirical-mvp/ExaLogLog (t = 2, d = 24, p = 8).csv",
    "APACHE_DATA_SKETCHES_CPC_10": "results/comparison-empirical-mvp/Apache Data Sketches Java CPC (p = 10).csv",
    "APACHE_DATA_SKETCHES_HLL4_11": "results/comparison-empirical-mvp/Apache Data Sketches Java HLL4 (p = 11).csv",
    "APACHE_DATA_SKETCHES_HLL6_11": "results/comparison-empirical-mvp/Apache Data Sketches Java HLL6 (p = 11).csv",
    "APACHE_DATA_SKETCHES_HLL8_11": "results/comparison-empirical-mvp/Apache Data Sketches Java HLL8 (p = 11).csv",
    "HyperLogLogLog (p=11)": "results/comparison-empirical-mvp/HyperLogLogLog (p = 11).csv",
    "SpikeSketch (128 buckets)": "results/comparison-empirical-mvp/SpikeSketch (numOfBuckets = 128).csv",
}


def plot_chart(data):

    fig, ax = plt.subplots(1, 2, sharex=True)
    fig.set_size_inches(5, 3)

    for algorithm in data:

        if algorithm in algorithm_blacklist:
            continue

        if algorithm not in algorithm_styles:
            print(algorithm)
            continue

        ax[0].plot(
            [float(d) for d in data[algorithm][1]["true distinct count"]],
            [float(d) / 1024 for d in data[algorithm][1]["average memory size"]],
            label=algorithm_styles[algorithm][0],
            color=algorithm_styles[algorithm][1],
            linestyle=algorithm_styles[algorithm][2],
            marker=algorithm_styles[algorithm][3],
            zorder=algorithm_styles[algorithm][4],
        )

        ax[1].plot(
            [float(d) for d in data[algorithm][1]["true distinct count"]],
            [float(d) for d in data[algorithm][1]["estimated memory MVP"]],
            label=algorithm_styles[algorithm][0],
            color=algorithm_styles[algorithm][1],
            linestyle=algorithm_styles[algorithm][2],
            marker=algorithm_styles[algorithm][3],
            zorder=algorithm_styles[algorithm][4],
        )

    ax[0].set_xlim([10, 1e6])
    ax[1].set_xlim([10, 1e6])
    ax[0].set_ylim([0, 2.3])
    ax[1].set_ylim([0, 11])
    ax[0].set_xscale("log")
    ax[1].set_xscale("log")
    ax[0].grid(visible=True)
    ax[1].grid(visible=True)
    ax[0].set_xlabel(r"distinct count $\symCardinality$")
    ax[1].set_xlabel(r"distinct count $\symCardinality$")
    ax[0].set_ylabel(r"memory (KiB)")
    ax[1].set_ylabel(r"empirical MVP")
    ax[0].xaxis.set_ticks([1e1, 1e2, 1e3, 1e4, 1e5, 1e6])
    ax[1].xaxis.set_ticks([1e1, 1e2, 1e3, 1e4, 1e5, 1e6])

    fig.subplots_adjust(top=0.99, bottom=0.14, left=0.09, right=0.98, wspace=0.25)

    fig.savefig(
        "paper/memory.pdf",
        format="pdf",
        dpi=1200,
        metadata={"CreationDate": None, "ModDate": None},
    )
    plt.close(fig)


data = load_data(algorithm_result_files)

plot_chart(data)
