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


def load_insertion_data():

    # load java benchmark data
    f = open("results/benchmarks/benchmark-results-java.json")
    data_java = json.load(f)

    result = {}

    for r in data_java:
        if r["benchmark"] != "com.dynatrace.exaloglogpaper.InsertionTest.insert":
            continue

        num_elements = int(r["params"]["numElements"])
        sketch_config = r["params"]["sketchConfig"]
        time_in_seconds = float(r["primaryMetric"]["score"]) / num_elements * 1e-6

        if not sketch_config in result:
            result[sketch_config] = []
        result[sketch_config].append((num_elements, time_in_seconds))

    # load c++ benchmark data

    _, data_cpp, size = read_data(
        "results/benchmarks/benchmark-results-insertion-cpp.csv"
    )

    for i in range(size):
        num_elements = int(data_cpp["distinct count"][i])
        sketch_config = data_cpp["data structure"][i]

        time_in_seconds = float(data_cpp["insertion time (ns)"][i]) * 1e-9
        if not sketch_config in result:
            result[sketch_config] = []
        result[sketch_config].append((num_elements, time_in_seconds))

    for x in result:

        result[x] = sorted(result[x])

    return result


def load_estimation_data():

    # load java benchmark data
    f = open("results/benchmarks/benchmark-results-java.json")
    data_java = json.load(f)

    result = {}

    for r in data_java:
        if r["benchmark"] != "com.dynatrace.exaloglogpaper.EstimationTest.estimate":
            continue

        num_sketches = int(r["params"]["numSketches"])
        num_elements = int(r["params"]["numElements"])
        sketch_config = r["params"]["sketchConfig"]
        time_in_seconds = float(r["primaryMetric"]["score"]) * 1e-6 / num_sketches

        if not sketch_config in result:
            result[sketch_config] = []
        result[sketch_config].append((num_elements, time_in_seconds))

    # load c++ benchmark data

    _, data_cpp, size = read_data(
        "results/benchmarks/benchmark-results-estimation-cpp.csv"
    )

    for i in range(size):
        num_elements = int(data_cpp["distinct count"][i])
        sketch_config = data_cpp["data structure"][i]

        time_in_seconds = float(data_cpp["estimation time (us)"][i]) * 1e-6
        if not sketch_config in result:
            result[sketch_config] = []
        result[sketch_config].append((num_elements, time_in_seconds))

    for x in result:

        result[x] = sorted(result[x])

    return result


def load_serialization_data():

    # load java benchmark data
    f = open("results/benchmarks/benchmark-results-java.json")
    data_java = json.load(f)

    result = {}

    for r in data_java:
        if r["benchmark"] != "com.dynatrace.exaloglogpaper.SerializationTest.serialize":
            continue

        num_sketches = int(r["params"]["numSketches"])
        num_elements = int(r["params"]["numElements"])
        sketch_config = r["params"]["sketchConfig"]
        time_in_seconds = float(r["primaryMetric"]["score"]) * 1e-6 / num_sketches

        if not sketch_config in result:
            result[sketch_config] = []
        result[sketch_config].append((num_elements, time_in_seconds))

    for x in result:

        result[x] = sorted(result[x])

    return result


def load_merge_and_estimation_data():

    # load java benchmark data
    f = open("results/benchmarks/benchmark-results-java.json")
    data_java = json.load(f)

    result = {}

    for r in data_java:
        if (
            r["benchmark"]
            != "com.dynatrace.exaloglogpaper.MergeAndEstimationTest.mergeAndEstimate"
        ):
            continue

        num_sketches = int(r["params"]["numSketches"])
        num_elements = int(r["params"]["numElements"])
        sketch_config = r["params"]["sketchConfig"]
        time_in_seconds = float(r["primaryMetric"]["score"]) * 1e-6 / num_sketches

        if not sketch_config in result:
            result[sketch_config] = []
        result[sketch_config].append((num_elements, time_in_seconds))

    # load c++ benchmark data

    _, data_cpp, size = read_data(
        "results/benchmarks/benchmark-results-merge-and-estimation-cpp.csv"
    )

    for i in range(size):
        num_elements = int(data_cpp["distinct count"][i])
        sketch_config = data_cpp["data structure"][i]

        time_in_seconds = float(data_cpp["merge time (us)"][i]) * 1e-6
        if not sketch_config in result:
            result[sketch_config] = []
        result[sketch_config].append((num_elements, time_in_seconds))

    for x in result:

        result[x] = sorted(result[x])

    return result


def load_merge_data():

    # load java benchmark data
    f = open("results/benchmarks/benchmark-results-java.json")
    data_java = json.load(f)

    result = {}

    for r in data_java:
        if r["benchmark"] != "com.dynatrace.exaloglogpaper.MergeTest.merge":
            continue

        num_sketches = int(r["params"]["numSketches"])
        num_elements = int(r["params"]["numElements"])
        sketch_config = r["params"]["sketchConfig"]
        time_in_seconds = float(r["primaryMetric"]["score"]) * 1e-6 / num_sketches

        if not sketch_config in result:
            result[sketch_config] = []
        result[sketch_config].append((num_elements, time_in_seconds))

    # load c++ benchmark data

    _, data_cpp, size = read_data("results/benchmarks/benchmark-results-merge-cpp.csv")

    for i in range(size):
        num_elements = int(data_cpp["distinct count"][i])
        sketch_config = data_cpp["data structure"][i]

        time_in_seconds = float(data_cpp["merge time (us)"][i]) * 1e-6
        if not sketch_config in result:
            result[sketch_config] = []
        result[sketch_config].append((num_elements, time_in_seconds))

    for x in result:

        result[x] = sorted(result[x])

    return result


def plot_merge_and_estimation(ax):
    data = load_merge_and_estimation_data()

    for algorithm in data:

        if algorithm not in algorithm_styles:
            print(algorithm)
            continue

        ax.plot(
            [d[0] for d in data[algorithm]],
            [d[1] for d in data[algorithm]],
            label=algorithm_styles[algorithm][0],
            color=algorithm_styles[algorithm][1],
            linestyle=algorithm_styles[algorithm][2],
            marker=algorithm_styles[algorithm][3],
            zorder=algorithm_styles[algorithm][4],
        )

    ax.set_ylim([1.5e-7, 1.5e-4])
    ax.text(
        0.03,
        0.97,
        r"merge + estimate",
        transform=ax.transAxes,
        verticalalignment="top",
        horizontalalignment="left",
        bbox=dict(facecolor="wheat", boxstyle="square,pad=0.2"),
    )


def plot_insert(ax):
    data = load_insertion_data()

    for algorithm in data:

        if algorithm not in algorithm_styles:
            print(algorithm)
            continue

        ax.plot(
            [d[0] for d in data[algorithm]],
            [d[1] for d in data[algorithm]],
            label=algorithm_styles[algorithm][0],
            color=algorithm_styles[algorithm][1],
            linestyle=algorithm_styles[algorithm][2],
            marker=algorithm_styles[algorithm][3],
            zorder=algorithm_styles[algorithm][4],
        )

    ax.set_ylim([1e-8, 3e-7])
    ax.text(
        0.03,
        0.97,
        r"insert",
        transform=ax.transAxes,
        verticalalignment="top",
        horizontalalignment="left",
        bbox=dict(facecolor="wheat", boxstyle="square,pad=0.2"),
    )


def plot_merge(ax):
    data = load_merge_data()

    for algorithm in data:

        if algorithm not in algorithm_styles:
            print(algorithm)
            continue

        ax.plot(
            [d[0] for d in data[algorithm]],
            [d[1] for d in data[algorithm]],
            label=algorithm_styles[algorithm][0],
            color=algorithm_styles[algorithm][1],
            linestyle=algorithm_styles[algorithm][2],
            marker=algorithm_styles[algorithm][3],
            zorder=algorithm_styles[algorithm][4],
        )

    ax.set_ylim([1.5e-7, 1.5e-4])
    ax.text(
        0.03,
        0.97,
        r"merge",
        transform=ax.transAxes,
        verticalalignment="top",
        horizontalalignment="left",
        bbox=dict(facecolor="wheat", boxstyle="square,pad=0.2"),
    )


def plot_estimate(ax):
    data = load_estimation_data()

    for algorithm in data:

        if algorithm not in algorithm_styles:
            print(algorithm)
            continue

        ax.plot(
            [d[0] for d in data[algorithm]],
            [d[1] for d in data[algorithm]],
            label=algorithm_styles[algorithm][0],
            color=algorithm_styles[algorithm][1],
            linestyle=algorithm_styles[algorithm][2],
            marker=algorithm_styles[algorithm][3],
            zorder=algorithm_styles[algorithm][4],
        )

    ax.set_ylim([2e-9, 4e-4])
    ax.text(
        0.03,
        0.97,
        r"estimate",
        transform=ax.transAxes,
        verticalalignment="top",
        horizontalalignment="left",
        bbox=dict(facecolor="wheat", boxstyle="square,pad=0.2"),
    )


def plot_serialize(ax):
    data = load_serialization_data()

    for algorithm in data:

        if algorithm not in algorithm_styles:
            print(algorithm)
            continue

        ax.plot(
            [d[0] for d in data[algorithm]],
            [d[1] for d in data[algorithm]],
            label=algorithm_styles[algorithm][0],
            color=algorithm_styles[algorithm][1],
            linestyle=algorithm_styles[algorithm][2],
            marker=algorithm_styles[algorithm][3],
            zorder=algorithm_styles[algorithm][4],
        )

    ax.set_ylim([1.5e-7, 1.5e-5])
    ax.text(
        0.03,
        0.97,
        r"serialize",
        transform=ax.transAxes,
        verticalalignment="top",
        horizontalalignment="left",
        bbox=dict(facecolor="wheat", boxstyle="square,pad=0.2"),
    )


def plot_chart():

    fig, axs = plt.subplots(3, 2, sharex=True, sharey=False)
    fig.set_size_inches(5, 6.5)
    fig.delaxes(axs[0][1])

    for axx in axs:
        for ax in axx:
            ax.set_xscale("log")
            ax.set_yscale("log")
            ax.grid(visible=True)
            ax.set_xlim([10, 1e6])
            ax.xaxis.set_ticks([1e1, 1e2, 1e3, 1e4, 1e5, 1e6])
    plot_insert(axs[0][0])
    plot_estimate(axs[1][0])
    plot_serialize(axs[1][1])
    plot_merge(axs[2][0])
    plot_merge_and_estimation(axs[2][1])

    axs[0][0].set_ylabel(r"average execution time (s)")
    axs[1][0].set_ylabel(r"average execution time (s)")
    axs[2][0].set_ylabel(r"average execution time (s)")
    axs[2][0].set_xlabel(r"distinct count $\symCardinality$")
    axs[2][1].set_xlabel(r"distinct count $\symCardinality$")

    handles, labels = axs[0][0].get_legend_handles_labels()
    legend_order = [3, 2, 4, 5, 1, 0, 6, 7, 8, 9, 10, 11]
    fig.legend(
        [handles[i] for i in legend_order],
        [labels[i] for i in legend_order],
        loc="upper right",
        bbox_to_anchor=(1, 1),
        ncols=1,
        columnspacing=1,
        labelspacing=0.2,
        borderpad=0.2,
        handletextpad=0.4,
        fancybox=False,
        framealpha=1,
    )

    fig.subplots_adjust(
        top=0.99, bottom=0.07, left=0.11, right=0.98, hspace=0.05, wspace=0.22
    )

    fig.savefig(
        "paper/benchmarks.pdf",
        format="pdf",
        dpi=1200,
        metadata={"CreationDate": None, "ModDate": None},
    )
    plt.close(fig)


plot_chart()
