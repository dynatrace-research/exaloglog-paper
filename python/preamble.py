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
import matplotlib

matplotlib.use("PDF")
import matplotlib.pyplot as plt

latex_preamble = ""
with open("paper/symbols.tex") as f:
    for l in f.readlines():
        if not "%" in l:
            latex_preamble += l[:-1]

latex_preamble += r"\RequirePackage[T1]{fontenc} \RequirePackage[tt=false, type1=true]{libertine} \RequirePackage[varqu]{zi4} \RequirePackage[libertine]{newtxmath}\RequirePackage{amsmath}"
matplotlib.use("PDF")

plt.rc("text", usetex=True)
plt.rc("text.latex", preamble=latex_preamble)


algorithm_styles = {
    "ULTRALOGLOG_10": (r"Hash4j ULL ($\symPrecision=10$)", "C1", "solid", None, 2),
    "HYPERLOGLOG_11": (r"Hash4j HLL ($\symPrecision=11$)", "C1", "dotted", None, 2),
    "EXALOGLOG_2_20_8": (
        r"ELL ($\symExtraBits=2$, $\symNumExtraBits=20$, $\symPrecision=8$, ML)",
        "k",
        "solid",
        None,
        2.5,
    ),
    "EXALOGLOG_2_20_8_MARTINGALE": (
        r"ELL ($\symExtraBits=2$, $\symNumExtraBits=20$, $\symPrecision=8$, marting.)",
        "k",
        (0, (5, 1)),
        None,
        2.5,
    ),
    "EXALOGLOG_2_24_8": (
        r"ELL ($\symExtraBits=2$, $\symNumExtraBits=24$, $\symPrecision=8$, ML)",
        "k",
        (0, (1, 1)),
        None,
        2.5,
    ),
    "EXALOGLOG_2_24_8_MARTINGALE": (
        r"ELL ($\symExtraBits=2$, $\symNumExtraBits=24$, $\symPrecision=8$, marting.)",
        "k",
        (0, (3, 1, 1, 1)),
        None,
        2.5,
    ),
    "APACHE_DATA_SKETCHES_CPC_10": (
        r"DataSketches CPC ($\symPrecision=10$)",
        "C9",
        "dashdot",
        None,
        2.1,
    ),
    "APACHE_DATA_SKETCHES_HLL4_11": (
        r"DataSketches HLL (4-bit, $\symPrecision=11$)",
        "C3",
        "solid",
        None,
        2,
    ),
    "APACHE_DATA_SKETCHES_HLL6_11": (
        r"DataSketches HLL (6-bit, $\symPrecision=11$)",
        "C3",
        "dashed",
        None,
        2,
    ),
    "APACHE_DATA_SKETCHES_HLL8_11": (
        r"DataSketches HLL (8-bit, $\symPrecision=11$)",
        "C3",
        (0, (1, 1)),
        None,
        2,
    ),
    "HyperLogLogLog (p=11)": (
        r"HyperLogLogLog (\symPrecision=11)",
        "C0",
        (0, (3, 1, 1, 1, 1, 1)),
        None,
        2.1,
    ),
    "SpikeSketch (128 buckets)": (
        r"SpikeSketch (128 buckets)",
        "C2",
        "dashdot",
        None,
        2,
    ),
}
