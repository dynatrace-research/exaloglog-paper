#
# Copyright (c) 2024 Dynatrace LLC. All rights reserved.
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
from mpmath import mp

mp.dps = 100
max_t = 64 - 6


def calculate_martingale_theoretical_relative_standard_error_constant(t, d):
    b = mp.power(2.0, mp.power(2.0, -t))
    x = mp.power(b, -d) / (b - 1.0)
    return mp.sqrt((mp.ln(b) / 2) * (1.0 + x))


def print_martingale_theoretical_relative_standard_error_constant(t, d):
    print(
        f"t={t}, d={d}, relative error = {calculate_martingale_theoretical_relative_standard_error_constant(t=t, d=d)}"
    )


print("{")
first_t = True
for t in range(max_t + 1):
    if first_t:
        first_t = False
    else:
        print(",")
    first_d = True
    s = "{"
    for d in range(64 - 6 - t + 1):
        if first_d:
            first_d = False
        else:
            s += ", "
        s += str(
            float(
                calculate_martingale_theoretical_relative_standard_error_constant(
                    t=t, d=d
                )
            )
        )
    s += "}"
    print(s)
print("}")
