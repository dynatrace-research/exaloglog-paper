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
from collections import namedtuple
from scipy.optimize import minimize_scalar
from scipy.optimize import minimize
import numpy

mp.dps = 100

Result = namedtuple("Result", ["q", "d", "b", "mvp"])


def expm1divx(x):
    if x == 0.0:
        return mp.mpf("1")
    else:
        return mp.expm1(x) / x


def calculate_fisher_information(d, b):
    if b > 1:
        return mp.zeta(2.0, 1.0 + mp.power(b, -d) / (b - 1.0)) / mp.log(b)
    else:
        return mp.mpf("1")


def entropy_integrand(d, b, z):
    assert b > 1
    p = mp.power(b, -d) / (b - 1)
    return mp.power(z, p) * ((1 - z) * mp.log1p(-z) / (z * mp.log(z)))


def calculate_entropy(d, b):
    if b > 1:
        p = mp.power(b, -d) / (b - 1)
        i = mp.quad(lambda z: entropy_integrand(d=d, b=b, z=z), [0, 1])
        return (mp.mpf("1.0") / (1 + p) + i) / (mp.log(2) * mp.log(b))
    else:
        return mp.mpf("inf")


def mvp_ml_compressed_func(d, b):
    return float(calculate_entropy(d=d, b=b) / calculate_fisher_information(d=d, b=b))


def mvp_martingale_compressed_func(d, b):
    return float(
        calculate_entropy(d=d, b=b)
        * (b - 1.0 + mp.power(b, -d))
        / (2.0 * expm1divx(mp.log(b)))
    )


def mvp_ml_compressed(d=None, b=None):
    d_max = 100
    b_min = 1
    b_max = 5

    result = None
    if d is not None and b is not None:
        mvp = mvp_ml_compressed_func(d=d, b=b)
        result = Result(None, d, b, mvp)
    elif d is None:
        for d in range(0, d_max + 1):
            r = mvp_ml_compressed(d, b)
            if result is None or r.mvp < result.mvp:
                result = r
    elif d is not None and b is None:
        r = minimize_scalar(
            lambda x: mvp_ml_compressed_func(d=d, b=x),
            bounds=(b_min, b_max),
            method="Bounded",
            options={"xatol": 1e-20},
        )
        assert r.success
        result = Result(None, d, r.x, r.fun)

    assert result is not None
    assert result.mvp > 0
    return result


def mvp_martingale_compressed(d=None, b=None):
    d_max = 100
    b_min = 1
    b_max = 5

    result = None
    if d is not None and b is not None:
        mvp = mvp_martingale_compressed_func(d=d, b=b)
        result = Result(None, d, b, mvp)
    elif d is None:
        for d in range(0, d_max + 1):
            r = mvp_martingale_compressed(d, b)
            if result is None or r.mvp < result.mvp:
                result = r
    elif d is not None and b is None:
        r = minimize_scalar(
            lambda x: mvp_martingale_compressed_func(d=d, b=x),
            bounds=(b_min, b_max),
            method="Bounded",
            options={"xatol": 1e-20},
        )
        assert r.success
        result = Result(None, d, r.x, r.fun)

    assert result is not None
    assert result.mvp > 0
    return result


def mvp_martingale_func(q, d, b):
    x = (b - 1.0 + mp.power(b, -d)) / (2.0 * expm1divx(mp.log(b)))
    required_bits = q + d
    return float(x * required_bits)


def mvp_martingale(q, d=None, b=None):
    d_max = 100
    b_min = 1
    b_max = 5

    result = None
    if d is not None and b is not None:
        mvp = mvp_martingale_func(q, d, b)
        result = Result(q, d, b, mvp)
    elif d is None:
        for d in range(0, d_max + 1):
            r = mvp_martingale(q, d, b)
            if result is None or r.mvp < result.mvp:
                result = r
    elif d is not None and b is None:
        r = minimize_scalar(
            lambda x: mvp_martingale_func(q, d, x),
            bounds=(b_min, b_max),
            method="Bounded",
            options={"xatol": 1e-20},
        )
        assert r.success
        result = Result(q, d, r.x, r.fun)

    assert result is not None
    assert result.mvp > 0
    return result


def mvp_ml_func(q, d, b):
    required_bits = q + d
    if b > 1.0:
        return float(
            required_bits * mp.log(b) / mp.zeta(2.0, 1.0 + mp.power(b, -d) / (b - 1.0))
        )
    else:
        return required_bits


def mvp_ml(q, d=None, b=None):
    d_max = 100
    b_min = 1
    b_max = 5

    result = None
    if d is not None and b is not None:
        mvp = mvp_ml_func(q, d, b)
        result = Result(q, d, b, mvp)
    elif d is None:
        for d in range(0, d_max + 1):
            r = mvp_ml(q, d, b)
            if result is None or r.mvp < result.mvp:
                result = r
    elif d is not None and b is None:
        r = minimize_scalar(
            lambda x: mvp_ml_func(q, d, x),
            bounds=(b_min, b_max),
            method="Bounded",
            options={"xatol": 1e-20},
        )
        assert r.success
        result = Result(q, d, r.x, r.fun)

    assert result is not None
    assert result.mvp > 0
    return result


def print_result(r, coefficients_calculator=None):
    s = str(r)

    if r.q is not None:
        v = r.mvp / (r.q + r.d)
        efficiency = mvp_ml_func(r.q, r.d, r.b) / r.mvp

        s += ", v = " + str(v)
        s += ", efficiency = " + str(efficiency)
    if coefficients_calculator is not None:
        coefficients = coefficients_calculator(r)

        for i in range(0, min(len(coefficients), 8)):
            s += ", eta" + str(i) + " = " + str(coefficients[i])

    print(s)


if __name__ == "__main__":
    print("ML estimation:")
    print_result(mvp_ml(q=6, b=2, d=0))
    print_result(mvp_ml(q=6, b=2, d=1))
    print_result(mvp_ml(q=6, b=2, d=2))
    print_result(mvp_ml(q=7, b=pow(2.0, 1.0 / 2), d=9))
    print_result(mvp_ml(q=8, b=pow(2.0, 1.0 / 4), d=16))
    print_result(mvp_ml(q=8, b=pow(2.0, 1.0 / 4), d=20))
    print_result(mvp_ml(q=8, b=pow(2.0, 1.0 / 4), d=24))

    print("martingale estimation:")
    print_result(mvp_martingale(q=6, b=2, d=0))
    print_result(mvp_martingale(q=6, b=2, d=1))
    print_result(mvp_martingale(q=6, b=2, d=2))
    print_result(mvp_martingale(q=7, b=pow(2.0, 1.0 / 2), d=9))
    print_result(mvp_martingale(q=8, b=pow(2.0, 1.0 / 4), d=16))
    print_result(mvp_martingale(q=8, b=pow(2.0, 1.0 / 4), d=20))
    print_result(mvp_martingale(q=8, b=pow(2.0, 1.0 / 4), d=24))
