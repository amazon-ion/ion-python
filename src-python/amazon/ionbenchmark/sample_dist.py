# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import statistics

# singleton NormalDist that is the _unit normal distribution_, used for getting z-scores for confidence levels
_unit_normal = statistics.NormalDist()


class SampleDist(list):
    """
    Represents a normal-ish sample distribution. Similar to [NormalDist], but retains the source data.
    """

    def __init__(self, data: list):
        super().__init__(data)
        self.__max = max(data)
        self.__min = min(data)
        self.__mean = statistics.fmean(data)  # This runs faster than the mean() function, and it always returns a float
        self.__stdev = statistics.stdev(data)
        self.__variance = statistics.variance(data, self.__mean)

    @property
    def min(self):
        """Return the min value in the sample set."""
        return self.__min

    @property
    def max(self):
        """Return the max value in the sample set."""
        return self.__max

    @property
    def mean(self):
        """Return the mean of the sample distribution."""
        return self.__mean

    @property
    def variance(self):
        """Return the variance of the sample distribution."""
        return self.__variance

    @property
    # Sometimes also spelled "stddev", but Python Std Lib statistic module uses "stdev".
    def stdev(self):
        """Return the standard deviation of the sample distribution."""
        return self.__stdev

    @property
    def rstdev(self):
        """Return the standard deviation of the sample distribution as a ratio relative to the sample mean"""
        return self.__stdev / self.__mean

    def margin_of_error(self, confidence: float):
        """
        Return the margin of error of the mean of this sample distribution at the given confidence level. Can be
        combined with the mean to express the confidence interval as `mean ± error`.
        """
        z_score = _unit_normal.inv_cdf((1 + confidence) / 2.)
        return z_score * self.stdev / (len(self) ** .5)
