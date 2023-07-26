# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import statistics
from math import ceil

from amazon.ionbenchmark.benchmark_runner import BenchmarkResult
from amazon.ionbenchmark.benchmark_spec import BenchmarkSpec


def report_stats(benchmark_spec: BenchmarkSpec, benchmark_result: BenchmarkResult, report_fields: list = None):
    """
    Generate a report for the outcome of a running a benchmark.

    Available fields:
     * `name` – the name of the benchmark
     * `operation` – the operation performed by the benchmark ('load', 'loads', 'dump', 'dumps')
     * `file_size` – the size of the input data used in this benchmark
     * `input_file` – the file used for this benchmark
     * `format` – the format used for this benchmark
     * `memory_usage_peak` – the peak amount of memory allocated while running the benchmark function
     * `time_<stat>` – time statistic for the benchmark; `<stat>` can be `mean`, `min`, `max`, `median`, or `p<n>` where
                       `<n>` is any number from 0 to 100 inclusive.
     * `rate_<stat>` – throughput statistic for the benchmark; `<stat>` can be `mean`, `min`, `max`, `median`, or `p<n>`
                       where `<n>` is any number from 0 to 100 inclusive.

    :param benchmark_spec: The spec for the benchmark that was run
    :param benchmark_result: The output from the benchmark
    :param report_fields: list[str] of fields to include in the report.
    :return:
    """
    if report_fields is None:
        report_fields = ['file_size', 'time_min', 'time_mean', 'memory_usage_peak']

    result = {'name': benchmark_spec.get_name()}

    for field in report_fields:
        if isinstance(field, str) and field.startswith("time_"):
            # Note–we use `field[len("time_"):]` instead of `removeprefix("time_")` to support python 3.7 and 3.8
            stat_value = _calculate_timing_stat(field[len("time_"):], benchmark_result.timings, benchmark_result.batch_size)
            result[f'{field}(ns)'] = stat_value
        elif isinstance(field, str) and field.startswith("rate_"):
            timing_value = _calculate_timing_stat(field[len("rate_"):], benchmark_result.timings, benchmark_result.batch_size)
            stat_value = ceil(benchmark_spec.get_input_file_size() * 1024 / (timing_value / benchmark_result.batch_size / 1000000000))
            result[f'{field}(kB/s)'] = stat_value
        elif field == 'format':
            result['format'] = benchmark_spec.get_format()
        elif field == 'input_file':
            result['input_file'] = os.path.basename(benchmark_spec.get_input_file())
        elif field == 'operation':
            result['operation'] = benchmark_spec.get_operation_name()
        elif field == 'file_size':
            result['file_size(B)'] = benchmark_spec.get_input_file_size()
        elif field == 'memory_usage_peak':
            result['memory_usage_peak(B)'] = benchmark_result.peak_memory_usage
        elif field == 'name':
            pass
        else:
            raise ValueError(f"Unrecognized report field '{field}'")

    return result


def _calculate_timing_stat(stat: str, timings, batch_size):
    """
    Calculate a statistic for the given timings.

    :param stat: Name of a statistic. Can be `min`, `max`, `median`, `mean`, or `p<N>` where `N` is 0 to 100 exclusive.
    :param timings: List of result times from running the benchmark function.
    :param batch_size: Number of times the benchmark function was invoked to produce a single timing result.
    :return:
    """
    if stat.startswith("p"):
        n = int(stat[1:])
        x = ceil(statistics.quantiles(timings, n=100, method='inclusive')[n-1]/batch_size)
    elif stat == 'mean':
        x = ceil(sum(timings) / (batch_size * len(timings)))
    elif stat == 'min':
        x = ceil(min(timings) / batch_size)
    elif stat == 'max':
        x = ceil(max(timings) / batch_size)
    elif stat == 'median':
        x = ceil(statistics.median(timings) / batch_size)
    else:
        raise ValueError(f"Unrecognized statistic {stat}")
    return x

