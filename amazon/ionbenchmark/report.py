# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import statistics
from math import ceil

from amazon.ionbenchmark.benchmark_runner import BenchmarkResult
from amazon.ionbenchmark.benchmark_spec import BenchmarkSpec


def report_stats(benchmark_spec: BenchmarkSpec, benchmark_result: BenchmarkResult, report_fields: list[str] = None):
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
    :param report_fields: List of fields to include in the report.
    :return:
    """
    if report_fields is None:
        report_fields = ['file_size', 'time_min', 'time_mean', 'memory_usage_peak']

    result = {'name': benchmark_spec.get_name()}

    for field in report_fields:
        match field:
            case str(s) if s.startswith("time_"):
                stat_value = _calculate_timing_stat(s.removeprefix("time_"), benchmark_result.timings, benchmark_result.batch_size)
                result[f'{s}(ns)'] = stat_value
            case str(s) if s.startswith("rate_"):
                timing_value = _calculate_timing_stat(s.removeprefix("rate_"), benchmark_result.timings, benchmark_result.batch_size)
                stat_value = ceil(benchmark_spec.get_input_file_size() * 1024 / (timing_value / benchmark_result.batch_size / 1000000000))
                result[f'{s}(kB/s)'] = stat_value
            case 'format':
                result['format'] = benchmark_spec.get_format()
            case 'input_file':
                result['input_file'] = os.path.basename(benchmark_spec.get_input_file())
            case 'operation':
                result['operation'] = benchmark_spec.get_operation_name()
            case 'file_size':
                result['file_size(B)'] = benchmark_spec.get_input_file_size()
            case 'memory_usage_peak':
                result['memory_usage_peak(B)'] = benchmark_result.peak_memory_usage
            case 'name':
                pass
            case _:
                raise ValueError(f"Unrecognized report field '{field}'")

    return result


def _calculate_timing_stat(stat: str, timings, batch_size):
    """
    Calculate a statistic for the given timings.

    :param stat: Name of a statistic. Can be `min`, `max`, `median`, `mean`, or `p<N>` where `N` is 0 to 100 inclusive.
    :param timings: List of result times from running the benchmark function.
    :param batch_size: Number of times the benchmark function was invoked to produce a single timing result.
    :return:
    """
    if stat.startswith("p"):
        n = int(stat.removeprefix("p"))
        x = ceil(statistics.quantiles(timings, n=100, method='inclusive')[n]/batch_size)
    else:
        match stat:
            case 'mean':
                x = ceil(sum(timings) / (batch_size * len(timings)))
            case 'min':
                x = ceil(min(timings) / batch_size)
            case 'max':
                x = ceil(max(timings) / batch_size)
            case 'median':
                x = ceil(statistics.median(timings) / batch_size)
            case _:
                raise ValueError(f"Unrecognized statistic {stat}")
    return x

