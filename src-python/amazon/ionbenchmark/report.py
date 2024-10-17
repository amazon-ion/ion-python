# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Callable

from dataclasses import dataclass

from amazon.ionbenchmark.benchmark_runner import BenchmarkResult
from amazon.ionbenchmark.benchmark_spec import BenchmarkSpec


@dataclass(frozen=True)
class ReportField:
    """
    Represents a field that can be included in the benchmark test report
    """
    name: str
    compute_fn: Callable  # Callable[[BenchmarkSpec, BenchmarkResult], Any]
    units: str = None
    # Direction of improvement, used by the compare command
    doi: int = None


REPORT_FIELDS = [
    # TODO: Make sure we have the fields we need to perform a statistically meaningful comparison
    # I.e. if we end up needing to use ANOVA or Independent Samples T Test, do we have the fields we need?
    ReportField(name="format",
                compute_fn=lambda spec, _: spec.get_format()),
    ReportField(name="input_file",
                compute_fn=lambda spec, _: spec.get_input_file()),
    ReportField(name="operation",
                compute_fn=lambda spec, _: spec.derive_operation_name()),
    ReportField(name="file_size", units="B", doi=-1,
                compute_fn=lambda spec, _: spec.get_input_file_size()),
    ReportField(name="memory_usage_peak", units="B", doi=-1,
                compute_fn=lambda _, result: result.peak_memory_usage),
    ReportField(name="time_mean", units="ns", doi=-1,
                compute_fn=lambda _, result: result.nanos_per_op.mean),
    ReportField(name="time_min", units="ns", doi=-1,
                compute_fn=lambda _, result: result.nanos_per_op.min),
    ReportField(name="time_max", units="ns", doi=-1,
                compute_fn=lambda _, result: result.nanos_per_op.max),
    ReportField(name="time_sd", units="ns",
                compute_fn=lambda _, result: result.nanos_per_op.stdev),
    ReportField(name="time_rsd", units="%",
                compute_fn=lambda _, result: result.nanos_per_op.rstdev * 100),
    ReportField(name="time_error", units="ns",
                compute_fn=lambda _, result: result.nanos_per_op.margin_of_error(confidence=0.999)),
    ReportField(name="ops/s_mean", doi=+1,
                compute_fn=lambda _, result: result.ops_per_second.mean),
    ReportField(name="ops/s_min", doi=+1,
                compute_fn=lambda _, result: result.ops_per_second.min),
    ReportField(name="ops/s_max", doi=+1,
                compute_fn=lambda _, result: result.ops_per_second.max),
    ReportField(name="ops/s_sd",
                compute_fn=lambda _, result: result.ops_per_second.stdev),
    ReportField(name="ops/s_rsd", units="%",
                compute_fn=lambda _, result: result.ops_per_second.rstdev * 100),
    ReportField(name="ops/s_error",
                compute_fn=lambda _, result: result.ops_per_second.margin_of_error(confidence=0.999)),
]


def get_report_field_by_name(name: str):
    for field in REPORT_FIELDS:
        if name == field.name:
            return field
    raise ValueError(f"Not a valid report field: {name}")


def report_stats(benchmark_spec: BenchmarkSpec, benchmark_result: BenchmarkResult, report_fields: list):
    """
    Generate a report for the outcome of a running a benchmark.

    Available fields:
     * `name` – the name of the benchmark
     * `operation` – the operation performed by the benchmark ('load', 'loads', 'dump', 'dumps')
     * `file_size` – the size of the input data used in this benchmark
     * `input_file` – the file used for this benchmark
     * `format` – the format used for this benchmark
     * `memory_usage_peak` – the peak amount of memory allocated while running the benchmark function
     * `time_<stat>` – time statistic for the benchmark
     * `ops/s_<stat>` – number of operations (invocations of the benchmark function) per second

    `<stat>` can be `mean`, `min`, `max`, `median`, `error`, `stdev`, or `rstdev`

    :param benchmark_spec: The spec for the benchmark that was run
    :param benchmark_result: The output from the benchmark
    :param report_fields: list[str] of fields to include in the report.
    :return:
    """

    result = {'name': benchmark_spec.get_name()}

    for field_name in report_fields:
        field = get_report_field_by_name(field_name)
        if field.units is not None:
            key = f"{field.name}({field.units})"
        else:
            key = field.name
        result[key] = field.compute_fn(benchmark_spec, benchmark_result)

    return result
