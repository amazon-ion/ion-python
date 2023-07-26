#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A repeatable benchmark tool for ion-python implementation.

Usage:
    ion_python_benchmark_cli.py (run|read|write|compare) [<args>]...
    ion_python_benchmark_cli.py (-v | --version)
    ion_python_benchmark_cli.py (-h | --help)

Command:
    write       Benchmark writing the given input file to the given output format(s). In order to isolate writing from
                reading, during the setup phase write instructions are generated from the input file and stored in
                memory. For large inputs, this can consume a lot of resources and take a long time to execute.

    read        First, re-write the given input file to the given output format(s) (if necessary), then benchmark
                reading the resulting log files.

    run         Run benchmarks for a benchmark spec file.

    compare     Compare the benchmark results generated by benchmarking ion-python from different commits. After the
                comparison process, relative changes of speed will be calculated and written into an Ion Struct.

Options:
     -h, --help                         Show this screen.
     -v, --version                      Display the tool version
"""
import itertools
import os
import platform

import amazon.ion.simpleion as ion
from docopt import docopt
from tabulate import tabulate

from amazon.ionbenchmark.Format import format_is_ion, rewrite_file_to_format
from amazon.ionbenchmark.benchmark_runner import run_benchmark
from amazon.ionbenchmark.report import report_stats
from amazon.ionbenchmark.benchmark_spec import BenchmarkSpec

# Relate pypy incompatible issue - https://github.com/amazon-ion/ion-python/issues/227
TOOL_VERSION = '1.0.0'


def _str_to_bool(v):
    """
    Convert a string CLI arg to a boolean
    """
    if isinstance(v, str):
        return v.lower() in ("true", "1")
    return False


def compare_command():
    """
    Compare the results of two benchmarks to determine if <new_result> has regressed compared to <previous_result>.

    Usage:
        ion_python_benchmark_cli.py compare <previous_result> <new_result> [-fq][--threshold <THRESHOLD>][--output <PATH>][-c <FIELD>]...

    Arguments:
        <previous_result>      A report from running a benchmark at some point in the past.

        <new_result>           A new report to compare against

    Options:
        -c <FIELD>, --compare <FIELD>     A field to compare in the reports. [default: file_size(B) time_min(ns)]
        -o --output PATH       File to write the regression report.
        -q --quiet             Suppress writing regressions to std out. [default: false]
        -t <FLOAT>, --threshold <FLOAT>         Margin of error for comparison. [default: 0.20]
        -f --fail              End with a non-zero exit code when a regression is detected. [default: false]
    """
    args = docopt(compare_command.__doc__)
    previous_path = args['<previous_result>']
    current_path = args['<new_result>']
    output_file_for_comparison = args['--output']
    regression_threshold = float(args['--threshold'])
    comparison_keywords = args['--compare']

    with open(previous_path, 'br') as p, open(current_path, 'br') as c:
        previous_results = ion.load(p)
        current_results = ion.load(c)
        # Creates a list to hold compared scores
        report = []
        has_regression = False
        # For results of each configuration pattern with the same file
        for idx, prev_result in enumerate(previous_results):
            cur_result = current_results[idx]
            name = cur_result['name']
            result = {'name': name}
            for keyword in comparison_keywords:
                cur = float(cur_result[keyword])
                prev = float(prev_result[keyword])
                relative_diff = (cur - prev) / prev
                pct_diff = f"{relative_diff:.2%}"
                result[keyword] = pct_diff

                if relative_diff > regression_threshold:
                    if not _str_to_bool(args['--quiet']):
                        print(f"{name} '{keyword}' changed by {pct_diff}: {prev} => {cur}")
                    has_regression = True

            report.append(result)

        if output_file_for_comparison:
            with open(output_file_for_comparison, 'bw') as o:
                ion.dump(report, o, binary=False)

        if not _str_to_bool(args['--quiet']):
            print(tabulate(report, tablefmt='fancy_grid', headers='keys'))

        if has_regression:
            if not _str_to_bool(args['--fail']):
                exit("One or more regressions detected.")


def read_write_command(read_or_write: str):
    # When the docs are rendered in the CLI, <read_or_write> will be replaced with 'read' or 'write' depending on which
    # command the user has entered.
    """
    Run a benchmark that <read_or_write>s data.

    Usage:
        ion_python_benchmark_cli.py <read_or_write> [--report <fields>] [--results-file <path>] [--api <api>]... [--iterator <bool>] [--c-extension <bool>] [--warmups <int>] [--iterations <int>] [--format <format>]... [--io-type <io_type>]... <input_file>

    Options:
         -h, --help                         Show this screen.

         -o, --results-file <path>          Destination for the benchmark results. By default, results will be written to
                                            stdout. Otherwise the results will be written to a file with the path <path>.

         --api <api>                        The API to exercise (load_dump, streaming). `load_dump` refers to
                                            the load/dump method. `streaming` refers to ion-python's event
                                            based non-blocking API specifically. Default to `load_dump`.

         -t, --iterator <bool>              If returns an iterator for simpleIon C extension read API. [default: False]

         -w, --warmups <int>                Number of benchmark warm-up iterations. [default: 1]

         -i, --iterations <int>             Number of benchmark iterations. [default: 100]

         -c, --c-extension <bool>           If the C extension is enabled, note that it only applies to simpleIon module.
                                            [default: True]

         -f, --format <format>              Format to benchmark, from the set (ion_binary | ion_text | json | simplejson |
                                            ujson | rapidjson | cbor | cbor2). May be specified multiple times to
                                            compare different formats. [default: ion_binary]

         -I, --io-type <io_type>            The source or destination type, from the set (buffer | file). If buffer is
                                            selected, buffers the input data in memory before reading and writes the output
                                            data to an in-memory buffer instead of a file. [default: file]

        -r --report FIELDS      Comma-separated list of fields to include in the report. [default: file_size, time_min, time_mean, memory_usage_peak ]

    """
    doc = read_write_command.__doc__.replace("<read_or_write>", read_or_write)
    arguments = docopt(doc, help=True)
    file = arguments['<input_file>']

    iterations = int(arguments['--iterations'])
    warmups = int(arguments['--warmups'])
    c_extension = _str_to_bool(arguments['--c-extension']) and platform.python_implementation() == 'CPython'
    iterator = _str_to_bool(arguments['--iterator'])
    output = arguments['--results-file']
    report_fields = arguments["--report"]

    applies_to_all = dict(
        command=read_or_write,
        iterations=iterations,
        warmups=warmups,
        c_extension=c_extension,
        iterator=iterator,
        input_file=file,
    )

    # For options may show up more than once, initialize them as below and added them into list option_configuration.
    # initialize options that might show up multiple times
    api = [*set(arguments['--api'])] if arguments['--api'] else ['load_dump']
    format_option = [*set(arguments['--format'])] if arguments['--format'] else ['ion_binary']
    io_type = [*set(arguments['--io-type'])] if arguments['--io-type'] else ['file']

    # option_configuration is used for tracking options may show up multiple times.
    option_configuration = [api, format_option, io_type]
    option_configuration_combination = list(itertools.product(*option_configuration))
    option_configuration_combination.sort()

    specs = []
    for (api, format_option, io_type) in list(itertools.product(*option_configuration)):
        spec = {'api': api, 'format': format_option, 'io_type': io_type}
        specs.append(BenchmarkSpec(spec, user_overrides=applies_to_all))

    _run_benchmarks(specs, report_fields, output)


def run_spec_command():
    """
    Run one or more benchmarks based on a benchmark spec.

    Usage:
        ion_python_benchmark_cli.py run [options] [--] <spec>

    Arguments:
        <specs>                 A file or string of Ion Text containing one or more benchmark specs.

    Options:
        -d --default SPEC       A file or string of Ion Text containing one benchmark spec. This benchmark spec
                                overrides the tool defaults, but has lower precedence than anything in the <specs>
                                argument.

        -O --override SPEC      A file or string of Ion Text containing one benchmark spec. Any value in this benchmark
                                spec overrides the corresponding value and the <specs> argument.

        -o --output FILE        Destination to store the report. If unset, prints to std out.

        -r --report FIELDS      Comma-separated list of fields to include in the report. [default: file_size, time_min, time_mean, memory_usage_peak]

    Example:
        ./ion_python_benchmark_cli.py run my_spec_file.ion -d '{iterations:1000}' -o '{warmups:0}' -r "time_min, file_size, peak_memory_usage"
    """
    args = docopt(run_spec_command.__doc__, help=True)
    spec = args["<spec>"]
    defaults = args["--default"]
    overrides = args["--override"]
    report_fields = args["--report"]
    output = args["--output"]

    if defaults is None:
        default_spec = {}
    elif os.path.exists(defaults):
        with open(defaults, 'rb') as f:
            default_spec = ion.load(f)
    else:
        default_spec = ion.loads(defaults)

    if overrides is None:
        override_spec = {}
    elif os.path.exists(overrides):
        with open(defaults, 'rb') as f:
            override_spec = ion.load(f)
    else:
        override_spec = ion.loads(overrides)

    if os.path.exists(spec):
        with open(spec, 'rb') as f:
            data = ion.load(f, single_value=False)
            spec_dir = os.path.dirname(spec)
    else:
        data = ion.loads(spec, single_value=False)
        spec_dir = os.getcwd()

    # Make sure they are actually dicts, not IonPyDict, so that the merge operator works
    default_spec = {**default_spec}
    override_spec = {**override_spec}

    specs = [BenchmarkSpec({**d}, default_spec, override_spec, spec_dir) for d in data]

    _run_benchmarks(specs, report_fields, output)


def _run_benchmarks(specs: list, report_fields, output_file):
    """
    Run benchmarks for the `read`, `write`, and `run` commands.

    The read/write/run commands build the BenchmarkSpecs and then delegate to this function for the common logic of
    running the actual benchmarks.

    :param specs: List of `BenchmarkSpec` for which to create and run test cases.
    :type specs: list[BenchmarkSpec]
    :param report_fields: List of single fields or a string containing a comma-delimited list of fields to include in
           the report.
    :type report_fields: list[str] | str
    :param output_file: Optional location to save the machine-readable report. Human-readable table is printed to stdout
           regardless of whether this parameter is set.
    :type output_file: str | None
    """
    if isinstance(report_fields, str):
        report_fields = [f.strip() for f in report_fields.split(',')]
    report = []

    for benchmark_spec in specs:
        # TODO. currently, we must provide the tool to convert to a corresponding file format for read benchmarking.
        #  For example, we must provide a CBOR file for CBOR APIs benchmarking. We cannot benchmark CBOR APIs by giving
        #  a JSON file. Lack of format conversion prevents us from benchmarking different formats concurrently.
        if format_is_ion(benchmark_spec.get_format()):
            benchmark_spec['input_file'] = rewrite_file_to_format(benchmark_spec.get_input_file(),
                                                                  benchmark_spec.get_format())

        result = run_benchmark(benchmark_spec)
        result_stats = report_stats(benchmark_spec, result, report_fields)
        report.append(result_stats)

    print(tabulate(report, tablefmt='fancy_grid', headers='keys'))

    if output_file:
        des_dir = os.path.dirname(output_file)
        if des_dir != '' and des_dir is not None and not os.path.exists(des_dir):
            os.makedirs(des_dir)
        with open(output_file, 'bw') as fp:
            ion.dump(report, fp, binary=False)


def _main():
    args = docopt(__doc__, help=True, options_first=True, version=TOOL_VERSION)
    if args['read']:
        read_write_command('read')
    elif args['write']:
        read_write_command('write')
    elif args['run']:
        run_spec_command()
    elif args['compare']:
        compare_command()
    else:
        exit(f"Invalid command. See help for usage.")


if __name__ == '__main__':
    _main()
