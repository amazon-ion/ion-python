## What is this?

This is an example of a benchmark spec that can be run using the `ion_benchmark_cli` tool. You can get started by running:

```shell
python3 ./amazon/ionbenchmark/ion_benchmark_cli.py run tests/benchmark_sample_data/sample_spec/spec.ion
```

The `spec.ion` file defines a group of benchmarks to be run together.

Common parameters include:
 * TODO: list them here

The CLI tool assigns default values for all required parameters except
 * TODO: list them here

Format specific parameters include:
 * TODO: list them here

To provide your own default values, use the `-d` option followed by an Ion struct or a file containing an Ion struct.
E.g.:
```shell
python3 ./amazon/ionbenchmark/ion_benchmark_cli.py run tests/benchmark_sample_data/sample_spec/spec.ion -d '{io_type:file}'
```

To override values in the spec file, use the `-O` option followed by an Ion struct or a file containing an Ion struct.
E.g.:
```shell
python3 ./amazon/ionbenchmark/ion_benchmark_cli.py run tests/benchmark_sample_data/sample_spec/spec.ion -O '{iterations:1000}'
```

## Contents of this directory

* `cat.cbor` – CBOR encoding of the "Cat" data
* `cat.desc` – Protobuf Descriptor File for the cat.proto schema
* `cat.ion` – Ion text encoding of the "Cat" data
* `cat.json` – JSON encoding of the "Cat" data
* `cat.proto` – Protobuf schema for "Cat" data
* `cat.protobuf_data` – Protobuf encoding of the "Cat" data
* `cat.sd_protobuf_data` – Self-describing Protobuf encoding of the "Cat" data
* `spec.ion` – A BenchmarkSpec file for comparing Ion, JSON, CBOR, and Protobuf formats of the same "Cat" data
