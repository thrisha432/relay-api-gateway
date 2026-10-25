# Load tests

Load and stress tests live in [`benchmarks/`](../../benchmarks/README.md) and
run with k6 against the Docker Compose stack. They are not part of `pytest`,
because they need the full stack and take minutes.
