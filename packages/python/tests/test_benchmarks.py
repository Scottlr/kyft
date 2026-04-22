import subprocess
import sys
from pathlib import Path


def test_python_benchmark_harness_smoke_runs() -> None:
    script = Path(__file__).parents[1] / "benchmarks" / "spanfold_benchmarks.py"

    completed = subprocess.run(
        [sys.executable, str(script), "--smoke", "--scenario", "small"],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert "benchmark,mean_ms,min_ms" in completed.stdout
    assert "small.run_overlap" in completed.stdout
