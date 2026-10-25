"""Run independent D01 checkers on a completed local reproduction."""
import argparse
import importlib.util
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", choices=["jcr", "transport"])
    parser.add_argument("--output", type=Path, help="Completed reproduction directory")
    args = parser.parse_args()
    output = (args.output or ROOT / "outputs" / args.suite).resolve()
    if not (output / "LOCAL_FILE_MANIFEST.json").is_file():
        raise FileNotFoundError("Run the experiment with the current reproduce.py first")
    checks = ROOT / "checks"
    sys.path.insert(0, str(checks))
    spec = importlib.util.spec_from_file_location("d01_independent_audit", checks / f"audit_joint_{'calendar' if args.suite == 'jcr' else 'tcp'}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.ROOT = output
    module.O = output
    module.R = output / "audit"
    module.R.mkdir(exist_ok=True)
    module.main()


if __name__ == "__main__":
    main()
