from __future__ import annotations

import sys

from mr_graph.cli import main as cli_main


if __name__ == "__main__":
    raise SystemExit(cli_main(sys.argv[1:]))
