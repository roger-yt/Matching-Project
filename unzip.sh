#!/usr/bin/env bash
set -euo pipefail

# change this to wherever your graphs live
GRAPH_DIR="graphs"

cd "$GRAPH_DIR"

for archive in *.tar; do
  echo "Extracting $archive…"
  tar -xvf "$archive"
done

echo "All .tar files extracted."