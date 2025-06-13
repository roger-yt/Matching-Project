#!/usr/bin/env python3
"""
match_and_eval.py

1. Read the total number of edges (nnz) from the MatrixMarket header.
2. Compute memory budget = fraction * nnz.
3. Compute in-core optimal matching (opt).
4. Stream blocks under the memory budget to compute streaming matching (alg).
5. Report alg, opt, and accuracy = alg/opt.
"""

import argparse
import networkx as nx
import random
import os

def parse_args():
    p = argparse.ArgumentParser(
        description="Compute in-core & streaming matchings within a memory budget and report accuracy"
    )
    p.add_argument(
        "--graph-dir",
        type=str,
        help="Path to input MatrixMarket file"
    )
    p.add_argument(
        "--fraction",
        type=float,
        default=0.3,
        help="Max fraction of edges (nnz) usable in memory (<= 1.0)"
    )
    p.add_argument(
        "--max-passes",
        type=int,
        default=5,
        help="Number of full streaming passes"
    )
    p.add_argument(
        "--shuffle",
        action="store_true",
        help="Shuffle edges before streaming"
    )
    p.add_argument(
        "--overlap",
        action="store_true",
        help="Use 50% overlapping windows in streaming"
    )
    return p.parse_args()

def read_nnz(mmfile):
    """
    Scan past comments to find the header line: nrows, ncols, nnz.
    Returns nnz as an integer.
    """
    with open(mmfile) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('%'):
                continue
            parts = line.split()
            if len(parts) == 3 and all(p.isdigit() for p in parts):
                return int(parts[2])
    raise ValueError("Could not find header with nnz")

def read_edges(mmfile):
    """
    Yield undirected edges (u, v) from any MatrixMarket coord file.
    Skips comments, header line, and self-loops.
    """
    with open(mmfile) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('%'):
                continue
            parts = line.split()
            # skip header line "nrows ncols nnz"
            if len(parts) == 3 and all(p.isdigit() for p in parts):
                continue
            u, v = int(parts[0]), int(parts[1])
            if u == v:
                continue
            # normalize so u < v
            if u > v:
                u, v = v, u
            yield u, v

def compute_optimal_matching(mmfile):
    """In-core: load full graph and compute true maximum matching."""
    G = nx.Graph()
    G.add_edges_from(read_edges(mmfile))
    M_opt = nx.max_weight_matching(G, maxcardinality=True)
    opt = len(M_opt)
    print(f"[In-core] optimal matching size (opt) = {opt}")
    return opt

def streaming_matching(mmfile, budget, max_passes, shuffle=False, overlap=False):
    """
    Out-of-core: stream in blocks/windows ensuring |M| + |block| <= budget.
    """
    edges = list(read_edges(mmfile))
    if shuffle:
        random.shuffle(edges)

    # prepare windows (overlapping if requested)
    if overlap:
        step = max(1, budget // 2)
        windows = [edges[i:i+budget] for i in range(0, len(edges), step)]
    else:
        windows = [edges[i:i+budget] for i in range(0, len(edges), budget)]

    M = set()
    for pass_num in range(1, max_passes + 1):
        print(f"\n[Streaming] Pass {pass_num}/{max_passes}")
        improved = False

        for window in windows:
            avail = budget - len(M)
            if avail <= 0:
                print("  ▶ Memory budget full (|M| ≥ budget); stopping streaming.")
                break
            block = window[:avail]

            G = nx.Graph()
            G.add_edges_from(M)
            G.add_edges_from(block)
            new_match = nx.max_weight_matching(G, maxcardinality=True)

            if len(new_match) > len(M):
                print(f"  Augmented: {len(M)} → {len(new_match)}")
                M = set(new_match)
                improved = True

        if not improved:
            print("  No improvement this pass, stopping early.")
            break

        if shuffle and not overlap:
            random.shuffle(edges)
            windows = [edges[i:i+budget] for i in range(0, len(edges), budget)]

    alg = len(M)
    print(f"[Streaming] final matching size (alg) = {alg}")
    return alg

def get_acc(mmfile, budget, max_passes, shuffle, overlap):
    opt = compute_optimal_matching(mmfile)
    alg = streaming_matching(
        mmfile,
        budget=budget,
        max_passes=max_passes,
        shuffle=shuffle,
        overlap=overlap
    )

    acc = alg / opt if opt > 0 else 1.0
    return acc
def main():
    args = parse_args()
    accs = []
    for dir in os.listdir(args.graph_dir):
        dir_path = os.path.join(args.graph_dir, dir)
        for file in os.listdir(dir_path):
            file_path = os.path.join(dir_path, file)
            try:
                nnz = read_nnz(file_path)
            except:
                continue
            print("file_path=", file_path)
            budget = max(1, int(args.fraction * nnz))
            print(f"Total edges (nnz) = {nnz}, memory budget = {budget} edges ({args.fraction*100:.0f}% of E)\n")
            acc = get_acc(file_path, budget, args.max_passes, args.shuffle, args.overlap)
            print("temp acc=", acc)
            accs.append(acc)
    print("average acc=", sum(accs)/len(accs))

    # opt = compute_optimal_matching(args.mmfile)
    # alg = streaming_matching(
    #     args.mmfile,
    #     budget=budget,
    #     max_passes=args.max_passes,
    #     shuffle=args.shuffle,
    #     overlap=args.overlap
    # )

    # acc = alg / opt if opt > 0 else 0.0
    # print(f"\nAccuracy (alg/opt) = {alg}/{opt} = {acc:.4f}")

if __name__ == "__main__":
    main()
