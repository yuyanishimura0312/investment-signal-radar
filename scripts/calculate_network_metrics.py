"""
Co-investment network analysis for Investment Signal Radar.

Builds a co-investment graph from round_participants data and calculates
centrality metrics for each investor node, storing results in network_metrics.
"""

import sqlite3
import subprocess
import sys
from collections import defaultdict
import datetime as dt
from datetime import datetime, timezone
from pathlib import Path

# Ensure networkx is available
try:
    import networkx as nx
except ImportError:
    print("networkx not found. Installing...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "networkx"])
    import networkx as nx

DB_PATH = Path(__file__).parent.parent / "data" / "investment_signal_v2.db"


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def build_co_investment_graph(conn):
    """Build a weighted co-investment graph.

    Nodes = investor organizations (is_investor=1)
    Edges = co-investment relationships (shared funding round)
    Edge weight = number of shared rounds
    """
    cur = conn.cursor()

    # Fetch all (funding_round_id, investor_id) pairs
    cur.execute("""
        SELECT rp.funding_round_id, rp.investor_id
        FROM round_participants rp
        JOIN organizations o ON o.id = rp.investor_id
        WHERE o.is_investor = 1
        ORDER BY rp.funding_round_id
    """)
    rows = cur.fetchall()

    # Group investors by round
    round_investors = defaultdict(list)
    for row in rows:
        round_investors[row["funding_round_id"]].append(row["investor_id"])

    print(f"Loaded {len(rows)} participation records across {len(round_investors)} rounds")

    # Build graph
    G = nx.Graph()

    # Add all investor nodes first
    cur.execute("SELECT id FROM organizations WHERE is_investor = 1")
    investor_ids = [r["id"] for r in cur.fetchall()]
    G.add_nodes_from(investor_ids)

    # Add edges for co-investors in the same round
    for round_id, investors in round_investors.items():
        for i in range(len(investors)):
            for j in range(i + 1, len(investors)):
                u, v = investors[i], investors[j]
                if G.has_edge(u, v):
                    G[u][v]["weight"] += 1
                else:
                    G.add_edge(u, v, weight=1)

    print(f"Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G


def calculate_metrics(G):
    """Calculate centrality metrics for all investor nodes."""
    print("\nCalculating degree_centrality...")
    degree_centrality = nx.degree_centrality(G)

    print("Calculating betweenness_centrality (this may take a while)...")
    betweenness_centrality = nx.betweenness_centrality(G, normalized=True, weight="weight")

    print("Calculating eigenvector_centrality...")
    try:
        eigenvector_centrality = nx.eigenvector_centrality(
            G, max_iter=1000, tol=1e-6, weight="weight"
        )
    except (nx.PowerIterationFailedConvergence, Exception) as e:
        print(f"  eigenvector_centrality failed ({e}), falling back to degree_centrality")
        eigenvector_centrality = degree_centrality.copy()

    print("Calculating co_investment_count (sum of edge weights)...")
    co_investment_count = {
        node: float(sum(data.get("weight", 1) for _, data in G[node].items()))
        for node in G.nodes()
    }

    print("Calculating unique_co_investors (raw degree)...")
    unique_co_investors = {node: float(deg) for node, deg in G.degree()}

    return {
        "degree_centrality": degree_centrality,
        "betweenness_centrality": betweenness_centrality,
        "eigenvector_centrality": eigenvector_centrality,
        "co_investment_count": co_investment_count,
        "unique_co_investors": unique_co_investors,
    }


def insert_metrics(conn, metrics):
    """Clear and repopulate network_metrics table."""
    cur = conn.cursor()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    cur.execute("DELETE FROM network_metrics")
    deleted = cur.rowcount
    print(f"\nCleared {deleted} existing rows from network_metrics")

    rows_to_insert = []
    for metric_type, values in metrics.items():
        for org_id, metric_value in values.items():
            rows_to_insert.append((org_id, metric_type, metric_value, now))

    cur.executemany(
        "INSERT INTO network_metrics (organization_id, metric_type, metric_value, calculated_at) VALUES (?, ?, ?, ?)",
        rows_to_insert,
    )
    conn.commit()
    print(f"Inserted {len(rows_to_insert)} rows into network_metrics")
    return len(rows_to_insert)


def print_top_investors(conn, metric_type, top_n=20):
    """Print top N investors for a given metric."""
    cur = conn.cursor()
    cur.execute("""
        SELECT o.name, o.name_en, nm.metric_value
        FROM network_metrics nm
        JOIN organizations o ON o.id = nm.organization_id
        WHERE nm.metric_type = ?
        ORDER BY nm.metric_value DESC
        LIMIT ?
    """, (metric_type, top_n))
    rows = cur.fetchall()

    print(f"\n--- Top {top_n} by {metric_type} ---")
    for rank, row in enumerate(rows, 1):
        name = row["name"] or row["name_en"] or f"(id unknown)"
        print(f"  {rank:2d}. {name:<40} {row['metric_value']:.6f}")


def main():
    print(f"DB: {DB_PATH}")
    print(f"networkx version: {nx.__version__}\n")

    conn = get_connection()

    # Step 1: Build graph
    G = build_co_investment_graph(conn)

    # Step 2: Calculate metrics
    metrics = calculate_metrics(G)

    # Step 3: Insert into DB
    total_inserted = insert_metrics(conn, metrics)

    # Step 4: Print top 20 per metric
    for metric_type in ["degree_centrality", "betweenness_centrality", "eigenvector_centrality", "co_investment_count", "unique_co_investors"]:
        print_top_investors(conn, metric_type, top_n=20)

    # Step 5: Summary
    print("\n=== Summary ===")
    print(f"  Nodes (investors): {G.number_of_nodes()}")
    print(f"  Edges (co-investment pairs): {G.number_of_edges()}")
    print(f"  Metrics inserted: {total_inserted}")
    print(f"  Metric types: {', '.join(metrics.keys())}")
    connected_components = nx.number_connected_components(G)
    largest_cc = max(nx.connected_components(G), key=len)
    print(f"  Connected components: {connected_components}")
    print(f"  Largest component size: {len(largest_cc)} nodes")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
