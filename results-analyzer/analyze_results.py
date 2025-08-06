#!/usr/bin/env python3

import argparse
import pandas as pd
import re
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Dict, Any, Tuple, List

def parse_filename(filepath: Path) -> Dict[str, Any]:
    """
    Parse metadata from load-generator result filename.
    
    Expected formats:
    - results_insert_{dbType}_{dataset}_c{clusterSize}_{workers}w_{batchSize}b_{bulkType}_{timestamp}.csv
    - results_query_{dbType}_{queryType}_c{clusterSize}_{workers}w_{queryCount}q_{timestamp}.csv
    """
    filename = filepath.name
    metadata = {}
    
    # Extract operation type
    if filename.startswith("results_insert_"):
        metadata["operation"] = "insert"
        # Pattern: results_insert_{dbType}_{dataset}_c{clusterSize}_{workers}w_{batchSize}b_{bulkType}_{timestamp}.csv
        pattern = r"results_insert_([^_]+)_([^_]+)_c(\d+)_(\d+)w_(\d+)b_([^_]+)_(\d+_\d+)\.csv"
        match = re.match(pattern, filename)
        if match:
            metadata["db_type"] = match.group(1)
            metadata["dataset"] = match.group(2)
            metadata["cluster_size"] = int(match.group(3))
            metadata["workers"] = int(match.group(4))
            metadata["batch_size"] = int(match.group(5))
            metadata["bulk_type"] = match.group(6)
            metadata["timestamp"] = match.group(7)
    
    elif filename.startswith("results_query_"):
        metadata["operation"] = "query"
        # Pattern: results_query_{dbType}_{queryType}_c{clusterSize}_{workers}w_{queryCount}q_{timestamp}.csv
        pattern = r"results_query_([^_]+)_([^_]+)_c(\d+)_(\d+)w_(\d+)q_(\d+_\d+)\.csv"
        match = re.match(pattern, filename)
        if match:
            metadata["db_type"] = match.group(1)
            metadata["query_type"] = match.group(2)
            metadata["cluster_size"] = int(match.group(3))
            metadata["workers"] = int(match.group(4))
            metadata["query_count"] = int(match.group(5))
            metadata["timestamp"] = match.group(6)
    
    return metadata

def detect_file_type(df: pd.DataFrame) -> str:
    """Detect if this is an insert or query results file based on columns."""
    columns = set(df.columns)
    
    insert_columns = {"workerId", "jobType", "batchSize", "useBulkInsert", "insertDurationMs"}
    query_columns = {"workerId", "jobType", "templateName", "queryDurationMs"}
    
    if insert_columns.issubset(columns):
        return "insert"
    elif query_columns.issubset(columns):
        return "query"
    else:
        raise ValueError(f"Unknown file type. Columns: {list(columns)}")

def load_and_process_csv(filepath: Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Load CSV file and process data types."""
    # Parse filename metadata
    metadata = parse_filename(filepath)
    
    # Load CSV
    df = pd.read_csv(filepath, low_memory=False)
    
    # Detect file type from headers
    detected_type = detect_file_type(df)
    
    # Validate against filename
    if metadata.get("operation") and metadata["operation"] != detected_type:
        print(f"Warning: Filename suggests {metadata['operation']} but headers suggest {detected_type}")
    
    metadata["operation"] = detected_type
    
    # Process datetime columns
    datetime_columns = ["startTime", "endTime"]
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    
    # Process boolean columns
    if "useBulkInsert" in df.columns:
        df["useBulkInsert"] = df["useBulkInsert"].astype(bool)
    
    if "successful" in df.columns:
        df["successful"] = df["successful"].astype(bool)
    
    return df, metadata

def load_all_results(results_dir: Path) -> List[Tuple[pd.DataFrame, Dict[str, Any]]]:
    """Load and process all CSV result files in the directory."""
    csv_files = list(results_dir.glob("results_*.csv"))
    
    if not csv_files:
        raise ValueError(f"No results files found in {results_dir}")
    
    print(f"Found {len(csv_files)} result files to process")
    
    all_results = []
    for csv_file in sorted(csv_files):
        try:
            df, metadata = load_and_process_csv(csv_file)
            metadata["filename"] = csv_file.name
            all_results.append((df, metadata))
            print(f"✓ Loaded {csv_file.name}")
        except Exception as e:
            print(f"✗ Error loading {csv_file.name}: {e}")
    
    return all_results

def plot_latency_timeseries(all_results: List[Tuple[pd.DataFrame, Dict[str, Any]]], output_dir: Path) -> None:
    """Create latency time-series plots for each result file."""
    output_dir.mkdir(exist_ok=True)
    
    for df, metadata in all_results:
        if metadata["operation"] == "insert":
            duration_col = "insertDurationMs"
            title_prefix = "Insert"
        elif metadata["operation"] == "query":
            duration_col = "queryDurationMs"
            title_prefix = "Query"
        else:
            continue
            
        if duration_col not in df.columns:
            continue
            
        # Sort by start time to get proper sequence
        df_sorted = df.sort_values('startTime').reset_index(drop=True)
        df_sorted['request_index'] = range(len(df_sorted))
        
        # Create the plot
        plt.figure(figsize=(12, 6))
        plt.plot(df_sorted['request_index'], df_sorted[duration_col], alpha=0.7, linewidth=0.5)
        
        # Add 10k request marker if we have enough data
        if len(df_sorted) >= 10000:
            plt.axvline(x=10000, color='red', linestyle='--', linewidth=2, 
                       label='10,000th request', alpha=0.8)
            plt.legend()
        
        # Formatting
        db_type = metadata.get('db_type', 'Unknown')
        cluster_size = metadata.get('cluster_size', 'Unknown')
        workers = metadata.get('workers', 'Unknown')
        
        plt.title(f'{title_prefix} Latency Over Time\n{db_type} (c{cluster_size}, {workers}w)')
        plt.xlabel('Request Index')
        plt.ylabel(f'{title_prefix} Duration (ms)')
        plt.grid(True, alpha=0.3)
        
        # Save plot
        filename_safe = metadata["filename"].replace('.csv', '_latency_timeseries.png')
        plt.savefig(output_dir / filename_safe, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"✓ Created latency plot: {filename_safe}")

def create_query_comparison_boxplots(all_results: List[Tuple[pd.DataFrame, Dict[str, Any]]], output_dir: Path) -> None:
    """Create comparative box plots for query performance across databases and cluster sizes."""
    output_dir.mkdir(exist_ok=True)
    
    # Filter for query results only
    query_results = [(df, meta) for df, meta in all_results if meta["operation"] == "query"]
    
    if not query_results:
        print("No query results found for box plot analysis")
        return
    
    # Group data by query template
    query_templates = set()
    for df, _ in query_results:
        if 'templateName' in df.columns:
            query_templates.update(df['templateName'].unique())
    
    for template in query_templates:
        plt.figure(figsize=(12, 8))
        
        # Prepare data for the box plot
        plot_data = []
        plot_labels = []
        
        # Order: CrateDB c3, MobilityDB c3, CrateDB c6, MobilityDB c6
        configs = [
            ('crateDB', 3), ('mobilityDB', 3), ('crateDB', 6), ('mobilityDB', 6)
        ]
        
        for db_type, cluster_size in configs:
            # Find matching results
            template_data = []
            for df, meta in query_results:
                if (meta.get('db_type') == db_type and 
                    meta.get('cluster_size') == cluster_size and
                    'templateName' in df.columns and
                    'queryDurationMs' in df.columns):
                    
                    template_df = df[df['templateName'] == template]
                    if not template_df.empty:
                        template_data.extend(template_df['queryDurationMs'].tolist())
            
            if template_data:
                plot_data.append(template_data)
                plot_labels.append(f'{db_type}\nc{cluster_size}')
        
        if len(plot_data) >= 2:  # Only create plot if we have data from multiple configs
            # Create box plot
            box_plot = plt.boxplot(plot_data, tick_labels=plot_labels, patch_artist=True)
            
            # Color the boxes
            colors = ['lightblue', 'lightcoral', 'lightgreen', 'lightyellow']
            for patch, color in zip(box_plot['boxes'], colors[:len(plot_data)]):
                patch.set_facecolor(color)
            
            plt.title(f'Query Performance Comparison: {template}')
            plt.xlabel('Database Configuration')
            plt.ylabel('Query Duration (ms)')
            plt.yscale('log')  # Use log scale for better visualization of wide ranges
            plt.grid(True, alpha=0.3)
            plt.xticks(rotation=45)
            
            # Save plot
            filename_safe = f'query_comparison_{template.replace(" ", "_")}.png'
            plt.savefig(output_dir / filename_safe, dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"✓ Created query comparison plot: {filename_safe}")
        else:
            plt.close()
            print(f"✗ Insufficient data for query template: {template}")

def analyze_all_results(all_results: List[Tuple[pd.DataFrame, Dict[str, Any]]], output_dir: Path) -> None:
    """Perform comprehensive analysis of all results."""
    print("=" * 80)
    print("COMPREHENSIVE BENCHMARK RESULTS ANALYSIS")
    print("=" * 80)
    
    # Summary statistics
    insert_files = [meta for _, meta in all_results if meta["operation"] == "insert"]
    query_files = [meta for _, meta in all_results if meta["operation"] == "query"]
    
    print(f"\nDataset Summary:")
    print(f"  Total files processed: {len(all_results)}")
    print(f"  Insert benchmark files: {len(insert_files)}")
    print(f"  Query benchmark files: {len(query_files)}")
    
    # Database and cluster breakdown
    db_configs = set()
    for _, meta in all_results:
        db_type = meta.get('db_type', 'Unknown')
        cluster_size = meta.get('cluster_size', 'Unknown')
        db_configs.add(f"{db_type}-c{cluster_size}")
    
    print(f"  Database configurations: {', '.join(sorted(db_configs))}")
    
    print(f"\nGenerating visualizations...")
    
    # Create plots directory
    plots_dir = output_dir / "plots"
    plots_dir.mkdir(exist_ok=True)
    
    # Generate latency time-series plots
    print(f"\n📊 Creating latency time-series plots...")
    plot_latency_timeseries(all_results, plots_dir / "latency_timeseries")
    
    # Generate query comparison box plots
    print(f"\n📊 Creating query performance comparison box plots...")
    create_query_comparison_boxplots(all_results, plots_dir / "query_comparisons")
    
    print(f"\n✅ Analysis complete! Check the plots in: {plots_dir}")
    print(f"   - Latency time-series: {plots_dir / 'latency_timeseries'}")
    print(f"   - Query comparisons: {plots_dir / 'query_comparisons'}")
    
    # Individual file statistics
    print(f"\nDetailed Statistics by File:")
    print("-" * 40)
    
    for df, metadata in all_results:
        print(f"\n📁 {metadata['filename']}")
        print(f"   Operation: {metadata.get('operation', 'Unknown')}")
        print(f"   Database: {metadata.get('db_type', 'Unknown')}")
        print(f"   Cluster Size: c{metadata.get('cluster_size', 'Unknown')}")
        print(f"   Workers: {metadata.get('workers', 'Unknown')}")
        print(f"   Records: {len(df):,}")
        
        if metadata.get("operation") == "insert" and "insertDurationMs" in df.columns:
            duration_stats = df["insertDurationMs"].describe()
            print(f"   Insert Duration (ms): {duration_stats['mean']:.1f} ± {duration_stats['std']:.1f}")
            print(f"   Range: {duration_stats['min']:.1f} - {duration_stats['max']:.1f}")
            
            if "successfullyInserted" in df.columns:
                total_inserted = df["successfullyInserted"].sum()
                print(f"   Total Inserted: {total_inserted:,}")
        
        elif metadata.get("operation") == "query" and "queryDurationMs" in df.columns:
            duration_stats = df["queryDurationMs"].describe()
            print(f"   Query Duration (ms): {duration_stats['mean']:.1f} ± {duration_stats['std']:.1f}")
            print(f"   Range: {duration_stats['min']:.1f} - {duration_stats['max']:.1f}")
            
            if "successful" in df.columns:
                success_rate = df["successful"].mean() * 100
                print(f"   Success Rate: {success_rate:.1f}%")

def analyze_data(df: pd.DataFrame, metadata: Dict[str, Any]) -> None:
    """Perform basic analysis and display results."""
    print("=" * 60)
    print("LOAD GENERATOR RESULTS ANALYSIS")
    print("=" * 60)
    
    print(f"\nFile Metadata:")
    print(f"  Operation: {metadata.get('operation', 'Unknown')}")
    print(f"  Database: {metadata.get('db_type', 'Unknown')}")
    print(f"  Cluster Size: {metadata.get('cluster_size', 'Unknown')}")
    print(f"  Workers: {metadata.get('workers', 'Unknown')}")
    
    if metadata.get("operation") == "insert":
        print(f"  Dataset: {metadata.get('dataset', 'Unknown')}")
        print(f"  Batch Size: {metadata.get('batch_size', 'Unknown')}")
        print(f"  Bulk Type: {metadata.get('bulk_type', 'Unknown')}")
    elif metadata.get("operation") == "query":
        print(f"  Query Type: {metadata.get('query_type', 'Unknown')}")
        print(f"  Query Count: {metadata.get('query_count', 'Unknown')}")
    
    print(f"  Timestamp: {metadata.get('timestamp', 'Unknown')}")
    
    print(f"\nDataset Info:")
    print(f"  Shape: {df.shape}")
    print(f"  Columns: {list(df.columns)}")
    
    print(f"\nBasic Statistics:")
    if metadata.get("operation") == "insert":
        if "insertDurationMs" in df.columns:
            duration_stats = df["insertDurationMs"].describe()
            print(f"  Insert Duration (ms):")
            print(f"    Mean: {duration_stats['mean']:.2f}")
            print(f"    Median: {duration_stats['50%']:.2f}")
            print(f"    Min: {duration_stats['min']:.2f}")
            print(f"    Max: {duration_stats['max']:.2f}")
        
        if "successfullyInserted" in df.columns:
            total_inserted = df["successfullyInserted"].sum()
            print(f"  Total Records Inserted: {total_inserted:,}")
        
        if "failedInserts" in df.columns:
            total_failed = df["failedInserts"].sum()
            print(f"  Total Failed Inserts: {total_failed:,}")
    
    elif metadata.get("operation") == "query":
        if "queryDurationMs" in df.columns:
            duration_stats = df["queryDurationMs"].describe()
            print(f"  Query Duration (ms):")
            print(f"    Mean: {duration_stats['mean']:.2f}")
            print(f"    Median: {duration_stats['50%']:.2f}")
            print(f"    Min: {duration_stats['min']:.2f}")
            print(f"    Max: {duration_stats['max']:.2f}")
        
        if "successful" in df.columns:
            success_rate = df["successful"].mean() * 100
            print(f"  Success Rate: {success_rate:.2f}%")
        
        if "templateName" in df.columns:
            template_counts = df["templateName"].value_counts()
            print(f"  Query Templates Used: {len(template_counts)}")
            print("  Top Templates:")
            for template, count in template_counts.head().items():
                print(f"    {template}: {count}")

def main():
    parser = argparse.ArgumentParser(
        description="Analyze load-generator benchmark results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python analyze_results.py results/
  python analyze_results.py /path/to/load-generator/results/
        """
    )
    
    parser.add_argument(
        "results_dir",
        type=Path,
        help="Path to the directory containing CSV results files to analyze"
    )
    
    args = parser.parse_args()
    
    if not args.results_dir.exists():
        print(f"Error: Directory '{args.results_dir}' does not exist")
        return 1
    
    if not args.results_dir.is_dir():
        print(f"Error: '{args.results_dir}' is not a directory")
        return 1
    
    try:
        # Load all result files
        all_results = load_all_results(args.results_dir)
        
        if not all_results:
            print("No valid result files found")
            return 1
        
        # Perform comprehensive analysis
        analyze_all_results(all_results, args.results_dir)
        
    except Exception as e:
        print(f"Error processing results: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
