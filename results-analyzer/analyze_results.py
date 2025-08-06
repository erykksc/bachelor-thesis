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
            db_type_raw = match.group(1)
            metadata["db_type"] = "MobilityDBC" if db_type_raw == "mobilityDB" else db_type_raw
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
            db_type_raw = match.group(1)
            metadata["db_type"] = "MobilityDBC" if db_type_raw == "mobilityDB" else db_type_raw
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

def apply_benchmark_filtering(df: pd.DataFrame, start_filter_min: int = 4, end_filter_min: int = 1) -> pd.DataFrame:
    """Filter out first N minutes and last M minutes of benchmark run to remove artifacts."""
    if df.empty or 'startTime' not in df.columns:
        return df
    
    # Calculate benchmark time boundaries
    benchmark_start = df['startTime'].min()
    benchmark_end = df['startTime'].max()
    
    # Define filtering boundaries
    start_cutoff = benchmark_start + pd.Timedelta(minutes=start_filter_min)
    end_cutoff = benchmark_end - pd.Timedelta(minutes=end_filter_min)
    
    # Apply filtering
    filtered_df = df[(df['startTime'] >= start_cutoff) & (df['startTime'] <= end_cutoff)]
    
    return filtered_df

def create_insert_performance_table(all_results: List[Tuple[pd.DataFrame, Dict[str, Any]]]) -> pd.DataFrame:
    """Create a summary table for insert performance metrics."""
    insert_results = [(df, meta) for df, meta in all_results if meta["operation"] == "insert"]
    
    if not insert_results:
        return pd.DataFrame()
    
    table_data = []
    
    for df, metadata in insert_results:
        # Apply benchmark filtering
        df_filtered = apply_benchmark_filtering(df)
        
        if df_filtered.empty:
            continue
            
        # Extract metadata
        database = metadata.get('db_type', 'Unknown')
        cluster_size = metadata.get('cluster_size', 'Unknown')
        workers = metadata.get('workers', 'Unknown')
        
        # Calculate metrics
        if 'insertDurationMs' in df_filtered.columns:
            # Success rate calculation
            total_operations = len(df_filtered)
            successful_operations = len(df_filtered[df_filtered['insertDurationMs'] > 0])
            success_rate = (successful_operations / total_operations * 100) if total_operations > 0 else 0
            
            # Duration statistics (only for successful operations)
            valid_durations = df_filtered[df_filtered['insertDurationMs'] > 0]['insertDurationMs']
            if not valid_durations.empty:
                total_duration = valid_durations.sum()
                std_deviation = valid_durations.std()
                mean_duration = valid_durations.mean()
            else:
                total_duration = 0
                std_deviation = 0
                mean_duration = 0
            
            # Total records inserted
            total_records = df_filtered['successfullyInserted'].sum() if 'successfullyInserted' in df_filtered.columns else 0
            
            table_data.append({
                'Database': database,
                'Cluster Size': f'c{cluster_size}',
                'Workers': workers,
                'Success Rate (%)': f'{success_rate:.1f}',
                'Total Duration (s)': f'{total_duration/1000:.1f}',
                'Mean Duration (ms)': f'{mean_duration:.1f}',
                'Std Deviation (ms)': f'{std_deviation:.1f}',
                'Total Records': f'{total_records:,}',
                'Operations': total_operations
            })
    
    return pd.DataFrame(table_data)

def plot_latency_distributions(all_results: List[Tuple[pd.DataFrame, Dict[str, Any]]], output_dir: Path) -> None:
    """Create latency distribution plots for each result file."""
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
        
        # Apply benchmark filtering (remove first 4min and last 1min)
        df_filtered = apply_benchmark_filtering(df)
        
        if df_filtered.empty:
            print(f"✗ No data remaining after filtering for {metadata.get('filename', 'unknown')}")
            continue
        
        # Remove zero or negative durations
        valid_durations = df_filtered[df_filtered[duration_col] > 0][duration_col]
        
        if valid_durations.empty:
            print(f"✗ No valid durations for {metadata.get('filename', 'unknown')}")
            continue
        
        # Create the distribution plot
        plt.figure(figsize=(12, 8))
        
        # Create histogram with KDE overlay
        sns.histplot(data=valid_durations, kde=True, alpha=0.6, color='skyblue', edgecolor='black')
        
        # Use log scale for better visualization of wide ranges
        plt.xscale('log')
        
        # Add statistical markers
        median_val = valid_durations.median()
        p95_val = valid_durations.quantile(0.95)
        
        plt.axvline(median_val, color='red', linestyle='-', linewidth=2, alpha=0.8, label=f'Median: {median_val:.1f}ms')
        plt.axvline(p95_val, color='orange', linestyle='--', linewidth=2, alpha=0.8, label=f'95th %ile: {p95_val:.1f}ms')
        
        # Formatting
        db_type = metadata.get('db_type', 'Unknown')
        cluster_size = metadata.get('cluster_size', 'Unknown')
        workers = metadata.get('workers', 'Unknown')
        
        plt.title(f'{title_prefix} Latency Distribution (Filtered)\n{db_type} (c{cluster_size}, {workers}w)')
        plt.xlabel(f'{title_prefix} Duration (ms)')
        plt.ylabel('Frequency')
        plt.grid(True, alpha=0.3)
        plt.legend()
        
        # Save plot with descriptive filename
        operation = metadata.get("operation", "unknown")
        db_type_safe = metadata.get("db_type", "unknown")
        cluster_size = metadata.get("cluster_size", "unknown")
        workers = metadata.get("workers", "unknown")
        
        filename_safe = f"latency_distribution_{operation}_{db_type_safe}_c{cluster_size}_{workers}w.pdf"
        plt.savefig(output_dir / filename_safe, bbox_inches='tight')
        plt.close()
        
        print(f"✓ Created latency distribution plot: {filename_safe}")

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
        plt.figure(figsize=(15, 10))
        
        # Collect all unique configurations for this template
        template_configs = set()
        for df, meta in query_results:
            if ('templateName' in df.columns and 
                template in df['templateName'].values):
                config = (meta.get('db_type'), meta.get('cluster_size'), meta.get('workers'))
                template_configs.add(config)
        
        # Sort configurations for consistent ordering
        template_configs = sorted(list(template_configs))
        
        plot_data = []
        plot_labels = []
        legend_labels = []
        
        # Collect data for each configuration
        for db_type, cluster_size, workers in template_configs:
            template_data = []
            for df, meta in query_results:
                if (meta.get('db_type') == db_type and 
                    meta.get('cluster_size') == cluster_size and
                    meta.get('workers') == workers and
                    'templateName' in df.columns and
                    'queryDurationMs' in df.columns):
                    
                    template_df = df[df['templateName'] == template]
                    if not template_df.empty:
                        # Apply benchmark filtering to box plot data
                        template_df_filtered = apply_benchmark_filtering(template_df)
                        if not template_df_filtered.empty:
                            # Remove zero or negative durations
                            valid_durations = template_df_filtered[template_df_filtered['queryDurationMs'] > 0]['queryDurationMs']
                            template_data.extend(valid_durations.tolist())
            
            if template_data:
                plot_data.append(template_data)
                # Create short labels for x-axis
                plot_labels.append(f'{db_type[:5]}\nc{cluster_size}')
                # Create detailed labels for legend
                legend_labels.append(f'{db_type} c{cluster_size} ({workers}w)')
        
        if len(plot_data) >= 2:  # Only create plot if we have data from multiple configs
            # Create box plot
            box_plot = plt.boxplot(plot_data, tick_labels=plot_labels, patch_artist=True)
            
            # Color the boxes with different colors/patterns
            colors = ['lightblue', 'lightcoral', 'lightgreen', 'lightyellow', 
                     'lightpink', 'lightgray', 'lightcyan', 'wheat']
            for i, (patch, color) in enumerate(zip(box_plot['boxes'], colors[:len(plot_data)])):
                patch.set_facecolor(color)
                # Add slight transparency to distinguish overlapping configs
                patch.set_alpha(0.8)
            
            plt.title(f'Query Performance Comparison: {template}')
            plt.xlabel('Database Configuration')
            plt.ylabel('Query Duration (ms)')
            plt.yscale('log')  # Use log scale for better visualization of wide ranges
            plt.grid(True, alpha=0.3)
            plt.xticks(rotation=45)
            
            # Add legend
            legend_patches = [plt.Rectangle((0,0),1,1, facecolor=colors[i], alpha=0.8) 
                            for i in range(len(legend_labels))]
            plt.legend(legend_patches, legend_labels, 
                      loc='upper left', bbox_to_anchor=(1, 1))
            
            # Determine if this is simple or complex queries based on metadata
            query_complexity = "unknown"
            for df, meta in query_results:
                if ('templateName' in df.columns and 
                    template in df['templateName'].values):
                    query_type = meta.get('query_type', '')
                    if 'simple' in query_type.lower():
                        query_complexity = "simple"
                        break
                    elif 'complex' in query_type.lower():
                        query_complexity = "complex"
                        break
            
            # Save plot with descriptive filename
            template_clean = template.replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_")
            filename_safe = f'boxplot_query_{query_complexity}_{template_clean}.pdf'
            plt.savefig(output_dir / filename_safe, bbox_inches='tight')
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
    
    # Generate and display insert performance table
    print(f"\n" + "=" * 80)
    print("INSERT PERFORMANCE SUMMARY TABLE")
    print("=" * 80)
    
    insert_table = create_insert_performance_table(all_results)
    if not insert_table.empty:
        print(insert_table.to_string(index=False))
        
        # Save table to CSV file
        table_output_file = output_dir / "insert_performance_summary.csv"
        insert_table.to_csv(table_output_file, index=False)
        print(f"\n💾 Insert performance table saved to: {table_output_file}")
    else:
        print("No insert data found for table generation")
    
    print(f"\nGenerating visualizations...")
    
    # Create plots directory
    plots_dir = output_dir / "plots"
    plots_dir.mkdir(exist_ok=True)
    
    # Generate latency distribution plots
    print(f"\n📊 Creating latency distribution plots...")
    plot_latency_distributions(all_results, plots_dir / "latency_distributions")
    
    # Generate query comparison box plots
    print(f"\n📊 Creating query performance comparison box plots...")
    create_query_comparison_boxplots(all_results, plots_dir / "query_comparisons")
    
    print(f"\n✅ Analysis complete! Check the plots in: {plots_dir}")
    print(f"   - Latency distributions: {plots_dir / 'latency_distributions'}")
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
