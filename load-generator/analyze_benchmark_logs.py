#!/usr/bin/env python3
"""
Benchmark Log Analysis Script

Parses JSON log files from the Go load-generator and creates comprehensive
performance analysis with pandas DataFrames.

Extracts:
- CLI configuration and dataset metadata
- Per-operation performance metrics (latency, success rates)
- Worker-level statistics
- Time-series throughput analysis
"""

import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from pathlib import Path
import argparse
from typing import Dict, List, Optional, Tuple
import warnings

warnings.filterwarnings('ignore')

class BenchmarkLogAnalyzer:
    """Analyzer for load-generator benchmark logs"""
    
    def __init__(self, log_file: str):
        self.log_file = log_file
        self.config = {}
        self.events = []
        self.df = None
        
    def parse_logs(self) -> None:
        """Parse JSON log file and extract configuration and events"""
        print(f"Parsing log file: {self.log_file}")
        
        with open(self.log_file, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    log_entry = json.loads(line)
                    self._process_log_entry(log_entry)
                except json.JSONDecodeError as e:
                    print(f"Warning: Could not parse line {line_num}: {e}")
                    continue
                    
        print(f"Parsed {len(self.events)} benchmark events")
        print(f"Configuration: {self.config}")
    
    def _process_log_entry(self, entry: Dict) -> None:
        """Process individual log entry"""
        msg = entry.get('msg', '')
        
        # Extract configuration from startup message
        if msg == "Starting load-generator with following cli arguments":
            self.config.update({
                'db_target': entry.get('db'),
                'districts_path': entry.get('districts'),
                'pois_path': entry.get('pois'),
                'trips_path': entry.get('trips'),
                'ddl_path': entry.get('ddl'),
                'mode': entry.get('mode'),
                'num_workers': entry.get('nworkers'),
                'skip_init': entry.get('skip-init'),
                'log_debug': entry.get('log-debug'),
                'queries_per_worker': entry.get('queries-per-worker'),
                'seed': entry.get('seed'),
                'query_templates': entry.get('qtemplates'),
                'trip_events_csv': entry.get('tevents')
            })
            
        # Extract dataset info
        elif "Loaded and parsed districts" in msg:
            self.config['total_districts'] = entry.get('count')
        elif "Loaded and parsed pois" in msg:
            self.config['total_pois'] = entry.get('count')
        elif "Loaded read queries templates" in msg:
            self.config['total_templates'] = entry.get('count')
            
        # Extract benchmark timing events
        elif msg in ["Starting Insert Benchmark", "Starting Query Benchmark"]:
            self.config['benchmark_start'] = entry.get('time')
            self.config['benchmark_type'] = 'insert' if 'Insert' in msg else 'query'
            
        # Extract performance events
        elif msg == "Worker finished insert":
            self._extract_insert_event(entry)
        elif msg == "Query worker finished query":
            self._extract_query_event(entry)
            
    def _extract_insert_event(self, entry: Dict) -> None:
        """Extract insert performance event"""
        event = {
            'timestamp': entry.get('time'),
            'job_type': 'insert',
            'worker_id': entry.get('workerId'),
            'start_time': entry.get('startTime'),
            'end_time': entry.get('endTime'),
            'latency_ms': entry.get('insertTime'),
            'wait_time_ms': entry.get('waitedForJobTime'),
            'successful': entry.get('successful'),
            'cmd_tag': entry.get('cmdTag'),
            'error': entry.get('queryErr'),
            'template_name': None,
            'query_index': None
        }
        self.events.append(event)
        
    def _extract_query_event(self, entry: Dict) -> None:
        """Extract query performance event"""
        event = {
            'timestamp': entry.get('time'),
            'job_type': 'query',
            'worker_id': entry.get('workerId'),
            'start_time': entry.get('startTime'),
            'end_time': entry.get('endTime'),
            'latency_ms': entry.get('queryDurationInMs'),
            'wait_time_ms': None,
            'successful': entry.get('successful'),
            'cmd_tag': None,
            'error': entry.get('error'),
            'template_name': entry.get('templateName'),
            'query_index': entry.get('queryIndex')
        }
        self.events.append(event)
        
    def create_dataframe(self) -> pd.DataFrame:
        """Create pandas DataFrame from parsed events"""
        if not self.events:
            raise ValueError("No events found. Run parse_logs() first.")
            
        self.df = pd.DataFrame(self.events)
        
        # Add configuration columns to each row
        for key, value in self.config.items():
            self.df[f'config_{key}'] = value
            
        # Convert timestamps
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        self.df['start_time'] = pd.to_datetime(self.df['start_time'])
        self.df['end_time'] = pd.to_datetime(self.df['end_time'])
        
        # Calculate relative timing
        if not self.df.empty:
            benchmark_start = self.df['start_time'].min()
            self.df['seconds_from_start'] = (self.df['start_time'] - benchmark_start).dt.total_seconds()
            
        print(f"Created DataFrame with {len(self.df)} rows and {len(self.df.columns)} columns")
        return self.df
    
    def calculate_summary_stats(self) -> Dict:
        """Calculate comprehensive summary statistics"""
        if self.df is None or self.df.empty:
            return {}
            
        stats = {}
        
        # Overall statistics
        total_ops = len(self.df)
        successful_ops = self.df['successful'].sum()
        stats['total_operations'] = total_ops
        stats['successful_operations'] = successful_ops
        stats['success_rate'] = successful_ops / total_ops if total_ops > 0 else 0
        stats['failed_operations'] = total_ops - successful_ops
        
        # Timing statistics
        if 'seconds_from_start' in self.df.columns:
            duration = self.df['seconds_from_start'].max()
            stats['total_duration_seconds'] = duration
            stats['overall_throughput_ops_per_sec'] = total_ops / duration if duration > 0 else 0
            
        # Latency statistics (successful operations only)
        successful_df = self.df[self.df['successful'] == True]
        if not successful_df.empty and 'latency_ms' in successful_df.columns:
            latencies = successful_df['latency_ms'].dropna()
            if not latencies.empty:
                stats['latency_stats'] = {
                    'min_ms': latencies.min(),
                    'max_ms': latencies.max(),
                    'mean_ms': latencies.mean(),
                    'median_ms': latencies.median(),
                    'p95_ms': latencies.quantile(0.95),
                    'p99_ms': latencies.quantile(0.99),
                    'std_ms': latencies.std()
                }
        
        # Per job type statistics
        for job_type in self.df['job_type'].unique():
            job_df = self.df[self.df['job_type'] == job_type]
            job_successful = job_df['successful'].sum()
            job_total = len(job_df)
            
            stats[f'{job_type}_statistics'] = {
                'total_operations': job_total,
                'successful_operations': job_successful,
                'success_rate': job_successful / job_total if job_total > 0 else 0,
                'failed_operations': job_total - job_successful
            }
            
            # Job-specific latency stats
            job_successful_df = job_df[job_df['successful'] == True]
            if not job_successful_df.empty and 'latency_ms' in job_successful_df.columns:
                job_latencies = job_successful_df['latency_ms'].dropna()
                if not job_latencies.empty:
                    stats[f'{job_type}_latency'] = {
                        'min_ms': job_latencies.min(),
                        'max_ms': job_latencies.max(),
                        'mean_ms': job_latencies.mean(),
                        'median_ms': job_latencies.median(),
                        'p95_ms': job_latencies.quantile(0.95),
                        'p99_ms': job_latencies.quantile(0.99)
                    }
        
        # Worker-level statistics
        worker_stats = []
        for worker_id in self.df['worker_id'].unique():
            worker_df = self.df[self.df['worker_id'] == worker_id]
            worker_successful = worker_df['successful'].sum()
            worker_total = len(worker_df)
            
            worker_stat = {
                'worker_id': worker_id,
                'total_operations': worker_total,
                'successful_operations': worker_successful,
                'success_rate': worker_successful / worker_total if worker_total > 0 else 0
            }
            
            # Worker latency stats
            worker_successful_df = worker_df[worker_df['successful'] == True]
            if not worker_successful_df.empty and 'latency_ms' in worker_successful_df.columns:
                worker_latencies = worker_successful_df['latency_ms'].dropna()
                if not worker_latencies.empty:
                    worker_stat['mean_latency_ms'] = worker_latencies.mean()
                    worker_stat['median_latency_ms'] = worker_latencies.median()
                    
            worker_stats.append(worker_stat)
            
        stats['worker_statistics'] = worker_stats
        
        # Configuration summary
        stats['configuration'] = self.config
        
        return stats
    
    def calculate_throughput_over_time(self, window_seconds: int = 10) -> pd.DataFrame:
        """Calculate throughput over time using sliding windows"""
        if self.df is None or self.df.empty or 'seconds_from_start' not in self.df.columns:
            return pd.DataFrame()
            
        # Create time windows
        max_time = self.df['seconds_from_start'].max()
        time_windows = np.arange(0, max_time + window_seconds, window_seconds)
        
        throughput_data = []
        for i in range(len(time_windows) - 1):
            window_start = time_windows[i]
            window_end = time_windows[i + 1]
            
            window_df = self.df[
                (self.df['seconds_from_start'] >= window_start) & 
                (self.df['seconds_from_start'] < window_end)
            ]
            
            if not window_df.empty:
                total_ops = len(window_df)
                successful_ops = window_df['successful'].sum()
                
                throughput_data.append({
                    'time_window_start': window_start,
                    'time_window_end': window_end,
                    'total_operations': total_ops,
                    'successful_operations': successful_ops,
                    'throughput_ops_per_sec': total_ops / window_seconds,
                    'successful_throughput_ops_per_sec': successful_ops / window_seconds,
                    'success_rate': successful_ops / total_ops if total_ops > 0 else 0
                })
        
        return pd.DataFrame(throughput_data)
    
    def create_visualizations(self, output_dir: str = "./plots") -> None:
        """Create comprehensive visualization plots"""
        if self.df is None or self.df.empty:
            print("No data available for visualization")
            return
            
        Path(output_dir).mkdir(exist_ok=True)
        
        # Set style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
        # 1. Latency Distribution
        self._plot_latency_distribution(output_dir)
        
        # 2. Throughput over time
        self._plot_throughput_over_time(output_dir)
        
        # 3. Success rates
        self._plot_success_rates(output_dir)
        
        # 4. Worker performance comparison
        self._plot_worker_performance(output_dir)
        
        # 5. Query template performance (if applicable)
        if 'template_name' in self.df.columns and self.df['template_name'].notna().any():
            self._plot_template_performance(output_dir)
            
    def _plot_latency_distribution(self, output_dir: str) -> None:
        """Plot latency distribution histograms"""
        successful_df = self.df[self.df['successful'] == True]
        if successful_df.empty or 'latency_ms' not in successful_df.columns:
            return
            
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Latency Distribution Analysis', fontsize=16)
        
        # Overall latency histogram
        latencies = successful_df['latency_ms'].dropna()
        axes[0, 0].hist(latencies, bins=50, alpha=0.7, edgecolor='black')
        axes[0, 0].set_title('Overall Latency Distribution')
        axes[0, 0].set_xlabel('Latency (ms)')
        axes[0, 0].set_ylabel('Frequency')
        
        # Box plot by job type
        if len(successful_df['job_type'].unique()) > 1:
            successful_df.boxplot(column='latency_ms', by='job_type', ax=axes[0, 1])
            axes[0, 1].set_title('Latency by Job Type')
            axes[0, 1].set_xlabel('Job Type')
            axes[0, 1].set_ylabel('Latency (ms)')
        
        # Latency over time (scatter plot)
        axes[1, 0].scatter(successful_df['seconds_from_start'], successful_df['latency_ms'], 
                          alpha=0.6, s=20)
        axes[1, 0].set_title('Latency Over Time')
        axes[1, 0].set_xlabel('Time (seconds from start)')
        axes[1, 0].set_ylabel('Latency (ms)')
        
        # Percentile plot
        percentiles = np.arange(1, 101)
        latency_percentiles = [np.percentile(latencies, p) for p in percentiles]
        axes[1, 1].plot(percentiles, latency_percentiles, linewidth=2)
        axes[1, 1].set_title('Latency Percentiles')
        axes[1, 1].set_xlabel('Percentile')
        axes[1, 1].set_ylabel('Latency (ms)')
        axes[1, 1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/latency_distribution.png", dpi=300, bbox_inches='tight')
        plt.close()
        
    def _plot_throughput_over_time(self, output_dir: str) -> None:
        """Plot throughput over time"""
        throughput_df = self.calculate_throughput_over_time(window_seconds=10)
        if throughput_df.empty:
            return
            
        fig, axes = plt.subplots(2, 1, figsize=(15, 10))
        fig.suptitle('Throughput Analysis Over Time', fontsize=16)
        
        # Throughput line plot
        axes[0].plot(throughput_df['time_window_start'], 
                    throughput_df['throughput_ops_per_sec'], 
                    linewidth=2, label='Total Ops/sec', marker='o')
        axes[0].plot(throughput_df['time_window_start'], 
                    throughput_df['successful_throughput_ops_per_sec'], 
                    linewidth=2, label='Successful Ops/sec', marker='s')
        axes[0].set_title('Throughput Over Time')
        axes[0].set_xlabel('Time (seconds)')
        axes[0].set_ylabel('Operations per Second')
        axes[0].legend()
        axes[0].grid(True, alpha=0.3)
        
        # Success rate over time
        axes[1].plot(throughput_df['time_window_start'], 
                    throughput_df['success_rate'] * 100, 
                    linewidth=2, color='green', marker='d')
        axes[1].set_title('Success Rate Over Time')
        axes[1].set_xlabel('Time (seconds)')
        axes[1].set_ylabel('Success Rate (%)')
        axes[1].set_ylim(0, 105)
        axes[1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/throughput_analysis.png", dpi=300, bbox_inches='tight')
        plt.close()
        
    def _plot_success_rates(self, output_dir: str) -> None:
        """Plot success rate analysis"""
        fig, axes = plt.subplots(1, 2, figsize=(15, 6))
        fig.suptitle('Success Rate Analysis', fontsize=16)
        
        # Success rate by job type
        job_success = self.df.groupby('job_type')['successful'].agg(['count', 'sum'])
        job_success['success_rate'] = job_success['sum'] / job_success['count'] * 100
        
        axes[0].bar(job_success.index, job_success['success_rate'], 
                   color=['skyblue', 'lightcoral'])
        axes[0].set_title('Success Rate by Job Type')
        axes[0].set_ylabel('Success Rate (%)')
        axes[0].set_ylim(0, 105)
        
        # Add value labels on bars
        for i, v in enumerate(job_success['success_rate']):
            axes[0].text(i, v + 1, f'{v:.1f}%', ha='center', va='bottom')
            
        # Success rate by worker
        worker_success = self.df.groupby('worker_id')['successful'].agg(['count', 'sum'])
        worker_success['success_rate'] = worker_success['sum'] / worker_success['count'] * 100
        
        axes[1].bar(range(len(worker_success)), worker_success['success_rate'].values,
                   color='lightgreen', alpha=0.7)
        axes[1].set_title('Success Rate by Worker')
        axes[1].set_xlabel('Worker ID')
        axes[1].set_ylabel('Success Rate (%)')
        axes[1].set_ylim(0, 105)
        axes[1].set_xticks(range(len(worker_success)))
        axes[1].set_xticklabels([f'W{i}' for i in worker_success.index], rotation=45)
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/success_rates.png", dpi=300, bbox_inches='tight')
        plt.close()
        
    def _plot_worker_performance(self, output_dir: str) -> None:
        """Plot worker performance comparison"""
        successful_df = self.df[self.df['successful'] == True]
        if successful_df.empty:
            return
            
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Worker Performance Analysis', fontsize=16)
        
        # Operations per worker
        worker_ops = self.df.groupby('worker_id').size()
        axes[0, 0].bar(range(len(worker_ops)), worker_ops.values, color='lightblue', alpha=0.7)
        axes[0, 0].set_title('Total Operations per Worker')
        axes[0, 0].set_xlabel('Worker ID')
        axes[0, 0].set_ylabel('Number of Operations')
        axes[0, 0].set_xticks(range(len(worker_ops)))
        axes[0, 0].set_xticklabels([f'W{i}' for i in worker_ops.index])
        
        # Average latency per worker
        if 'latency_ms' in successful_df.columns:
            worker_latency = successful_df.groupby('worker_id')['latency_ms'].mean()
            axes[0, 1].bar(range(len(worker_latency)), worker_latency.values, 
                          color='orange', alpha=0.7)
            axes[0, 1].set_title('Average Latency per Worker')
            axes[0, 1].set_xlabel('Worker ID')
            axes[0, 1].set_ylabel('Average Latency (ms)')
            axes[0, 1].set_xticks(range(len(worker_latency)))
            axes[0, 1].set_xticklabels([f'W{i}' for i in worker_latency.index])
        
        # Worker timeline (when each worker was active)
        for worker_id in self.df['worker_id'].unique():
            worker_df = self.df[self.df['worker_id'] == worker_id]
            axes[1, 0].scatter(worker_df['seconds_from_start'], 
                             [worker_id] * len(worker_df), 
                             alpha=0.6, s=20, label=f'Worker {worker_id}')
        axes[1, 0].set_title('Worker Activity Timeline')
        axes[1, 0].set_xlabel('Time (seconds from start)')
        axes[1, 0].set_ylabel('Worker ID')
        
        # Latency variance per worker
        if 'latency_ms' in successful_df.columns:
            worker_latency_std = successful_df.groupby('worker_id')['latency_ms'].std()
            axes[1, 1].bar(range(len(worker_latency_std)), worker_latency_std.values, 
                          color='red', alpha=0.7)
            axes[1, 1].set_title('Latency Standard Deviation per Worker')
            axes[1, 1].set_xlabel('Worker ID')
            axes[1, 1].set_ylabel('Latency Std Dev (ms)')
            axes[1, 1].set_xticks(range(len(worker_latency_std)))
            axes[1, 1].set_xticklabels([f'W{i}' for i in worker_latency_std.index])
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/worker_performance.png", dpi=300, bbox_inches='tight')
        plt.close()
        
    def _plot_template_performance(self, output_dir: str) -> None:
        """Plot query template performance analysis"""
        query_df = self.df[
            (self.df['job_type'] == 'query') & 
            (self.df['successful'] == True) & 
            (self.df['template_name'].notna())
        ]
        
        if query_df.empty:
            return
            
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Query Template Performance Analysis', fontsize=16)
        
        # Average latency by template
        template_latency = query_df.groupby('template_name')['latency_ms'].mean().sort_values()
        axes[0, 0].barh(range(len(template_latency)), template_latency.values, color='skyblue')
        axes[0, 0].set_title('Average Latency by Template')
        axes[0, 0].set_xlabel('Average Latency (ms)')
        axes[0, 0].set_yticks(range(len(template_latency)))
        axes[0, 0].set_yticklabels(template_latency.index, fontsize=8)
        
        # Query count by template
        template_counts = query_df.groupby('template_name').size().sort_values()
        axes[0, 1].barh(range(len(template_counts)), template_counts.values, color='lightgreen')
        axes[0, 1].set_title('Query Count by Template')
        axes[0, 1].set_xlabel('Number of Queries')
        axes[0, 1].set_yticks(range(len(template_counts)))
        axes[0, 1].set_yticklabels(template_counts.index, fontsize=8)
        
        # Box plot of latencies by template
        if len(template_latency) <= 10:  # Only if we don't have too many templates
            query_df.boxplot(column='latency_ms', by='template_name', ax=axes[1, 0])
            axes[1, 0].set_title('Latency Distribution by Template')
            axes[1, 0].set_xlabel('Template')
            axes[1, 0].set_ylabel('Latency (ms)')
            plt.setp(axes[1, 0].xaxis.get_majorticklabels(), rotation=45, fontsize=8)
        
        # Template success rates
        template_success = query_df.groupby('template_name')['successful'].agg(['count', 'sum'])
        template_success['success_rate'] = template_success['sum'] / template_success['count'] * 100
        template_success = template_success.sort_values('success_rate')
        
        axes[1, 1].barh(range(len(template_success)), template_success['success_rate'].values, 
                       color='orange')
        axes[1, 1].set_title('Success Rate by Template')
        axes[1, 1].set_xlabel('Success Rate (%)')
        axes[1, 1].set_yticks(range(len(template_success)))
        axes[1, 1].set_yticklabels(template_success.index, fontsize=8)
        axes[1, 1].set_xlim(0, 105)
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/template_performance.png", dpi=300, bbox_inches='tight')
        plt.close()
    
    def export_data(self, output_dir: str = "./output", formats: List[str] = ['csv', 'json']) -> None:
        """Export processed data and analysis results"""
        if self.df is None or self.df.empty:
            print("No data available for export")
            return
            
        Path(output_dir).mkdir(exist_ok=True)
        
        # Export main DataFrame
        if 'csv' in formats:
            self.df.to_csv(f"{output_dir}/benchmark_data.csv", index=False)
            print(f"Exported DataFrame to {output_dir}/benchmark_data.csv")
            
        if 'parquet' in formats:
            self.df.to_parquet(f"{output_dir}/benchmark_data.parquet", index=False)
            print(f"Exported DataFrame to {output_dir}/benchmark_data.parquet")
            
        # Export summary statistics
        stats = self.calculate_summary_stats()
        if 'json' in formats:
            with open(f"{output_dir}/summary_stats.json", 'w') as f:
                json.dump(stats, f, indent=2, default=str)
            print(f"Exported summary statistics to {output_dir}/summary_stats.json")
            
        # Export throughput analysis
        throughput_df = self.calculate_throughput_over_time()
        if not throughput_df.empty and 'csv' in formats:
            throughput_df.to_csv(f"{output_dir}/throughput_analysis.csv", index=False)
            print(f"Exported throughput analysis to {output_dir}/throughput_analysis.csv")
            
        # Create human-readable report
        self._create_text_report(output_dir, stats)
        
    def _create_text_report(self, output_dir: str, stats: Dict) -> None:
        """Create a human-readable text report"""
        report_path = f"{output_dir}/benchmark_report.txt"
        
        with open(report_path, 'w') as f:
            f.write("="*80 + "\n")
            f.write("BENCHMARK ANALYSIS REPORT\n")
            f.write("="*80 + "\n\n")
            
            # Configuration section
            f.write("CONFIGURATION\n")
            f.write("-"*40 + "\n")
            config = stats.get('configuration', {})
            for key, value in config.items():
                f.write(f"{key:25}: {value}\n")
            f.write("\n")
            
            # Overall performance section
            f.write("OVERALL PERFORMANCE\n")
            f.write("-"*40 + "\n")
            f.write(f"{'Total Operations':<25}: {stats.get('total_operations', 'N/A')}\n")
            f.write(f"{'Successful Operations':<25}: {stats.get('successful_operations', 'N/A')}\n")
            f.write(f"{'Success Rate':<25}: {stats.get('success_rate', 0)*100:.2f}%\n")
            f.write(f"{'Failed Operations':<25}: {stats.get('failed_operations', 'N/A')}\n")
            f.write(f"{'Total Duration (sec)':<25}: {stats.get('total_duration_seconds', 'N/A'):.2f}\n")
            f.write(f"{'Overall Throughput':<25}: {stats.get('overall_throughput_ops_per_sec', 'N/A'):.2f} ops/sec\n")
            f.write("\n")
            
            # Latency statistics
            latency_stats = stats.get('latency_stats', {})
            if latency_stats:
                f.write("LATENCY STATISTICS (Successful Operations)\n")
                f.write("-"*40 + "\n")
                f.write(f"{'Minimum':<25}: {latency_stats.get('min_ms', 'N/A'):.2f} ms\n")
                f.write(f"{'Maximum':<25}: {latency_stats.get('max_ms', 'N/A'):.2f} ms\n")
                f.write(f"{'Mean':<25}: {latency_stats.get('mean_ms', 'N/A'):.2f} ms\n")
                f.write(f"{'Median':<25}: {latency_stats.get('median_ms', 'N/A'):.2f} ms\n")
                f.write(f"{'95th Percentile':<25}: {latency_stats.get('p95_ms', 'N/A'):.2f} ms\n")
                f.write(f"{'99th Percentile':<25}: {latency_stats.get('p99_ms', 'N/A'):.2f} ms\n")
                f.write(f"{'Standard Deviation':<25}: {latency_stats.get('std_ms', 'N/A'):.2f} ms\n")
                f.write("\n")
            
            # Per job type statistics
            for job_type in ['insert', 'query']:
                job_stats = stats.get(f'{job_type}_statistics')
                if job_stats:
                    f.write(f"{job_type.upper()} PERFORMANCE\n")
                    f.write("-"*40 + "\n")
                    f.write(f"{'Total Operations':<25}: {job_stats.get('total_operations', 'N/A')}\n")
                    f.write(f"{'Successful Operations':<25}: {job_stats.get('successful_operations', 'N/A')}\n")
                    f.write(f"{'Success Rate':<25}: {job_stats.get('success_rate', 0)*100:.2f}%\n")
                    f.write(f"{'Failed Operations':<25}: {job_stats.get('failed_operations', 'N/A')}\n")
                    
                    job_latency = stats.get(f'{job_type}_latency')
                    if job_latency:
                        f.write(f"{'Mean Latency':<25}: {job_latency.get('mean_ms', 'N/A'):.2f} ms\n")
                        f.write(f"{'Median Latency':<25}: {job_latency.get('median_ms', 'N/A'):.2f} ms\n")
                        f.write(f"{'95th Percentile':<25}: {job_latency.get('p95_ms', 'N/A'):.2f} ms\n")
                        f.write(f"{'99th Percentile':<25}: {job_latency.get('p99_ms', 'N/A'):.2f} ms\n")
                    f.write("\n")
            
            # Worker statistics
            worker_stats = stats.get('worker_statistics', [])
            if worker_stats:
                f.write("WORKER PERFORMANCE\n")
                f.write("-"*40 + "\n")
                f.write(f"{'Worker ID':<10} {'Total Ops':<10} {'Success':<10} {'Rate %':<8} {'Mean Lat':<10}\n")
                f.write("-"*48 + "\n")
                for worker in worker_stats:
                    worker_id = worker.get('worker_id', 'N/A')
                    total_ops = worker.get('total_operations', 'N/A')
                    successful = worker.get('successful_operations', 'N/A')
                    rate = worker.get('success_rate', 0) * 100
                    mean_lat = worker.get('mean_latency_ms', 'N/A')
                    mean_lat_str = f"{mean_lat:.2f}" if isinstance(mean_lat, (int, float)) else str(mean_lat)
                    f.write(f"{worker_id:<10} {total_ops:<10} {successful:<10} {rate:<8.1f} {mean_lat_str:<10}\n")
            
        print(f"Created detailed report: {report_path}")


def main():
    """Main function for command-line usage"""
    parser = argparse.ArgumentParser(description='Analyze benchmark log files from load-generator')
    parser.add_argument('log_file', help='Path to the JSON log file')
    parser.add_argument('--output-dir', default='./analysis_output', 
                       help='Output directory for analysis results (default: ./analysis_output)')
    parser.add_argument('--no-plots', action='store_true', 
                       help='Skip generating visualization plots')
    parser.add_argument('--export-formats', nargs='+', default=['csv', 'json'],
                       choices=['csv', 'json', 'parquet'],
                       help='Export formats for data (default: csv json)')
    parser.add_argument('--window-seconds', type=int, default=10,
                       help='Time window for throughput analysis in seconds (default: 10)')
    
    args = parser.parse_args()
    
    # Validate input file
    if not Path(args.log_file).exists():
        print(f"Error: Log file '{args.log_file}' not found")
        return 1
        
    print(f"Analyzing log file: {args.log_file}")
    print(f"Output directory: {args.output_dir}")
    print("-" * 50)
    
    try:
        # Initialize analyzer
        analyzer = BenchmarkLogAnalyzer(args.log_file)
        
        # Parse logs
        analyzer.parse_logs()
        
        # Create DataFrame
        df = analyzer.create_dataframe()
        
        if df.empty:
            print("Warning: No performance events found in log file")
            return 1
            
        # Calculate and display summary statistics
        stats = analyzer.calculate_summary_stats()
        print("\nSUMMARY STATISTICS:")
        print(f"Total operations: {stats.get('total_operations', 'N/A')}")
        print(f"Success rate: {stats.get('success_rate', 0)*100:.2f}%")
        print(f"Overall throughput: {stats.get('overall_throughput_ops_per_sec', 'N/A'):.2f} ops/sec")
        
        latency_stats = stats.get('latency_stats', {})
        if latency_stats:
            print(f"Mean latency: {latency_stats.get('mean_ms', 'N/A'):.2f} ms")
            print(f"95th percentile latency: {latency_stats.get('p95_ms', 'N/A'):.2f} ms")
        
        # Generate visualizations
        if not args.no_plots:
            print(f"\nGenerating visualizations...")
            analyzer.create_visualizations(f"{args.output_dir}/plots")
            print("Visualizations saved to plots/ directory")
        
        # Export data
        print(f"\nExporting data...")
        analyzer.export_data(args.output_dir, args.export_formats)
        
        print(f"\nAnalysis complete! Results saved to: {args.output_dir}")
        return 0
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())