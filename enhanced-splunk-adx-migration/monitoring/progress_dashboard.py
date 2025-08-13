"""
Real-time Migration Progress Dashboard
Provides monitoring, metrics, and progress tracking for the Splunk to ADX migration
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import yaml
import json
import os
from pathlib import Path
import logging
import subprocess
import psutil
from typing import Dict, List, Optional

class MigrationDashboard:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.checkpoint_dir = Path(self.config['checkpoint']['directory'])
        self.log_dir = Path(self.config['logging']['log_directory'])
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def get_migration_stats(self) -> Dict:
        """Get current migration statistics."""
        stats = {
            'total_partitions': 0,
            'completed_partitions': 0,
            'failed_partitions': 0,
            'progress_percentage': 0.0,
            'estimated_completion_time': None,
            'data_transferred_gb': 0,
            'average_throughput_mb_per_sec': 0
        }
        
        # Read completed partitions
        completed_file = self.checkpoint_dir / "completed_partitions.txt"
        if completed_file.exists():
            with open(completed_file, 'r') as f:
                completed_partitions = set(line.strip() for line in f)
                stats['completed_partitions'] = len(completed_partitions)
        
        # Calculate total partitions based on config
        migration_config = self.config['migration']
        start_date = datetime.strptime(migration_config['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(migration_config['end_date'], '%Y-%m-%d')
        partition_days = migration_config['partition_size_days']
        days_total = (end_date - start_date).days
        partitions_per_index = days_total // partition_days + (1 if days_total % partition_days else 0)
        stats['total_partitions'] = partitions_per_index * len(migration_config['indexes'])
        
        # Calculate progress percentage
        if stats['total_partitions'] > 0:
            stats['progress_percentage'] = (stats['completed_partitions'] / stats['total_partitions']) * 100
        
        return stats
    
    def get_recent_logs(self, num_lines: int = 50) -> List[str]:
        """Get recent log entries."""
        logs = []
        if self.log_dir.exists():
            log_files = sorted(self.log_dir.glob("migration_*.log"), key=os.path.getmtime, reverse=True)
            if log_files:
                try:
                    with open(log_files[0], 'r') as f:
                        logs = f.readlines()[-num_lines:]
                except Exception:
                    logs = ["Unable to read log file"]
        else:
            logs = ["Log directory not found"]
        
        return logs
    
    def get_performance_metrics(self) -> Dict:
        """Get system performance metrics."""
        metrics = {
            'cpu_usage_percent': psutil.cpu_percent(interval=1),
            'memory_usage_percent': psutil.virtual_memory().percent,
            'disk_usage_percent': psutil.disk_usage('/').percent,
            'network_io': psutil.net_io_counters(),
            'active_connections': len(psutil.net_connections())
        }
        return metrics
    
    def get_error_summary(self) -> Dict:
        """Get error summary from logs."""
        error_counts = {'ERROR': 0, 'WARNING': 0, 'CRITICAL': 0}
        
        if self.log_dir.exists():
            log_files = list(self.log_dir.glob("migration_*.log"))
            for log_file in log_files:
                try:
                    with open(log_file, 'r') as f:
                        content = f.read()
                        for level in error_counts:
                            error_counts[level] += content.count(f'[{level}]')
                except Exception:
                    continue
        
        return error_counts
    
    def estimate_completion_time(self, stats: Dict) -> Optional[datetime]:
        """Estimate migration completion time."""
        if stats['completed_partitions'] == 0:
            return None
        
        # Simple estimation based on current progress
        remaining_partitions = stats['total_partitions'] - stats['completed_partitions']
        if remaining_partitions <= 0:
            return datetime.now()
        
        # Assume average time per partition (this could be improved with actual timing data)
        avg_time_per_partition_minutes = 10  # Placeholder - should be calculated from actual data
        estimated_remaining_minutes = remaining_partitions * avg_time_per_partition_minutes
        
        return datetime.now() + timedelta(minutes=estimated_remaining_minutes)

def main():
    st.set_page_config(
        page_title="Splunk to ADX Migration Dashboard",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("🚀 Enhanced Splunk to ADX Migration Dashboard")
    st.markdown("Real-time monitoring for 250TB data migration")
    
    # Configuration
    config_path = st.sidebar.text_input(
        "Configuration File Path", 
        value="config/migration-config.yaml"
    )
    
    if not os.path.exists(config_path):
        st.error(f"Configuration file not found: {config_path}")
        return
    
    # Initialize dashboard
    dashboard = MigrationDashboard(config_path)
    
    # Auto-refresh
    auto_refresh = st.sidebar.checkbox("Auto Refresh", value=True)
    if auto_refresh:
        refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 10)
        time.sleep(refresh_interval)
        st.rerun()
    
    # Manual refresh button
    if st.sidebar.button("🔄 Refresh Now"):
        st.rerun()
    
    # Get current statistics
    stats = dashboard.get_migration_stats()
    performance = dashboard.get_performance_metrics()
    errors = dashboard.get_error_summary()
    
    # Main metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Progress",
            f"{stats['progress_percentage']:.1f}%",
            f"{stats['completed_partitions']}/{stats['total_partitions']} partitions"
        )
    
    with col2:
        st.metric(
            "Completed Partitions",
            stats['completed_partitions'],
            f"Remaining: {stats['total_partitions'] - stats['completed_partitions']}"
        )
    
    with col3:
        st.metric(
            "System CPU",
            f"{performance['cpu_usage_percent']:.1f}%"
        )
    
    with col4:
        st.metric(
            "Memory Usage",
            f"{performance['memory_usage_percent']:.1f}%"
        )
    
    # Progress visualization
    st.subheader("📈 Migration Progress")
    
    # Progress bar
    progress_bar = st.progress(stats['progress_percentage'] / 100)
    
    # Progress chart
    if stats['total_partitions'] > 0:
        progress_data = pd.DataFrame({
            'Status': ['Completed', 'Remaining', 'Failed'],
            'Count': [
                stats['completed_partitions'],
                stats['total_partitions'] - stats['completed_partitions'] - stats.get('failed_partitions', 0),
                stats.get('failed_partitions', 0)
            ]
        })
        
        fig_progress = px.pie(
            progress_data, 
            values='Count', 
            names='Status',
            title='Partition Status Distribution',
            color_discrete_map={
                'Completed': '#00cc00',
                'Remaining': '#cccccc',
                'Failed': '#ff0000'
            }
        )
        st.plotly_chart(fig_progress, use_container_width=True)
    
    # Time estimation
    estimated_completion = dashboard.estimate_completion_time(stats)
    if estimated_completion:
        st.info(f"⏱️ Estimated completion time: {estimated_completion.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # System Performance
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🖥️ System Performance")
        
        # Performance metrics
        perf_data = pd.DataFrame({
            'Metric': ['CPU', 'Memory', 'Disk'],
            'Usage %': [
                performance['cpu_usage_percent'],
                performance['memory_usage_percent'],
                performance['disk_usage_percent']
            ]
        })
        
        fig_perf = px.bar(
            perf_data,
            x='Metric',
            y='Usage %',
            title='System Resource Usage',
            color='Usage %',
            color_continuous_scale='RdYlGn_r'
        )
        fig_perf.update_layout(showlegend=False)
        st.plotly_chart(fig_perf, use_container_width=True)
    
    with col2:
        st.subheader("⚠️ Error Summary")
        
        if any(errors.values()):
            error_df = pd.DataFrame(list(errors.items()), columns=['Level', 'Count'])
            fig_errors = px.bar(
                error_df,
                x='Level',
                y='Count',
                title='Error Count by Level',
                color='Level',
                color_discrete_map={
                    'WARNING': '#ffaa00',
                    'ERROR': '#ff6600',
                    'CRITICAL': '#ff0000'
                }
            )
            st.plotly_chart(fig_errors, use_container_width=True)
        else:
            st.success("✅ No errors reported")
    
    # Recent logs
    st.subheader("📝 Recent Logs")
    logs = dashboard.get_recent_logs(20)
    
    log_container = st.container()
    with log_container:
        if logs:
            log_text = "\n".join(logs[-10:])  # Show last 10 lines
            st.code(log_text, language=None)
        else:
            st.info("No logs available")
    
    # Configuration display
    with st.expander("⚙️ Migration Configuration"):
        st.yaml(dashboard.config)
    
    # Control panel
    st.subheader("🎛️ Control Panel")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("▶️ Start Migration", type="primary"):
            st.info("Starting migration... (This would execute the migration script)")
            # In a real implementation, you would start the migration process here
    
    with col2:
        if st.button("⏸️ Pause Migration"):
            st.warning("Migration paused (feature to be implemented)")
    
    with col3:
        if st.button("🛑 Stop Migration"):
            st.error("Migration stopped (feature to be implemented)")
    
    # Export options
    st.subheader("📤 Export Options")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📊 Export Progress Report"):
            report_data = {
                'timestamp': datetime.now().isoformat(),
                'statistics': stats,
                'performance': performance,
                'errors': errors
            }
            st.download_button(
                "Download JSON Report",
                data=json.dumps(report_data, indent=2),
                file_name=f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
    
    with col2:
        if st.button("📋 Export Configuration"):
            st.download_button(
                "Download Configuration",
                data=yaml.dump(dashboard.config, indent=2),
                file_name="migration-config.yaml",
                mime="text/yaml"
            )

if __name__ == "__main__":
    main()
