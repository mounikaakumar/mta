#!/usr/bin/env python3
"""
Optimized Database helper functions for MTA Dashboard
Reduced from 38 columns to 18 columns (53% reduction)
"""

import sqlite3
import logging

# Configure logging
logger = logging.getLogger(__name__)

def create_optimized_tables(db_path):
    """Create optimized SQLite tables with reduced columns."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Optimized test data table (18 columns instead of 38)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS terra_test_data_optimized (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            -- Core data (5 columns)
            exe_date DATE,
            build_version TEXT,
            test_case TEXT,
            automation_lob TEXT,
            method_type TEXT,
            
            -- Platform (2 columns) - merged platform fields
            app_platform TEXT,
            os_version TEXT,
            
            -- Results (5 columns)
            result TEXT,
            no_of_runs INTEGER,
            no_of_passed_runs INTEGER,
            no_of_infra_runs INTEGER,
            dl_failures INTEGER,
            
            -- Bug tracking (5 columns)
            bug_id TEXT,
            issue_id TEXT,
            issue_key TEXT,
            status TEXT,
            priority TEXT,
            bug_creation_date DATE,
            bug_resolution_date DATE,
            project_name TEXT,
            project_category_name TEXT,
    
            
            -- Analysis (5 columns) - Added detailed breakdown fields
            bug_type TEXT,
            pb_bug_type TEXT,
            pfc_bug_type TEXT,
            occurence TEXT,
            no_of_masked_infra_runs INTEGER,
            l1_category TEXT,
            
            
            -- Metadata (1 column)
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Optimized feature data table (22 columns instead of 18)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS terra_feature_data_optimized (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            -- Core data (5 columns)
            exe_date DATE,
            build_version TEXT,
            scenarioname TEXT,
            automation_lob TEXT,
            method_type TEXT,
            
            -- Platform (2 columns)
            app_platform TEXT,
            os_version TEXT,
            
            -- Results (5 columns)
            result TEXT,
            no_of_runs INTEGER,
            no_of_passed_runs INTEGER,
            no_of_infra_runs INTEGER,
            dl_failures INTEGER,
            
            -- Bug tracking (5 columns)
            bug_id TEXT,
            issue_id TEXT,
            issue_key TEXT,
            status TEXT,
            priority TEXT,
            bug_creation_date DATE,
            bug_resolution_date DATE,
            project_name TEXT,
            project_category_name TEXT,
          
            
            -- Analysis (5 columns) - Added detailed breakdown fields
            bug_type TEXT,
            pb_bug_type TEXT,
            pfc_bug_type TEXT,
            occurence TEXT,
            no_of_masked_infra_runs INTEGER,
            l1_category TEXT,
            
            -- Metadata (1 column)
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Optimized indexes (focus on essential queries only)
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_test_exe_date ON terra_test_data_optimized(exe_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_test_platform ON terra_test_data_optimized(app_platform)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_test_lob ON terra_test_data_optimized(automation_lob)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_test_bug_type ON terra_test_data_optimized(bug_type)')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_feature_exe_date ON terra_feature_data_optimized(exe_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_feature_platform ON terra_feature_data_optimized(app_platform)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_feature_lob ON terra_feature_data_optimized(automation_lob)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_feature_bug_type ON terra_feature_data_optimized(bug_type)')
    
    # Performance indexes for aggregation queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_test_runs_perf ON terra_test_data_optimized(no_of_runs, no_of_passed_runs, automation_lob)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_feature_runs_perf ON terra_feature_data_optimized(no_of_runs, no_of_passed_runs, scenarioname)')
    
    # Keep existing support tables as-is
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mta_refresh_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            refresh_type TEXT,
            refresh_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            rows_processed INTEGER,
            status TEXT,
            details TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dropdown_options (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filter_type TEXT NOT NULL,
            value TEXT NOT NULL,
            count INTEGER DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(filter_type, value)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS feature_pass_rate_breakdown (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            feature_name TEXT NOT NULL,
            bugs INTEGER DEFAULT 0,
            total_runs INTEGER DEFAULT 0,
            passed_runs INTEGER DEFAULT 0,
            pass_rate REAL DEFAULT 0.0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(feature_name)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS test_method_pass_rate_breakdown (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            test_method TEXT NOT NULL,
            bugs INTEGER DEFAULT 0,
            total_runs INTEGER DEFAULT 0,
            passed_runs INTEGER DEFAULT 0,
            pass_rate REAL DEFAULT 0.0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(test_method)
        )
    ''')
    
    # Create sync status table for real-time progress tracking
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mta_sync_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sync_type TEXT NOT NULL,
            status TEXT NOT NULL,  -- in_progress, completed, failed
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            progress_percentage INTEGER DEFAULT 0,
            rows_processed INTEGER DEFAULT 0,
            total_rows INTEGER DEFAULT 0,
            message TEXT,
            error_details TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(sync_type)
        )
    ''')

    conn.commit()
    conn.close()
    logger.info("Optimized database setup completed successfully")
    return True

def get_optimized_column_orders():
    """
    Get optimized column orders for database tables (23 columns instead of 18).
    
    Returns:
        dict: Optimized column orders for each table
    """
    return {
        'terra_test_data_optimized': [
            # Core data (5)
            'exe_date', 'build_version', 'test_case', 'automation_lob', 'method_type',
            # Platform (2)
            'app_platform', 'os_version',
            # Results (5)
            'result', 'no_of_runs', 'no_of_passed_runs', 'no_of_infra_runs', 'dl_failures',
            # Bug tracking (5)
            'bug_id', 'issue_id', 'issue_key', 'status', 'priority', 'bug_creation_date','bug_resolution_date','project_name','project_category_name',
            # Analysis (5) - Added detailed breakdown fields
            'bug_type', 'pb_bug_type', 'pfc_bug_type', 'occurence', 'no_of_masked_infra_runs','l1_category'
        ],
        'terra_feature_data_optimized': [
            # Core data (5)
            'exe_date', 'build_version', 'scenarioname', 'automation_lob', 'method_type',
            # Platform (2)
            'app_platform', 'os_version',
            # Results (5)
            'result', 'no_of_runs', 'no_of_passed_runs', 'no_of_infra_runs', 'dl_failures',
            # Bug tracking (5)
            'bug_id', 'issue_id', 'issue_key', 'status', 'priority', 'bug_creation_date','bug_resolution_date','project_name','project_category_name',
            # Analysis (5) - Added detailed breakdown fields
            'bug_type', 'pb_bug_type', 'pfc_bug_type', 'occurence', 'no_of_masked_infra_runs','l1_category'
        ]
    }

def extract_optimized_row_values(row, column_order):
    """
    Extract values from a row in the correct column order for optimized tables.
    
    Args:
        row: Database row (dict, tuple, or list)
        column_order: List of column names in correct order
        
    Returns:
        tuple: Values in the correct order for database insertion
    """
    if isinstance(row, dict):
        return tuple(row.get(col) for col in column_order)
    elif hasattr(row, '_asdict'):
        # Handle namedtuple
        row_dict = row._asdict()
        return tuple(row_dict.get(col) for col in column_order)
    else:
        # Handle tuple/list data (fallback)
        return tuple(row[:len(column_order)])

# Calculated fields (move from database to application layer)
def calculate_age(bug_creation_date):
    """Calculate bug age in days."""
    from datetime import datetime, date
    if not bug_creation_date:
        return None
    
    if isinstance(bug_creation_date, str):
        try:
            creation_date = datetime.strptime(bug_creation_date, '%Y-%m-%d').date()
        except ValueError:
            return None
    elif isinstance(bug_creation_date, datetime):
        creation_date = bug_creation_date.date()
    elif isinstance(bug_creation_date, date):
        creation_date = bug_creation_date
    else:
        return None
    
    return (date.today() - creation_date).days

def calculate_pass_rate(passed_runs, total_runs):
    """Calculate pass rate percentage."""
    if not total_runs or total_runs == 0:
        return 0.0
    return round((passed_runs / total_runs) * 100, 2)

def calculate_rank(data, partition_cols, order_col, ascending=False):
    """Calculate dense rank for data partitioned by columns."""
    # This would be implemented in the application layer instead of SQL
    # For now, return 1 as placeholder
    return 1
