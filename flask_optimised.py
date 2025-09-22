import sqlite3
from queryrunner_client import Client
from flask import Flask, render_template, request, jsonify
import os
from flask_apscheduler import APScheduler
from flask_compress import Compress
from datetime import datetime, timedelta
import logging
from query import query_test_optimized, query_feature_optimized
from helper_new import create_optimized_tables, get_optimized_column_orders, extract_optimized_row_values, calculate_age, calculate_pass_rate
import threading
from functools import lru_cache
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)
Compress(app)

# Configure scheduler
scheduler = APScheduler()
scheduler.api_enabled = True
scheduler.init_app(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Optimized Database path (new database for optimized version)
DB_PATH_OPTIMIZED = '/home/mkumar215/mkumar215_nfs/MTA_Recent/DashboardMTA/optimized_test_recent.db'

# Performance optimizations
_db_lock = threading.Lock()

# Thread pool for async operations
executor = ThreadPoolExecutor(max_workers=8)  # Increased for concurrent dashboard loading

# Cache removed as per user request - always fetch fresh data

def get_db_connection(use_row_factory=True, timeout=30.0):
    """Get optimized database connection with better error handling and optional row factory."""
    max_retries = 3
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(DB_PATH_OPTIMIZED, timeout=timeout)
            
            # Performance optimizations
            conn.execute("PRAGMA journal_mode = WAL")  # Write-Ahead Logging for better concurrency
            conn.execute("PRAGMA synchronous = NORMAL")  # Faster writes
            conn.execute("PRAGMA cache_size = 10000")  # 10MB cache
            conn.execute("PRAGMA temp_store = MEMORY")  # Store temp tables in memory
            conn.execute("PRAGMA mmap_size = 268435456")  # 256MB memory-mapped I/O
            conn.execute("PRAGMA busy_timeout = 30000")  # 30 second busy timeout
            
            # Optional row factory (disable for faster raw queries)
            if use_row_factory:
                conn.row_factory = sqlite3.Row  # Enable column access by name
            
            return conn
            
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                logger.warning(f"Database locked on attempt {attempt + 1}, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                continue
            else:
                logger.error(f"Optimized database connection error after {attempt + 1} attempts: {str(e)}")
                return None
        except Exception as e:
            logger.error(f"Optimized database connection error: {str(e)}")
            return None
    
    return None

def execute_presto_query(query_str):
    """Execute Presto query and return results."""
    try:
        client = Client(user_email='mkumar215@ext.uber.com')
        result = client.execute('presto', query_str)
        return result.fetchall()
    except Exception as e:
        logger.error(f"Error executing Presto query: {str(e)}")
        return None

def sync_test_data_optimized():
    """Sync test data from Presto to optimized SQLite with reduced columns."""
    sync_start_time = time.time()
    sync_type = "test_data_optimized"
    
    logger.info("🚀 Starting optimized test data sync...")
    logger.info(f"📊 Database: {DB_PATH_OPTIMIZED}")
    logger.info(f"📝 Query: Optimized (18 columns)")
    
    # Set sync status to in_progress (ONLY UPDATE AT START)
    update_sync_status(sync_type, 'in_progress', 0, 0, 0, "Starting test data sync from Presto...")
    
    try:
        # Execute optimized Presto query (18 columns instead of 38)
        logger.info("🔍 Executing Presto query for test data...")
        
        query_start_time = time.time()
        result = execute_presto_query(query_test_optimized)
        query_duration = time.time() - query_start_time
        
        if result is None:
            logger.error(f"❌ Failed to execute optimized test data query after {query_duration:.2f}s")
            update_sync_status(sync_type, 'failed', 0, 0, 0, "Failed to execute Presto query", f"Query failed after {query_duration:.2f}s")
            return False
        
        logger.info(f"✅ Presto query completed in {query_duration:.2f}s")
        logger.info(f"📈 Retrieved {len(result):,} rows from Presto")
        
        conn = get_db_connection()
        if not conn:
            logger.error("Failed to get optimized database connection")
            update_sync_status(sync_type, 'failed', 0, 0, 0, "Failed to get database connection", "Database connection error")
            return False
        
        cursor = conn.cursor()
        logger.info("🔗 Connected to database, starting data insertion...")
        
        # 🚀 OPTIMIZED BULK INSERT with smaller transactions to reduce lock time
        cursor.execute("PRAGMA synchronous = OFF")  # Disable sync for speed
        cursor.execute("PRAGMA journal_mode = WAL")  # Keep WAL mode for concurrency
        
        # TRUNCATE table for clean data load (quick operation)
        cursor.execute("DELETE FROM terra_test_data_optimized")
        conn.commit()  # Commit truncate immediately to release lock
        logger.info("🗑️ Truncated terra_test_data_optimized table")
        
        # TESTING: Allow duplicates to match Looker Studio counts
        insert_sql = '''
            INSERT INTO terra_test_data_optimized (
                exe_date, build_version, test_case, automation_lob, method_type,
                app_platform, os_version, result, no_of_runs, no_of_passed_runs, no_of_infra_runs, dl_failures,
                bug_id, issue_id, issue_key, status, priority, bug_creation_date,bug_resolution_date, project_name,project_category_name,
                bug_type,pb_bug_type, pfc_bug_type, occurence, no_of_masked_infra_runs,l1_category
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        rows_inserted = 0
        batch_size = 5000  # Smaller batch size to reduce lock time
        batch_data = []
        total_rows = len(result)
        
        # Get optimized column order
        column_order = get_optimized_column_orders()['terra_test_data_optimized']
        print("column order is....",column_order)
        
        for i, row in enumerate(result):
            try:

                # row['age'] = calculate_age(row.get('bug_creation_date'))
                # Extract values in optimized order (23 columns)
                row_values = extract_optimized_row_values(row, column_order)
                batch_data.append(row_values)
                # print("len id........",len(batch_data))
                
                if len(batch_data) >= batch_size:
                    # Use smaller transactions to reduce lock time
                    cursor.execute("BEGIN IMMEDIATE")  # Start transaction
                    cursor.executemany(insert_sql, batch_data)
                    cursor.execute("COMMIT")  # Commit immediately
                    
                    rows_inserted += len(batch_data)
                    logger.info(f"📊 Inserted {rows_inserted:,} test data rows...")  # NO STATUS UPDATE - just log
                    batch_data = []
                    
            except Exception as e:
                logger.error(f"Error processing test data row: {str(e)}")
                break
        
        # Insert remaining data
        if batch_data:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.executemany(insert_sql, batch_data)
            cursor.execute("COMMIT")
            rows_inserted += len(batch_data)
        
        # 🚀 RESTORE database settings
        cursor.execute("PRAGMA synchronous = NORMAL")  # Restore sync
        
        conn.close()
        
        sync_duration = time.time() - sync_start_time
        logger.info(f"✅ Optimized test data sync completed: {rows_inserted} rows inserted in {sync_duration:.2f}s")
        
        # Update refresh log and sync status (ONLY UPDATE AT END)
        update_refresh_log('test_data_optimized', rows_inserted, 'success')
        update_sync_status(sync_type, 'completed', 100, rows_inserted, total_rows, f"Sync completed successfully: {rows_inserted:,} rows inserted in {sync_duration:.2f}s")
        return True
        
    except Exception as e:
        sync_duration = time.time() - sync_start_time
        logger.error(f"❌ Error syncing optimized test data: {str(e)}")
        update_refresh_log('test_data_optimized', 0, f'error: {str(e)}')
        update_sync_status(sync_type, 'failed', 0, 0, 0, "Sync failed with error", str(e))
        return False

def sync_feature_data_optimized():
    """Sync feature data from Presto to optimized SQLite with reduced columns."""
    sync_start_time = time.time()
    sync_type = "feature_data_optimized"
    
    logger.info("🚀 Starting optimized feature data sync...")
    logger.info(f"📊 Database: {DB_PATH_OPTIMIZED}")
    logger.info(f"📝 Query: Optimized (18 columns)")
    
    # Set sync status to in_progress (ONLY UPDATE AT START)
    update_sync_status(sync_type, 'in_progress', 0, 0, 0, "Starting feature data sync from Presto...")
    
    try:
        # Execute optimized Presto query (18 columns instead of 38)
        logger.info("🔍 Executing Presto query for feature data...")
        
        query_start_time = time.time()
        result = execute_presto_query(query_feature_optimized)
        query_duration = time.time() - query_start_time
        
        if result is None:
            logger.error(f"❌ Failed to execute optimized feature data query after {query_duration:.2f}s")
            update_sync_status(sync_type, 'failed', 0, 0, 0, "Failed to execute Presto query", f"Query failed after {query_duration:.2f}s")
            return False
        
        logger.info(f"✅ Presto query completed in {query_duration:.2f}s")
        logger.info(f"📈 Retrieved {len(result):,} rows from Presto")
        
        conn = get_db_connection()
        if not conn:
            logger.error("Failed to get optimized database connection")
            update_sync_status(sync_type, 'failed', 0, 0, 0, "Failed to get database connection", "Database connection error")
            return False
        
        cursor = conn.cursor()
        logger.info("🔗 Connected to database, starting data insertion...")
        
        # 🚀 OPTIMIZED BULK INSERT with smaller transactions to reduce lock time
        cursor.execute("PRAGMA synchronous = OFF")  # Disable sync for speed
        cursor.execute("PRAGMA journal_mode = WAL")  # Keep WAL mode for concurrency
        
        # TRUNCATE table for clean data load (quick operation)
        cursor.execute("DELETE FROM terra_feature_data_optimized")
        conn.commit()  # Commit truncate immediately to release lock
        logger.info("🗑️ Truncated terra_feature_data_optimized table")
        
        # TESTING: Allow duplicates to match Looker Studio counts
        insert_sql = '''
            INSERT INTO terra_feature_data_optimized (
                exe_date, build_version, scenarioname, automation_lob, method_type,
                app_platform, os_version, result, no_of_runs, no_of_passed_runs, no_of_infra_runs, dl_failures,
                bug_id, issue_id, issue_key, status, priority, bug_creation_date, bug_resolution_date,project_name,project_category_name,
                bug_type,
                pb_bug_type, pfc_bug_type, occurence, no_of_masked_infra_runs,l1_category
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        rows_inserted = 0
        batch_size = 5000  # Smaller batch size to reduce lock time
        batch_data = []
        total_rows = len(result)
        
        # Get optimized column order
        column_order = get_optimized_column_orders()['terra_feature_data_optimized']
        
        for i, row in enumerate(result):
            try:
                # row['age'] = calculate_age(row.get('bug_creation_date'))
                # Extract values in optimized order (23 columns)
                row_values = extract_optimized_row_values(row, column_order)
                batch_data.append(row_values)
                
                if len(batch_data) >= batch_size:
                    # Use smaller transactions to reduce lock time
                    cursor.execute("BEGIN IMMEDIATE")  # Start transaction
                    cursor.executemany(insert_sql, batch_data)
                    cursor.execute("COMMIT")  # Commit immediately
                    
                    rows_inserted += len(batch_data)
                    logger.info(f"📊 Inserted {rows_inserted:,} feature data rows...")  # NO STATUS UPDATE - just log
                    batch_data = []
                    
            except Exception as e:
                logger.error(f"Error processing feature data row: {str(e)}")
                break
        
        # Insert remaining data
        if batch_data:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.executemany(insert_sql, batch_data)
            cursor.execute("COMMIT")
            rows_inserted += len(batch_data)
        
        # 🚀 RESTORE database settings
        cursor.execute("PRAGMA synchronous = NORMAL")  # Restore sync
        
        conn.close()
        
        sync_duration = time.time() - sync_start_time
        logger.info(f"✅ Optimized feature data sync completed: {rows_inserted} rows inserted in {sync_duration:.2f}s")
        
        # Update refresh log and sync status (ONLY UPDATE AT END)
        update_refresh_log('feature_data_optimized', rows_inserted, 'success')
        update_sync_status(sync_type, 'completed', 100, rows_inserted, total_rows, f"Sync completed successfully: {rows_inserted:,} rows inserted in {sync_duration:.2f}s")
        return True
        
    except Exception as e:
        sync_duration = time.time() - sync_start_time
        logger.error(f"❌ Error syncing optimized feature data: {str(e)}")
        update_refresh_log('feature_data_optimized', 0, f'error: {str(e)}')
        update_sync_status(sync_type, 'failed', 0, 0, 0, "Sync failed with error", str(e))
        return False

def update_refresh_log(refresh_type, rows_processed, status):
    """Update the refresh log table."""
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO mta_refresh_log (refresh_type, rows_processed, status)
                VALUES (?, ?, ?)
            ''', (refresh_type, rows_processed, status))
            conn.commit()
            conn.close()
    except Exception as e:
        logger.error(f"Error updating refresh log: {str(e)}")

def update_sync_status(sync_type, status, progress_percentage=0, rows_processed=0, total_rows=0, message=None, error_details=None):
    """Update the sync status table for real-time progress tracking with retry logic."""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            # Use shorter timeout for status updates to avoid blocking
            conn = get_db_connection(use_row_factory=False, timeout=10.0)
            if not conn:
                if attempt < max_retries - 1:
                    time.sleep(0.5 * (attempt + 1))  # Brief delay before retry
                    continue
                logger.warning(f"Failed to get database connection for sync status update after {max_retries} attempts")
                return False
            
            cursor = conn.cursor()
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            try:
                if status == 'in_progress':
                    # Starting a new sync
                    cursor.execute('''
                        INSERT OR REPLACE INTO mta_sync_status 
                        (sync_type, status, start_time, progress_percentage, rows_processed, total_rows, message, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (sync_type, status, current_time, progress_percentage, rows_processed, total_rows, message, current_time))
                elif status in ['completed', 'failed']:
                    # Completing a sync
                    cursor.execute('''
                        INSERT OR REPLACE INTO mta_sync_status 
                        (sync_type, status, start_time, end_time, progress_percentage, rows_processed, total_rows, message, error_details, last_updated)
                        SELECT sync_type, ?, COALESCE(start_time, ?), ?, ?, ?, ?, ?, ?, ?
                        FROM mta_sync_status WHERE sync_type = ?
                        UNION SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM mta_sync_status WHERE sync_type = ?)
                    ''', (status, current_time, current_time, progress_percentage, rows_processed, total_rows, message, error_details, current_time, sync_type,
                         sync_type, status, current_time, current_time, progress_percentage, rows_processed, total_rows, message, error_details, current_time, sync_type))
                else:
                    # Progress update
                    cursor.execute('''
                        UPDATE mta_sync_status 
                        SET status = ?, progress_percentage = ?, rows_processed = ?, message = ?, last_updated = ?
                        WHERE sync_type = ?
                    ''', (status, progress_percentage, rows_processed, message, current_time, sync_type))
                
                conn.commit()
                conn.close()
                logger.info(f"📊 Sync status updated: {sync_type} -> {status} ({progress_percentage}%)")
                return True
                
            except sqlite3.OperationalError as e:
                conn.close()
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    logger.warning(f"Database locked during sync status update, attempt {attempt + 1}")
                    time.sleep(0.5 * (attempt + 1))  # Brief delay before retry
                    continue
                else:
                    raise
                    
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Error updating sync status (attempt {attempt + 1}): {str(e)}")
                time.sleep(0.5 * (attempt + 1))
                continue
            else:
                logger.error(f"Error updating sync status after {max_retries} attempts: {str(e)}")
                return False
    
    return False

def get_sync_status(sync_type=None):
    """Get current sync status for a specific type or all types with retry logic."""
    max_retries = 2
    
    for attempt in range(max_retries):
        try:
            # Use shorter timeout for status reads to avoid blocking
            conn = get_db_connection(use_row_factory=False, timeout=5.0)
            if not conn:
                if attempt < max_retries - 1:
                    time.sleep(0.2)
                    continue
                return None
            
            cursor = conn.cursor()
            
            try:
                if sync_type:
                    cursor.execute('''
                        SELECT sync_type, status, start_time, end_time, progress_percentage, 
                               rows_processed, total_rows, message, error_details, last_updated
                        FROM mta_sync_status 
                        WHERE sync_type = ?
                        ORDER BY last_updated DESC
                        LIMIT 1
                    ''', (sync_type,))
                    result = cursor.fetchone()
                else:
                    cursor.execute('''
                        SELECT sync_type, status, start_time, end_time, progress_percentage, 
                               rows_processed, total_rows, message, error_details, last_updated
                        FROM mta_sync_status 
                        ORDER BY last_updated DESC
                    ''')
                    result = cursor.fetchall()
                
                conn.close()
                
                if result:
                    if sync_type:
                        # Single result
                        return {
                            'sync_type': result[0],
                            'status': result[1],
                            'start_time': result[2],
                            'end_time': result[3],
                            'progress_percentage': result[4],
                            'rows_processed': result[5],
                            'total_rows': result[6],
                            'message': result[7],
                            'error_details': result[8],
                            'last_updated': result[9]
                        }
                    else:
                        # Multiple results
                        return [{
                            'sync_type': row[0],
                            'status': row[1],
                            'start_time': row[2],
                            'end_time': row[3],
                            'progress_percentage': row[4],
                            'rows_processed': row[5],
                            'total_rows': row[6],
                            'message': row[7],
                            'error_details': row[8],
                            'last_updated': row[9]
                        } for row in result]
                
                return None
                
            except sqlite3.OperationalError as e:
                conn.close()
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    logger.warning(f"Database locked during sync status read, attempt {attempt + 1}")
                    time.sleep(0.2)
                    continue
                else:
                    raise
                    
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Error getting sync status (attempt {attempt + 1}): {str(e)}")
                time.sleep(0.2)
                continue
            else:
                logger.error(f"Error getting sync status after {max_retries} attempts: {str(e)}")
                return None
    
    return None

def is_sync_in_progress():
    """Check if any sync operation is currently in progress with retry logic."""
    try:
        # Use shorter timeout for quick status checks
        conn = get_db_connection(use_row_factory=False, timeout=3.0)
        if not conn:
            return False
        
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM mta_sync_status 
            WHERE status = 'in_progress'
        ''')
        result = cursor.fetchone()
        conn.close()
        
        return result[0] > 0 if result else False
        
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e):
            logger.warning("Database locked during sync progress check, assuming sync in progress")
            return True  # Assume sync is in progress if database is locked
        else:
            logger.error(f"Error checking sync progress: {str(e)}")
            return False
    except Exception as e:
        logger.error(f"Error checking sync progress: {str(e)}")
        return False

def calculate_optimized_metrics(filters):
    """Calculate dashboard metrics using optimized database with application-layer calculations."""
    import time
    start_time = time.time()
    logger.info("Starting optimized metrics calculation...")
    
    try:
        conn = get_db_connection()
        if not conn:
            return get_empty_dashboard_data()
        
        cursor = conn.cursor()
        
        # Build WHERE clauses separately for test and feature tables
        test_where_clause, test_params = build_where_clause_optimized(filters, target='test')
        feature_where_clause, feature_params = build_where_clause_optimized(filters, target='feature')
        
        # Scenario count query from feature data
        scenario_query = f"""
            SELECT COUNT(DISTINCT scenarioname) FROM terra_feature_data_optimized
            {feature_where_clause}
            {"AND" if feature_where_clause.strip() else "WHERE"} scenarioname IS NOT NULL
        """
        logger.info("Scenario query SQL: %s", scenario_query)
        logger.info("Scenario query params: %s", feature_params)
        cursor.execute(scenario_query, feature_params)
        scenario_count = cursor.fetchone()[0] or 0
        
        # Main metrics query on test-level data
        test_query = f"""
        WITH metrics_data AS (
            SELECT 
                test_case,
                COALESCE(no_of_runs, 0) as no_of_runs,
                COALESCE(no_of_passed_runs, 0) as no_of_passed_runs,
                COALESCE(no_of_infra_runs, 0) as no_of_infra_runs,
                COALESCE(dl_failures, 0) as dl_failures,
                bug_type,
                l1_category
            FROM terra_test_data_optimized
            {test_where_clause}
        ),
        aggregated_metrics AS (
            SELECT 
                COUNT(DISTINCT test_case) as total_features,
                SUM(no_of_runs) as runs,
                SUM(no_of_passed_runs) as passed_runs,
                SUM(no_of_runs - no_of_passed_runs) as failed_runs,
                -- Redefine infra_runs_new as sum of runs where l1_category is Infra or Masked Infra
                SUM(CASE WHEN l1_category IN ('Infra','Masked Infra') THEN no_of_runs ELSE 0 END) as infra_runs,
                SUM(dl_failures) as dl_failures
            FROM metrics_data
        ),
        bug_type_counts AS (
            SELECT 
                SUM(case
                when bug_type='Product Bugs' or l1_category="Product Bugs" or bug_type='Potential Product Bugs' or l1_category='Potential Product Bugs' then 1 else 0
                end) as product_bugs,
                SUM(CASE WHEN bug_type = 'Product Flow Changes' or l1_category="Product Flow Changes" THEN 1 ELSE 0 END) as pc_failure,
                SUM(CASE WHEN bug_type = 'Test Script Failures' THEN 1 ELSE 0 END) as t_script_failure,
                SUM(CASE WHEN bug_type = 'Not triaged' THEN 1 ELSE 0 END) as untriaged,
                SUM(CASE WHEN bug_type = 'Framework Tickets' THEN 1 ELSE 0 END) as frame_failures,
                SUM(case when l1_category='Unclassified' then 1 else 0 end) as unclassified
            FROM metrics_data
        )
        SELECT 
            a.total_features, a.runs, a.passed_runs, a.failed_runs, a.infra_runs, a.dl_failures,
            b.product_bugs, b.pc_failure, b.t_script_failure, b.untriaged, b.frame_failures, b.unclassified
        FROM aggregated_metrics a, bug_type_counts b
        """
        logger.info("Test query SQL: %s", test_query)
        logger.info("Test query params: %s", test_params)
        cursor.execute(test_query, test_params)
        result = cursor.fetchone()
        
        # Combined test methods count (test_case + scenarioname)
        combined_test_methods_query = f"""
        SELECT COUNT(DISTINCT test_method_name) FROM (
            SELECT DISTINCT test_case as test_method_name 
            FROM terra_test_data_optimized
            {test_where_clause}
            {"AND" if test_where_clause.strip() else "WHERE"} test_case IS NOT NULL
            
    
        )
        """
        logger.info("Combined test methods query SQL: %s", combined_test_methods_query)

        logger.info("Combined test methods query params: %s", test_params)
        cursor.execute(combined_test_methods_query, test_params)
        combined_test_methods_count = cursor.fetchone()[0] or 0
        
        # Breakdown query from test data
        breakdown_query = f"""
        SELECT 
            -- Product Bug breakdowns
            SUM(CASE WHEN pb_bug_type = 'One time' THEN 1 ELSE 0 END) as one_time,
            SUM(CASE WHEN pb_bug_type = 'Intermittent' THEN 1 ELSE 0 END) as intermittent,
            SUM(CASE WHEN pb_bug_type = 'Always' THEN 1 ELSE 0 END) as always,
            SUM(case when bug_type='Potential Product Bugs' or l1_category='Potential Product Bugs' THEN 1 ELSE 0 END) as PPB_failures,
            -- PFC breakdowns  
            SUM(CASE WHEN pfc_bug_type = 'Flow Change' THEN 1 ELSE 0 END) as flow_change,
            SUM(CASE WHEN pfc_bug_type = 'Identifier' THEN 1 ELSE 0 END) as identifier,
            SUM(CASE WHEN pfc_bug_type = 'Others' THEN 1 ELSE 0 END) as others,
            -- Masked infra runs
            SUM(no_of_masked_infra_runs) as masked_infra_runs
        FROM terra_test_data_optimized
        {test_where_clause}
        """
        logger.info("Breakdown query SQL: %s", breakdown_query)
        logger.info("Breakdown query params: %s", test_params)
        cursor.execute(breakdown_query, test_params)
        breakdown_result = cursor.fetchone()
        
        # Occurrence: count distinct occurence values in filtered test data
        occurrence_query = f"""
        SELECT COUNT(DISTINCT occurence) FROM terra_test_data_optimized
        {test_where_clause}
        """
        logger.info("Occurrence query SQL: %s", occurrence_query)
        logger.info("Occurrence query params: %s", test_params)
        cursor.execute(occurrence_query, test_params)
        occurrence_count = cursor.fetchone()[0] or 0

        if result and breakdown_result:
            metrics = {
                'total_features': result[0] or 0,
                'tests': scenario_count,
                'test_methods': combined_test_methods_count,
                'runs': result[1] or 0,
                'passed_runs': result[2] or 0,
                'fail': result[3] or 0,
                'dl_failures': result[5] or 0,
                'infra_runs_new': result[4] or 0,
                'no_of_masked_infra_runs': breakdown_result[6] or 0,
                'product_bugs': result[6] or 0,
                'one_time': breakdown_result[0] or 0,
                'intermittent': breakdown_result[1] or 0,
                'always': breakdown_result[2] or 0,
                'PPB_failures':breakdown_result[3] or 0,
                'pc_failure': result[7] or 0,
                'flow_change': breakdown_result[3] or 0,
                'identifier': breakdown_result[4] or 0,
                'others': breakdown_result[5] or 0,
                't_script_failure': result[8] or 0,
                'u1': result[9] or 0,
                'frame_failures': result[10] or 0,
                'unclassified_failure': result[11] or 0,
                'occurrence': occurrence_count
            }
        else:
            metrics = {key: 0 for key in ['total_features', 'test_methods', 'runs', 'passed_runs', 'fail']}
        
        # Calculate percentages
        total_runs = metrics['runs']
        total_features = metrics['total_features']
        
        # Run-based percentages
        if total_runs > 0:
            metrics['pass_rate'] = f"{(metrics['passed_runs'] / total_runs * 100):.2f}%"
            metrics['fail_rate'] = f"{(metrics['fail'] / total_runs * 100):.2f}%"
            metrics['dl_failures_p'] = f"{(metrics['dl_failures'] / total_runs * 100):.2f}%"
            metrics['infra_runs_new_p'] = f"{(metrics['infra_runs_new'] / total_runs * 100):.2f}%"
            # Requested: pb_perc and pc_failure_p over runs
            metrics['pb_perc'] = f"{(metrics['product_bugs'] / total_runs * 100):.2f}%"
            metrics['pc_failure_p'] = f"{(metrics['pc_failure'] / total_runs * 100):.2f}%"
            metrics['u1_p'] = f"{(metrics['u1'] / total_runs * 100):.2f}%"
            metrics['unclassified_failure_p'] = f"{(metrics['unclassified_failure'] / total_runs * 100):.2f}%"

        else:
            metrics.update({
                'pass_rate': "0.0%", 'fail_rate': "0.0%",
                'dl_failures_p': "0.0%", 'infra_runs_new_p': "0.0%",
                'pb_perc': "0.0%", 'pc_failure_p': "0.0%"
            })
        
        # Feature-based percentages (leave others as feature-based)
        if total_features > 0:
            percentage_keys = [
                ('t_script_failure_p', 't_script_failure'),
                ('frame_failures_p', 'frame_failures')
            ]
            for perc_key, count_key in percentage_keys:
                metrics[perc_key] = f"{(metrics[count_key] / total_features * 100):.1f}%"
            
            # Product Bug breakdown percentages
            metrics['one_time_p'] = f"{(metrics['one_time'] / total_features * 100):.2f}%"
            metrics['intermittent_p'] = f"{(metrics['intermittent'] / total_features * 100):.2f}%"
            metrics['always_p'] = f"{(metrics['always'] / total_features * 100):.2f}%"
            # metrics['ppb_p']=f"{(metrics['PPB_failures'] / total_features * 100):.2f}%"
            metrics['ppb_p'] = f"{(metrics['PPB_failures'] / total_runs * 100):.2f}%"
            # PFC breakdown percentages
            metrics['flow_change_p'] = f"{(metrics['flow_change'] / total_features * 100):.2f}%"
            metrics['identifier_p'] = f"{(metrics['identifier'] / total_features * 100):.2f}%"
            metrics['others_p'] = f"{(metrics['others'] / total_features * 100):.2f}%"
        else:
            metrics.update({
                'one_time_p': "0.0%", 'intermittent_p': "0.0%", 'always_p': "0.0%",'ppb_p' : "0.0%",
                'flow_change_p': "0.0%", 'identifier_p': "0.0%", 'others_p': "0.0%"
            })
        
        # Additional fields
        metrics.update({'p_c_name': "N/A", 'occurence': "N/A"})
        
        conn.close()
        
        duration = time.time() - start_time
        logger.info(f"✅ Optimized metrics calculation completed in {duration:.2f}s")
        
        return metrics
        
    except Exception as e:
        logger.error(f"❌ Error calculating optimized metrics: {str(e)}")
        return get_empty_dashboard_data()


def build_where_clause_optimized(filters, target='both'):
    """
    Build SQL WHERE clause and parameters for optimized queries.

    Args:
        filters (dict): Dictionary of possible filters.
        target (str): Filter context - 'feature', 'test', or 'both' (default).

    Returns:
        tuple: (where_clause_str, params_list)
    """
    where_conditions = []
    params = []

    def is_valid(value):
        if value is None:
            return False
        # Accept non-empty lists for multi-select
        if isinstance(value, (list, tuple)):
            return len([v for v in value if v != '' and v is not None]) > 0
        return value != ''

    if filters:
        if is_valid(filters.get('latest_release')):
            rel = filters.get('latest_release')
            if isinstance(rel, (list, tuple)):
                rel = [r for r in rel if r]
                if rel:
                    or_parts = ["build_version LIKE ?" for _ in rel]
                    where_conditions.append("(" + " OR ".join(or_parts) + ")")
                    params.extend([f"%{r}%" for r in rel])
            else:
                where_conditions.append("build_version LIKE ?")
                params.append(f"%{rel}%")

        if is_valid(filters.get('build_version')):
            bv = filters.get('build_version')
            if isinstance(bv, (list, tuple)):
                bv = [b for b in bv if b]
                placeholders = ",".join(["?"] * len(bv))
                where_conditions.append(f"build_version IN ({placeholders})")
                params.extend(bv)
            else:
                where_conditions.append("build_version = ?")
                params.append(bv)

        if is_valid(filters.get('lob')):
            lob = filters.get('lob')
            if isinstance(lob, (list, tuple)):
                lob = [l for l in lob if l]
                placeholders = ",".join(["?"] * len(lob))
                where_conditions.append(f"automation_lob IN ({placeholders})")
                params.extend(lob)
            else:
                where_conditions.append("automation_lob = ?")
                params.append(lob)

        if is_valid(filters.get('app_platform')):
            ap = filters.get('app_platform')
            if isinstance(ap, (list, tuple)):
                ap = [a for a in ap if a]
                placeholders = ",".join(["?"] * len(ap))
                where_conditions.append(f"app_platform IN ({placeholders})")
                params.extend(ap)
            else:
                where_conditions.append("app_platform = ?")
                params.append(ap)

        if is_valid(filters.get('os_version')):
            osv = filters.get('os_version')
            if isinstance(osv, (list, tuple)):
                osv = [o for o in osv if o]
                placeholders = ",".join(["?"] * len(osv))
                where_conditions.append(f"os_version IN ({placeholders})")
                params.extend(osv)
            else:
                where_conditions.append("os_version = ?")
                params.append(osv)

        if is_valid(filters.get('project_name')):
            pn = filters.get('project_name')
            if isinstance(pn, (list, tuple)):
                pn = [p for p in pn if p]
                placeholders = ",".join(["?"] * len(pn))
                where_conditions.append(f"project_name IN ({placeholders})")
                params.extend(pn)
            else:
                where_conditions.append("project_name = ?")
                params.append(pn)

        if is_valid(filters.get('project_category_name')):
            pcn = filters.get('project_category_name')
            if isinstance(pcn, (list, tuple)):
                pcn = [p for p in pcn if p]
                placeholders = ",".join(["?"] * len(pcn))
                where_conditions.append(f"project_category_name IN ({placeholders})")
                params.extend(pcn)
            else:
                where_conditions.append("project_category_name = ?")
                params.append(pcn)
  
      

        # Add scenarioname filter ONLY for feature or both targets
        if is_valid(filters.get('scenarioname')):
            if target in ['feature', 'both']:
                sn = filters.get('scenarioname')
                if isinstance(sn, (list, tuple)):
                    sn = [s for s in sn if s]
                    placeholders = ",".join(["?"] * len(sn))
                    where_conditions.append(f"scenarioname IN ({placeholders})")
                    params.extend(sn)
                else:
                    where_conditions.append("scenarioname = ?")
                    params.append(sn)

        if is_valid(filters.get('test_case')):
            if target in ['test', 'both']:
                tc = filters.get('test_case')
                if isinstance(tc, (list, tuple)):
                    tc = [t for t in tc if t]
                    placeholders = ",".join(["?"] * len(tc))
                    where_conditions.append(f"test_case IN ({placeholders})")
                    params.extend(tc)
                else:
                    where_conditions.append("test_case = ?")
                    params.append(tc)

        if is_valid(filters.get('l1_category')):
            if target in ['test', 'both']:
                l1 = filters.get('l1_category')
                if isinstance(l1, (list, tuple)):
                    l1 = [x for x in l1 if x]
                    placeholders = ",".join(["?"] * len(l1))
                    where_conditions.append(f"l1_category IN ({placeholders})")
                    params.extend(l1)
                else:
                    where_conditions.append("l1_category = ?")
                    params.append(l1)        

        # Handle date range filters safely, defaulting to last 30 days if none provided
        start_date = filters.get('start_date')
        end_date = filters.get('end_date')
        try:
            if is_valid(start_date) and is_valid(end_date):
                where_conditions.append("exe_date BETWEEN ? AND ?")
                params.extend([f"{start_date} 00:00:00", f"{end_date} 23:59:59"])
            elif is_valid(start_date):
                where_conditions.append("exe_date >= ?")
                params.append(f"{start_date} 00:00:00")
            elif is_valid(end_date):
                where_conditions.append("exe_date <= ?")
                params.append(f"{end_date} 23:59:59")
            else:
                # Default: last 30 days
                where_conditions.append("exe_date >= date('now','-30 day')")
        except Exception:
            # On any parsing error, fall back to last 30 days
            where_conditions.append("exe_date >= date('now','-30 day')")

    # Always apply this base condition
    where_conditions.append("no_of_runs > 0")

    where_clause = f"WHERE {' AND '.join(where_conditions)}"
    return where_clause, params


def get_filter_options_optimized(active_filters=None):
    """Get filter options contextually, applying other active filters without clearing current selections."""
    start_time = time.time()
    logger.info("Starting get_filter_options_optimized (context-aware)...")
    
    # Ensure dict
    active_filters = active_filters or {}

    conn = get_db_connection(use_row_factory=False)
    if not conn:
        logger.warning("Failed to get database connection for filter options")
        return {
            'latest_release': [],
            'build_version': [],
            'lob': [],
            'app_platform': [],
            'os_version': [],
            'project_name': [],
            'project_category_name': [],
            'test_case': [],
            'scenarioname': [],
            'l1_category': []
        }

    def other_filters(exclude_key: str):
        filt = {k: v for k, v in active_filters.items() if k != exclude_key}
        return filt

    try:
        cursor = conn.cursor()

        results = {
            'latest_release': [],
            'build_version': [],
            'lob': [],
            'app_platform': [],
            'os_version': [],
            'project_name': [],
            'project_category_name': [],
            'test_case': [],
            'scenarioname': [],
            'l1_category': []
        }

        # Build options per dimension using other active filters
        # 1) latest_release (derived from build_version) - use test table
        wl, pl = build_where_clause_optimized(other_filters('latest_release'), target='test')
        query_latest_release = f"""
            SELECT 
                CASE 
                    WHEN build_version LIKE '%10000%' THEN '10000'
                    WHEN build_version LIKE '%10001%' THEN '10001'
                    WHEN build_version LIKE '%10002%' THEN '10002'
                    WHEN build_version LIKE '%10003%' THEN '10003'
                    WHEN build_version LIKE '%10004%' THEN '10004'
                    ELSE 'Other'
                END as value,
                COUNT(*) as cnt
            FROM terra_test_data_optimized
            {wl} AND build_version IS NOT NULL
            GROUP BY value
            ORDER BY cnt DESC
        """
        cursor.execute(query_latest_release, pl)
        results['latest_release'] = [(row[0],) for row in cursor.fetchall()]

        # 2) build_version - test table
        wl, pl = build_where_clause_optimized(other_filters('build_version'), target='test')
        cursor.execute(f"""
            SELECT build_version, COUNT(*) as cnt
            FROM terra_test_data_optimized
            {wl} AND build_version IS NOT NULL
            GROUP BY build_version
            ORDER BY cnt DESC
            LIMIT 30
        """, pl)
        results['build_version'] = cursor.fetchall()

        # 3) lob (automation_lob) - test table
        wl, pl = build_where_clause_optimized(other_filters('lob'), target='test')
        cursor.execute(f"""
            SELECT automation_lob, COUNT(*) as cnt
            FROM terra_test_data_optimized
            {wl} AND automation_lob IS NOT NULL
            GROUP BY automation_lob
            ORDER BY cnt DESC
        """, pl)
        results['lob'] = cursor.fetchall()

        # 4) app_platform - test table
        wl, pl = build_where_clause_optimized(other_filters('app_platform'), target='test')
        cursor.execute(f"""
            SELECT app_platform, COUNT(*) as cnt
            FROM terra_test_data_optimized
            {wl} AND app_platform IS NOT NULL
            GROUP BY app_platform
            ORDER BY cnt DESC
        """, pl)
        results['app_platform'] = cursor.fetchall()

        # 5) os_version - test table
        wl, pl = build_where_clause_optimized(other_filters('os_version'), target='test')
        cursor.execute(f"""
            SELECT os_version, COUNT(*) as cnt
            FROM terra_test_data_optimized
            {wl} AND os_version IS NOT NULL
            GROUP BY os_version
            ORDER BY cnt DESC
        """, pl)
        results['os_version'] = cursor.fetchall()

        # 6) project_name - test table
        wl, pl = build_where_clause_optimized(other_filters('project_name'), target='test')
        cursor.execute(f"""
            SELECT project_name, COUNT(*) as cnt
            FROM terra_test_data_optimized
            {wl} AND project_name IS NOT NULL
            GROUP BY project_name
            ORDER BY cnt DESC
        """, pl)
        results['project_name'] = cursor.fetchall()

        # 7) project_category_name - test table
        wl, pl = build_where_clause_optimized(other_filters('project_category_name'), target='test')
        cursor.execute(f"""
            SELECT project_category_name, COUNT(*) as cnt
            FROM terra_test_data_optimized
            {wl} AND project_category_name IS NOT NULL
            GROUP BY project_category_name
            ORDER BY cnt DESC
        """, pl)
        results['project_category_name'] = cursor.fetchall()

        # 8) test_case - test table
        wl, pl = build_where_clause_optimized(other_filters('test_case'), target='test')
        cursor.execute(f"""
            SELECT test_case, COUNT(*) as cnt
            FROM terra_test_data_optimized
            {wl} AND test_case IS NOT NULL
            GROUP BY test_case
            ORDER BY cnt DESC
        """, pl)
        results['test_case'] = cursor.fetchall()

        # 9) scenarioname - feature table
        wf, pf = build_where_clause_optimized(other_filters('scenarioname'), target='feature')
        cursor.execute(f"""
            SELECT scenarioname, COUNT(*) as cnt
            FROM terra_feature_data_optimized
            {wf} AND scenarioname IS NOT NULL
            GROUP BY scenarioname
            ORDER BY cnt DESC
        """, pf)
        results['scenarioname'] = cursor.fetchall()

        # 10) l1_category - test table
        wl, pl = build_where_clause_optimized(other_filters('l1_category'), target='test')
        cursor.execute(f"""
            SELECT l1_category, COUNT(*) as cnt
            FROM terra_test_data_optimized
            {wl} AND l1_category IS NOT NULL
            GROUP BY l1_category
            ORDER BY cnt DESC
        """, pl)
        results['l1_category'] = cursor.fetchall()

        conn.close()
        elapsed_time = time.time() - start_time
        logger.info(f"✅ get_filter_options_optimized (context-aware) completed in {elapsed_time:.2f} seconds")
        return results

    except Exception as e:
        logger.error(f"Error getting optimized filter options: {str(e)}")
        if conn:
            conn.close()
        return {
            'latest_release': [],
            'build_version': [],
            'lob': [],
            'app_platform': [],
            'os_version': [],
            'project_name': [],
            'project_category_name': [],
            'test_case': [],
            'scenarioname': [],
            'l1_category': []
        }

def get_pass_rate_breakdowns_optimized(filters=None):
    """Get optimized pass rate breakdowns by LOB and App Platform."""
    if filters is None:
        filters = {}
    
    start_time = time.time()
    logger.info("Starting get_pass_rate_breakdowns_optimized...")
    
    conn = get_db_connection()
    if not conn:
        return {'lob_breakdown': [], 'platform_breakdown': []}
    
    try:
        cursor = conn.cursor()

        # Build separate WHERE clauses for test and feature data
        test_where_clause, test_params = build_where_clause_optimized(filters, target='test')
        feature_where_clause, feature_params = build_where_clause_optimized(filters, target='feature')
        
        combined_query = f"""
        SELECT 
            'lob' as breakdown_type,
            automation_lob as category,
            SUM(no_of_runs) as total_runs,
            SUM(no_of_passed_runs) as passed_runs,
            ROUND((SUM(no_of_passed_runs) * 100.0 / SUM(no_of_runs)), 2) as pass_rate
        FROM (
            SELECT automation_lob, no_of_runs, no_of_passed_runs
            FROM terra_test_data_optimized 
            {test_where_clause} AND automation_lob IS NOT NULL
            UNION ALL
            SELECT automation_lob, no_of_runs, no_of_passed_runs
            FROM terra_feature_data_optimized 
            {feature_where_clause} AND automation_lob IS NOT NULL
        ) combined_lob_data
        GROUP BY automation_lob
        
        UNION ALL
        
        SELECT 
            'platform' as breakdown_type,
            app_platform as category,
            SUM(no_of_runs) as total_runs,
            SUM(no_of_passed_runs) as passed_runs,
            ROUND((SUM(no_of_passed_runs) * 100.0 / SUM(no_of_runs)), 2) as pass_rate
        FROM (
            SELECT app_platform, no_of_runs, no_of_passed_runs
            FROM terra_test_data_optimized 
            {test_where_clause} AND app_platform IS NOT NULL
            UNION ALL
            SELECT app_platform, no_of_runs, no_of_passed_runs
            FROM terra_feature_data_optimized 
            {feature_where_clause} AND app_platform IS NOT NULL
        ) combined_platform_data
        GROUP BY app_platform
        
        ORDER BY breakdown_type, pass_rate DESC
        """

        # Combine params for test + feature for LOB and platform queries (4 sets)
        all_params = test_params + feature_params + test_params + feature_params
        
        cursor.execute(combined_query, all_params)
        results = cursor.fetchall()
        
        lob_breakdown = []
        platform_breakdown = []
        total_runs_all = 0
        total_passed_all = 0
        total_runs_platform = 0
        total_passed_platform = 0
        
        for row in results:
            breakdown_type, category, total, passed, rate = row
            
            if breakdown_type == 'lob':
                lob_breakdown.append({
                    'lob': category,
                    'total_runs': total,
                    'passed_runs': passed,
                    'pass_rate': f"{rate}%"
                })
                total_runs_all += total
                total_passed_all += passed
            else:  # platform
                platform_breakdown.append({
                    'platform': category,
                    'total_runs': total,
                    'passed_runs': passed,
                    'pass_rate': f"{rate}%"
                })
                total_runs_platform += total
                total_passed_platform += passed
        
        # Add grand totals
        if total_runs_all > 0:
            grand_total_rate = round((total_passed_all * 100.0 / total_runs_all), 2)
            lob_breakdown.append({
                'lob': 'Grand total',
                'total_runs': total_runs_all,
                'passed_runs': total_passed_all,
                'pass_rate': f"{grand_total_rate}%",
                'is_total': True
            })
        
        if total_runs_platform > 0:
            grand_total_platform_rate = round((total_passed_platform * 100.0 / total_runs_platform), 2)
            platform_breakdown.append({
                'platform': 'Grand total',
                'total_runs': total_runs_platform,
                'passed_runs': total_passed_platform,
                'pass_rate': f"{grand_total_platform_rate}%",
                'is_total': True
            })
        
        conn.close()
        
        elapsed_time = time.time() - start_time
        logger.info(f"get_pass_rate_breakdowns_optimized completed in {elapsed_time:.2f} seconds")
        
        return {
            'lob_breakdown': lob_breakdown,
            'platform_breakdown': platform_breakdown
        }
        
    except Exception as e:
        logger.error(f"Error getting optimized pass rate breakdowns: {str(e)}")
        if conn:
            conn.close()
        return {'lob_breakdown': [], 'platform_breakdown': []}


import time
import logging

logger = logging.getLogger(__name__)

def get_feature_breakdowns_optimized(filters=None, feature_page=1, per_page=10):
    if filters is None:
        filters = {}

    start_time = time.time()
    logger.info("Starting get_feature_test_breakdowns_optimized...")
    logger.info(f"Filters applied: {filters}")

    conn = get_db_connection()
    if not conn:
        return {
            'feature_breakdown': [],
          
            'feature_total': 0,
          
            'feature_pages': 0,
          
            'feature_current_page': feature_page,
      
            'per_page': per_page
        }

    try:
        cursor = conn.cursor()

        feature_where_clause, feature_params = build_where_clause_optimized(filters,target='feature')
        print(feature_where_clause)
        print(feature_params)

        # --- Feature total count with filters ---
        feature_count_query = f"""
        SELECT COUNT(DISTINCT scenarioname)
        FROM terra_feature_data_optimized
        {feature_where_clause} AND scenarioname IS NOT NULL
        """
        cursor.execute(feature_count_query, feature_params)
        feature_total = cursor.fetchone()[0]

        feature_offset = (feature_page - 1) * per_page

        feature_query = f"""
        SELECT 
            scenarioname AS feature_name,
            COUNT(CASE WHEN bug_id IS NOT NULL AND bug_id != '' THEN 1 END) AS bugs,
            SUM(no_of_runs) AS total_runs,
            SUM(no_of_passed_runs) AS passed_runs,
            ROUND((SUM(no_of_passed_runs) * 100.0 / SUM(no_of_runs)), 2) AS pass_rate
            
        FROM terra_feature_data_optimized
        {feature_where_clause} AND scenarioname IS NOT NULL
        GROUP BY scenarioname
        ORDER BY bugs DESC, pass_rate ASC
        LIMIT ? OFFSET ?
        """
        cursor.execute(feature_query, feature_params + [per_page, feature_offset])
        feature_results = cursor.fetchall()

        feature_breakdown = []
        total_bugs = 0
        total_runs_feature = 0
        total_passed_feature = 0

        for feature_name, bugs, total_runs, passed_runs, pass_rate in feature_results:
            feature_breakdown.append({
                'feature': feature_name,
                'bugs': bugs or 0,
                'pass_rate': pass_rate or 0.0
            })
            total_bugs += (bugs or 0)
            total_runs_feature += (total_runs or 0)
            total_passed_feature += (passed_runs or 0)

        if total_runs_feature > 0:
            grand_rate_feature = round(total_passed_feature / total_runs_feature, 2)
            feature_breakdown.append({
                'feature': 'Grand total',
                'bugs': total_bugs,
                'pass_rate': grand_rate_feature,
                'is_total': True
            })

       

       

       
        conn.close()

        elapsed_time = time.time() - start_time
        logger.info(f"get_feature_breakdowns_optimized completed in {elapsed_time:.2f} seconds")

        return {
            'feature_breakdown': feature_breakdown,
           
            'feature_total': feature_total,
         
            'feature_pages': (feature_total + per_page - 1) // per_page,
            
            'feature_current_page': feature_page,
           
            'per_page': per_page
        }

    except Exception as e:
        logger.error(f"Error getting optimized feature/test breakdowns: {str(e)}")
        if conn:
            conn.close()
        return {
            'feature_breakdown': [],
            
            'feature_total': 0,
            
            'feature_pages': 0,
           
            'feature_current_page': feature_page,
            
            'per_page': per_page
        }



def get_test_breakdowns_optimized(filters=None, test_method_page=1, per_page=20):
    if filters is None:
        filters = {}

    start_time = time.time()
    logger.info("Starting get_test_breakdowns_optimized...")
    logger.info(f"Filters applied: {filters}")

    conn = get_db_connection()
    if not conn:
        return {
            'test_method_breakdown': [],
            'test_method_total': 0,
            'test_method_pages': 0,
            'test_method_current_page': test_method_page,
            'per_page': per_page
        }

    try:
        cursor = conn.cursor()

        test_where_clause, test_params = build_where_clause_optimized(filters, target='test')

        # Log the WHERE clause and parameters
        logger.info(f"Test WHERE clause: {test_where_clause}")
        logger.info(f"Test query parameters: {test_params}")

        test_method_offset = (test_method_page - 1) * per_page

        count_query = f"""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT test_case AS name FROM terra_test_data_optimized
            {test_where_clause} AND test_case IS NOT NULL
        ) AS combined_methods
        """

        logger.info(f"Count query: {count_query.strip()}")
        logger.info(f"Count query params: {test_params}")

        cursor.execute(count_query, test_params)
        test_method_total = cursor.fetchone()[0]

        test_method_query = f"""
        SELECT 
            test_method_name,
            SUM(bugs) AS bugs,
            ROUND((SUM(passed_runs) * 100.0 / SUM(total_runs)), 2) AS pass_rate
        FROM (
            SELECT 
                test_case AS test_method_name,
                COUNT(CASE WHEN bug_id IS NOT NULL AND bug_id != '' THEN 1 END) AS bugs,
                SUM(no_of_runs) AS total_runs,
                SUM(no_of_passed_runs) AS passed_runs
            FROM terra_test_data_optimized
            {test_where_clause} AND test_case IS NOT NULL
            GROUP BY test_case
        ) combined_methods
        GROUP BY test_method_name
        ORDER BY pass_rate DESC
        LIMIT ? OFFSET ?
        """

        logger.info(f"Test method query: {test_method_query.strip()}")
        logger.info(f"Test method query params: {test_params + [per_page, test_method_offset]}")

        # FIX: Remove duplicate test_params, only one set needed
        cursor.execute(test_method_query, test_params + [per_page, test_method_offset])
        test_method_results = cursor.fetchall()

        test_method_breakdown = []
        for test_method_name, bugs, pass_rate in test_method_results:
            test_method_breakdown.append({
                'test_method': test_method_name,
                'bugs': bugs or 0,
                'pass_rate': f"{pass_rate or 0.0}%"
            })

        conn.close()

        elapsed_time = time.time() - start_time
        logger.info(f"get_test_breakdowns_optimized completed in {elapsed_time:.2f} seconds")

        return {
            'test_method_breakdown': test_method_breakdown,
            'test_method_total': test_method_total,
            'test_method_pages': (test_method_total + per_page - 1) // per_page,
            'test_method_current_page': test_method_page,
            'per_page': per_page
        }

    except Exception as e:
        logger.error(f"Error getting optimized feature/test breakdowns: {str(e)}")
        if conn:
            conn.close()
        return {
            'test_method_breakdown': [],
            'test_method_total': 0,
            'test_method_pages': 0,
            'test_method_current_page': test_method_page,
            'per_page': per_page
        }

def add_bug_filter(where_clause):
    where_clause = where_clause.strip()
    if where_clause.lower().startswith("where"):
        return where_clause + " AND bug_id IS NOT NULL AND bug_id != ''"
    elif where_clause == "":
        return "WHERE bug_id IS NOT NULL AND bug_id != ''"
    else:
        return "WHERE " + where_clause + " AND bug_id IS NOT NULL AND bug_id != ''"

def get_bug_level_data_optimized(filters=None, page=1, per_page=10):
    if filters is None:
        filters = {}

    start_time = time.time()
    logger.info(f"Starting get_bug_level_data_optimized for page {page}...")

    conn = get_db_connection()
    if not conn:
        return {'bugs': [], 'total': 0, 'pages': 0}

    try:
        cursor = conn.cursor()

        test_where_clause, test_params = build_where_clause_optimized(filters, target='test')
        feature_where_clause, feature_params = build_where_clause_optimized(filters, target='feature')

        test_where_clause = add_bug_filter(test_where_clause)
        feature_where_clause = add_bug_filter(feature_where_clause)

        # Count total bugs from BOTH test and feature data
        count_query = f"""
        SELECT COUNT(*) FROM (
            SELECT bug_id FROM terra_test_data_optimized {test_where_clause}
            UNION ALL
            SELECT bug_id FROM terra_feature_data_optimized {feature_where_clause}
        )
        """

        cursor.execute(count_query, test_params + feature_params)
        total_bugs = cursor.fetchone()[0]

        offset = (page - 1) * per_page

        bug_query = f"""
        SELECT 
            bug_id,
            project_category_name as project_category,
            project_name as jira_project,
            build_version,
            occurence,
            status,
            priority,
            test_case_or_scenario,
            age,
            app_platform as platform,
            l1_category,
            CASE
                WHEN age > SLA_days THEN 'Y'
                ELSE 'N'
            END as oosla,
            no_of_runs as failures
        FROM (
            SELECT 
                bug_id, build_version, status, priority, test_case as test_case_or_scenario,
                app_platform, no_of_runs, project_category_name, project_name, occurence, 
                CASE 
                    WHEN status = 'Closed' THEN 
                        CAST(julianday(bug_resolution_date) - julianday(bug_creation_date) AS INTEGER)
                    ELSE 
                        CAST(julianday(CURRENT_DATE) - julianday(bug_creation_date) AS INTEGER)
                END as age,
                l1_category,
                CASE
                    WHEN (bug_type="Product Bugs") AND priority="P0" THEN 1
                    WHEN (bug_type="Product Bugs") AND priority="P1" THEN 7
                    WHEN (bug_type="Product Bugs") AND priority="P2" THEN 30
                    WHEN (bug_type="Product Bugs") AND priority="P3" THEN 120
                    WHEN (bug_type="Product Bugs") AND priority="P4" THEN 365
                    WHEN (bug_type="Infra Bugs" OR l1_category="Masked Infra" OR l1_category='Infra') AND priority="P0" THEN 1
                    WHEN (bug_type="Infra Bugs" OR l1_category="Masked Infra" OR l1_category='Infra') AND priority="P1" THEN 7
                    WHEN (bug_type="Infra Bugs" OR l1_category="Masked Infra" OR l1_category='Infra') AND priority="P2" THEN 30
                    WHEN (bug_type="Infra Bugs" OR l1_category="Masked Infra" OR l1_category='Infra') AND priority="P3" THEN 120
                    WHEN (bug_type="Infra Bugs" OR l1_category="Masked Infra" OR l1_category='Infra') AND priority="P4" THEN 365
                    WHEN bug_type="Product Flow Changes" THEN 14
                    WHEN bug_type="Product Flow Changes" AND automation_lob='UWF' THEN 7
                    WHEN bug_type="Test Script Bugs" THEN 7
                END as SLA_days
            FROM terra_test_data_optimized {test_where_clause}
        ) combined_bugs
        ORDER BY bug_id
        
        """

        # Use only test_params (no feature data in bug table)
        # params = test_params + [per_page, offset]
        # cursor.execute(bug_query, params)
        # bug_results = cursor.fetchall()


        # --------------------------
        # Count total (no LIMIT/OFFSET yet)
        # --------------------------
        count_query = f"SELECT COUNT(*) FROM ({bug_query})"
        cursor.execute(count_query, test_params)
        total_bugs = cursor.fetchone()[0]

        # --------------------------
        # Apply pagination only here
        # --------------------------
        bug_query_paged = f"""
        SELECT * FROM ({bug_query})
        ORDER BY bug_id
        LIMIT ? OFFSET ?
        """
        params = test_params + [per_page, (page - 1) * per_page]
        cursor.execute(bug_query_paged, params)
        bug_results = cursor.fetchall()

        # --------------------------
        # Format result
        # --------------------------
        # bugs = []

        bugs = []
        for row in bug_results:
            bugs.append({
                'bug_id': row[0] or 'N/A',
                'project_category': row[1] or 'GSS',
                'jira_project': row[2] or 'N/A',
                'build_version': row[3] or 'N/A',
                'occurrence': row[4] or 'N/A',
                'status': row[5] or 'Open',
                'priority': row[6] or 'P1',
                'test_case': row[7] or 'N/A',
                'age': row[8] or 0,
                'platform': row[9] or 'android',
                'l1_category': row[10],
                'oosla': row[11] or 'N',
                'failures': row[12] or 0
            })

        conn.close()

        result = {
            'bugs': bugs,
            'total': total_bugs,
            'pages': (total_bugs + per_page - 1) // per_page,
            'current_page': page,
            'per_page': per_page
        }

        elapsed_time = time.time() - start_time
        logger.info(f"get_bug_level_data_optimized completed in {elapsed_time:.2f} seconds")

        return result

    except Exception as e:
        logger.error(f"Error in get_bug_level_data_optimized: {e}")
        if conn:
            conn.close()
        return {'bugs': [], 'total': 0, 'pages': 0}



def get_empty_dashboard_data():
    """Return empty data structure for dashboard error handling."""
    return {
        'latest_release': [], 'build_version': [], 'lob': [], 
        'app_platform': [], 'os_version': [],
        'total_features': 0, 'tests': 0, 'test_methods': 0, 'runs': 0,
        'passed_runs': 0, 'fail': 0, 'pass_rate': '0.0%', 'fail_rate': '0.0%',
        'product_bugs': 0, 'pb_perc': '0.0%', 'one_time': 0, 'intermittent': 0, 'always': 0,
        'one_time_p': '0.0%', 'intermittent_p': '0.0%', 'always_p': '0.0%','ppb_p': '0.0%',
        'pc_failure': 0, 'pc_failure_p': '0.0%', 'flow_change': 0, 'identifier': 0, 'others': 0,
        'flow_change_p': '0.0%', 'identifier_p': '0.0%', 'others_p': '0.0%',
        'infra_runs_new': 0, 'infra_runs_new_p': '0.0%', 'no_of_masked_infra_runs': 0,
        't_script_failure': 0, 't_script_failure_p': '0.0%', 'u1': 0, 'u1_p': '0.0%',
        'frame_failures': 0, 'frame_failures_p': '0.0%', 'unclassified_failure': 0,
        'unclassified_failure_p': '0.0%', 'dl_failures': 0, 'dl_failures_p': '0.0%',
        'p_c_name': 'N/A', 'occurence': 'N/A',
        'lob_breakdown': [], 'platform_breakdown': [],
        'feature_breakdown': [], 'test_method_breakdown': [],
        'bugs': [], 'total': 0, 'pages': 0, 'current_page': 1, 'per_page': 10
    }

def get_mta_releases_filter_options_optimized():
    """Get filter options for MTA releases data (optimized version)."""
    start_time = time.time()
    logger.info("Starting get_mta_releases_filter_options_optimized...")
    
    conn = get_db_connection()
    if not conn:
        return {
            'build_version': [], 'automation_lob': [], 'app_platform': [], 
            'result': [], 'bug_type': [], 'method_type': [], 'project_name':[], 'project_category_name':[], 'test_case':[],
            'scenarioname':[],'l1_category':[]
        }
    
    try:
        cursor = conn.cursor()
        filters = {}
        
        # Build versions
        cursor.execute("""
            SELECT DISTINCT build_version, COUNT(*) as cnt
            FROM terra_feature_data_optimized
            WHERE build_version IS NOT NULL
            GROUP BY build_version
            ORDER BY cnt DESC, build_version DESC LIMIT 30
        """)
        filters['build_version'] = cursor.fetchall()
        
        # LOBs
        cursor.execute("""
            SELECT DISTINCT automation_lob, COUNT(*) as cnt
            FROM terra_feature_data_optimized
            WHERE automation_lob IS NOT NULL
            GROUP BY automation_lob
            ORDER BY cnt DESC
        """)
        filters['automation_lob'] = cursor.fetchall()
        
        # App platforms
        cursor.execute("""
            SELECT DISTINCT app_platform, COUNT(*) as cnt
            FROM terra_feature_data_optimized
            WHERE app_platform IS NOT NULL
            GROUP BY app_platform
            ORDER BY cnt DESC
        """)
        filters['app_platform'] = cursor.fetchall()
        
        # Results
        cursor.execute("""
            SELECT DISTINCT result, COUNT(*) as cnt
            FROM terra_feature_data_optimized
            WHERE result IS NOT NULL
            GROUP BY result
            ORDER BY cnt DESC
        """)
        filters['result'] = cursor.fetchall()
        
        # Bug types
        cursor.execute("""
            SELECT DISTINCT bug_type, COUNT(*) as cnt
            FROM terra_feature_data_optimized
            WHERE bug_type IS NOT NULL AND bug_type != 'NA'
            GROUP BY bug_type
            ORDER BY cnt DESC
        """)
        filters['bug_type'] = cursor.fetchall()
        
        # Method types
        cursor.execute("""
            SELECT DISTINCT method_type, COUNT(*) as cnt
            FROM terra_feature_data_optimized
            WHERE method_type IS NOT NULL
            GROUP BY method_type
            ORDER BY cnt DESC
        """)
        filters['method_type'] = cursor.fetchall()

        ########################
        cursor.execute("""
            SELECT DISTINCT project_name, COUNT(*) as cnt
            FROM terra_feature_data_optimized
            WHERE project_name IS NOT NULL
            GROUP BY project_name
            ORDER BY cnt DESC
        """)
        filters['project_name'] = cursor.fetchall()


        cursor.execute("""
            SELECT DISTINCT project_category_name, COUNT(*) as cnt
            FROM terra_feature_data_optimized
            WHERE project_category_name IS NOT NULL
            GROUP BY project_category_name
            ORDER BY cnt DESC
        """)
        filters['project_category_name'] = cursor.fetchall()

        cursor.execute("""
            SELECT DISTINCT l1_category, COUNT(*) as cnt
            FROM terra_test_data_optimized
            WHERE l1_category IS NOT NULL
            GROUP BY l1_category
            ORDER BY cnt DESC
        """)
        filters['l1_category'] = cursor.fetchall()
        
        elapsed_time = time.time() - start_time
        logger.info(f"get_mta_releases_filter_options_optimized completed in {elapsed_time:.2f} seconds")
        return filters
        
    except Exception as e:
        logger.error(f"Error getting optimized MTA releases filter options: {str(e)}")
        return {
            'build_version': [], 'automation_lob': [], 'app_platform': [], 
            'result': [], 'bug_type': [], 'method_type': [],'project_name':[],'project_category_name':[], 'test_case':[],
            'scenarioname':[]
        }
    finally:
        conn.close()

def get_mta_releases_data_optimized(filters, page=1, per_page=50):
    """Get optimized MTA releases data with filtering and pagination."""
    start_time = time.time()
    logger.info(f"Starting get_mta_releases_data_optimized for page {page}...")
    
    conn = get_db_connection()
    if not conn:
        return {'data': [], 'total': 0, 'page': page, 'per_page': per_page}
    
    try:
        cursor = conn.cursor()
        
        # Build WHERE clause
        conditions = []
        params = []
        
        if filters.get('build_version'):
            conditions.append("build_version = ?")
            params.append(filters['build_version'])
        
        if filters.get('automation_lob'):
            conditions.append("automation_lob = ?")
            params.append(filters['automation_lob'])
        
        if filters.get('app_platform'):
            conditions.append("app_platform = ?")
            params.append(filters['app_platform'])
        
        if filters.get('result'):
            conditions.append("result = ?")
            params.append(filters['result'])
        
        if filters.get('bug_type'):
            conditions.append("bug_type = ?")
            params.append(filters['bug_type'])
        
        if filters.get('method_type'):
            conditions.append("method_type = ?")
            params.append(filters['method_type'])

        if filters.get('project_name'):
            conditions.append("project_name = ?")
            params.append(filters['project_name'])

        if filters.get('project_category_name'):
            conditions.append("project_category_name = ?")
            params.append(filters['project_category_name'])
            
        
        if filters.get('start_date') or filters.get('end_date'):
            try:
                if filters.get('start_date') and filters.get('end_date'):
                    conditions.append("exe_date BETWEEN ? AND ?")
                    params.extend([f"{filters['start_date']} 00:00:00", f"{filters['end_date']} 23:59:59"])
                elif filters.get('start_date'):
                    conditions.append("exe_date >= ?")
                    params.append(f"{filters['start_date']} 00:00:00")
                elif filters.get('end_date'):
                    conditions.append("exe_date <= ?")
                    params.append(f"{filters['end_date']} 23:59:59")
            except:
                pass
        
        if filters.get('search'):
            search_term = f"%{filters['search']}%"
            conditions.append("(scenarioname LIKE ? OR bug_id LIKE ? OR issue_key LIKE ?)")
            params.extend([search_term, search_term, search_term])
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # Get total count
        count_query = f"SELECT COUNT(*) FROM terra_feature_data_optimized WHERE {where_clause}"
        cursor.execute(count_query, params)
        total = cursor.fetchone()[0]
        
        # Get paginated data
        offset = (page - 1) * per_page
        data_query = f"""
            SELECT exe_date, build_version, scenarioname, automation_lob, app_platform,
                   result, bug_type, method_type, no_of_runs, no_of_passed_runs,
                   issue_key, status, priority, bug_creation_date,
                   no_of_infra_runs
            FROM terra_feature_data_optimized 
            WHERE {where_clause}
            ORDER BY exe_date DESC, build_version DESC
            LIMIT ? OFFSET ?
        """
        cursor.execute(data_query, params + [per_page, offset])
        
        # Convert to list of dictionaries
        columns = [desc[0] for desc in cursor.description]
        data = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        result = {
            'data': data,
            'total': total,
            'page': page,
            'per_page': per_page,
            'total_pages': (total + per_page - 1) // per_page
        }
        
        elapsed_time = time.time() - start_time
        logger.info(f"get_mta_releases_data_optimized completed in {elapsed_time:.2f} seconds")
        return result
        
    except Exception as e:
        logger.error(f"Error getting optimized MTA releases data: {str(e)}")
        return {'data': [], 'total': 0, 'page': page, 'per_page': per_page, 'total_pages': 0}
    finally:
        conn.close()

def get_last_refresh_log_optimized(refresh_type):
    """Get last refresh log entry for optimized database."""
    try:
        conn = get_db_connection()
        if not conn:
            return None
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT refresh_timestamp, rows_processed, status 
            FROM mta_refresh_log 
            WHERE refresh_type = ? 
            ORDER BY refresh_timestamp DESC LIMIT 1
        """, (refresh_type,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return {
                'timestamp': result[0],
                'rows': result[1],
                'status': result[2]
            }
        return None
        
    except Exception as e:
        logger.error(f"Error getting refresh log: {str(e)}")
        return None

# Routes for optimized dashboard





def query_features_based_on(filters=None):
    print("started")
    if filters is None:
        filters = {}

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    cursor = conn.cursor()

    # Use the reusable WHERE clause builder
    where_clause, params = build_where_clause_optimized(filters, target='feature')
    print("where .........",where_clause)
    print("params..........",params)

    query = f"""
        SELECT DISTINCT scenarioname
        FROM terra_feature_data_optimized
        {where_clause}
    """
    print("........",query)

    result = cursor.execute(query, params).fetchall()

    return [row[0] for row in result if row[0]]


def query_tests_based_on(filters=None):
    print("started")
    if filters is None:
        filters = {}

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    cursor = conn.cursor()

    # Use the reusable WHERE clause builder
    where_clause, params = build_where_clause_optimized(filters, target='test')
    print("where .........",where_clause)
    print("params..........",params)

    query = f"""
        SELECT DISTINCT test_case
        FROM terra_test_data_optimized
        {where_clause}
    """
    print("........",query)

    result = cursor.execute(query, params).fetchall()

    return [row[0] for row in result if row[0]]




@app.route('/')
def dashboard_optimized():
    """Optimized dashboard with CONCURRENT data loading using threads."""
    try:
        date_range = request.args.get('date_range') 
        # Get filters from request args (multi-select aware via getlist)
        filters = {
            'latest_release': request.args.getlist('latest_release') or request.args.get('latest_release'),
            'build_version': request.args.getlist('build_version') or request.args.get('build_version'),
            'lob': request.args.getlist('lob') or request.args.get('lob'),
            'app_platform': request.args.getlist('app_platform') or request.args.get('app_platform'),
            'os_version': request.args.getlist('os_version') or request.args.get('os_version'),
            'start_date': request.args.get('start_date'),
            'end_date': request.args.get('end_date'),
            'project_name': request.args.getlist('project_name') or request.args.get('project_name'),
            'project_category_name': request.args.getlist('project_category_name') or request.args.get('project_category_name'),
            'date_range' : date_range,
            'scenarioname': request.args.getlist('scenarioname') or request.args.get('scenarioname'),
            'test_case': request.args.getlist('test_case') or request.args.get('test_case'),
            'l1_category': request.args.getlist('l1_category') or request.args.get('l1_category')
        }
       
        if date_range:
            try:
                # start_date, end_date = date_range.split(" to ")
                # start_date = start_date.strip()
                # end_date = end_date.strip()
                # conditions.append("exe_date BETWEEN ? AND ?")
                # params.extend([start_date, end_date])
                start_date, end_date = date_range.split(" to ")
                filters['start_date'] = start_date.strip()
                filters['end_date'] = end_date.strip()
            except ValueError:
                pass

        test_filters = {
            
        }
         # 🔢 Pagination parameters (with fallback defaults)

        def safe_int(value, default=1):
            try:
                return int(value)
            except (ValueError, TypeError):
                return default

        feature_page = safe_int(request.args.get('feature_page'), 1)
        test_method_page = safe_int(request.args.get('test_method_page'), 1)
        per_page = safe_int(request.args.get('per_page'), 10) 
        bug_page = safe_int(request.args.get('bug_page'), 1)
        bug_per_page = safe_int(request.args.get('bug_per_page'), 10)

        


        # Remove None values
        filters = {k: v for k, v in filters.items() if v}
        
        # Ensure a default last-30-days window if no dates provided
        if not filters.get('start_date') and not filters.get('end_date'):
            today = datetime.utcnow().date()
            start_default = today - timedelta(days=30)
            filters['start_date'] = start_default.strftime('%Y-%m-%d')
            filters['end_date'] = today.strftime('%Y-%m-%d')

        # 🚀 ADVANCED CONCURRENT EXECUTION: Run data retrieval functions in parallel
        start_time = time.time()
        logger.info("🚀 Starting ADVANCED CONCURRENT dashboard data loading...")
        # Move feature/test option queries into the pool as they can be heavy
        # Default values for all data components
        results = {
            'filter_options': {'latest_release': [], 'build_version': [], 'lob': [], 'app_platform': [], 'os_version': [],
            'project_name':[],'project_category_name':[], 
            'scenarioname':[],'test_case':[],'l1_category':[]},
            'metrics': get_empty_dashboard_data(),
            'breakdowns': {'lob_breakdown': [], 'platform_breakdown': []},
            'feature_test_breakdowns': {'feature_breakdown': []},
            'test_breakdowns': {'test_method_breakdown': []},
            'bug_data': {'bugs': [], 'total': 0, 'pages': 0, 'current_page': 1, 'per_page': 10}
        }
        
        with ThreadPoolExecutor(max_workers=6) as thread_executor:
            # Submit all tasks with their identifiers
            futures = {
                thread_executor.submit(get_filter_options_optimized, filters): 'filter_options',
                thread_executor.submit(calculate_optimized_metrics, filters): 'metrics',
                thread_executor.submit(get_pass_rate_breakdowns_optimized, filters): 'breakdowns',
                # thread_executor.submit(get_feature_test_breakdowns_optimized, filters): 'feature_test_breakdowns',
                thread_executor.submit(
                    get_feature_breakdowns_optimized,
                    filters,
                    feature_page,
                   
                    per_page
                ): 'feature_test_breakdowns',
                thread_executor.submit(
                    get_test_breakdowns_optimized,
                    filters,
                    test_method_page,
                   
                    per_page
                ): 'test_breakdowns',
                thread_executor.submit(get_bug_level_data_optimized, filters, bug_page, bug_per_page): 'bug_data',
                thread_executor.submit(query_features_based_on, filters): 'scenarioname',
                thread_executor.submit(query_tests_based_on, filters): 'test_case'

            }
            
            # Process results as they complete (fastest wins!)
            completed_tasks = 0
            for future in as_completed(futures, timeout=30):
                task_name = futures[future]
                try:
                    result = future.result()
                    results[task_name] = result
                    completed_tasks += 1
                    elapsed = time.time() - start_time
                    logger.info(f"✅ {task_name} completed in {elapsed:.2f}s ({completed_tasks}/7 tasks done)")
                except Exception as e:
                    logger.error(f"❌ {task_name} failed: {str(e)}")
            
            # Extract results
            filter_options = results['filter_options']
            metrics = results['metrics']
            breakdowns = results['breakdowns']
            feature_test_breakdowns = results['feature_test_breakdowns']
            test_breakdowns=results['test_breakdowns']
            bug_data = results['bug_data']
            scenarioname = results.get('scenarioname', [])
            test_case = results.get('test_case', [])
        
        total_time = time.time() - start_time
        logger.info(f"🎯 CONCURRENT dashboard loading completed in {total_time:.2f} seconds")
        
        # Get sync status for UI
         # Get sync status for UI
        sync_status_data = get_sync_status()
        is_syncing = is_sync_in_progress()

        # Helper function to parse datetime
        def parse_datetime(dt):
            """Parse datetime from string or return as is if already datetime."""
            if isinstance(dt, datetime):
                return dt
            if isinstance(dt, str):
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                    try:
                        return datetime.strptime(dt, fmt)
                    except ValueError:
                        continue
            return None

        # Get most recent timestamp
        timestamp = None
        if sync_status_data:
            if isinstance(sync_status_data, list):
                most_recent = max(sync_status_data, key=lambda x: parse_datetime(x['last_updated']) or datetime.min)
                last_updated_val = most_recent.get('last_updated')
                dt_obj = parse_datetime(last_updated_val)
                if dt_obj:
                    timestamp = dt_obj.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(sync_status_data, dict):
                last_updated_val = sync_status_data.get('last_updated')
                dt_obj = parse_datetime(last_updated_val)
                if dt_obj:
                    timestamp = dt_obj.strftime('%Y-%m-%d %H:%M:%S')

        # Combine all data for template
        template_data = {
            **filter_options, 
            **metrics, 
            **breakdowns, 
            **feature_test_breakdowns, 
            **test_breakdowns,
            **bug_data,
            'sync_status': sync_status_data,
            'timestamp': timestamp,
            'is_sync_in_progress': is_syncing,
            'feature_current_page': feature_page,
            'test_method_current_page': test_method_page,
            'bug_current_page': bug_page,
            'bug_per_page': bug_per_page,
            'scenarioname': scenarioname
        }

        # Check if this is an AJAX request
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return render_template('dash_recent.html', **template_data)

        # Return full page for regular requests
        return render_template('dash_recent.html', **template_data)

    except Exception as e:
        logger.error(f"Error in optimized dashboard route: {str(e)}")
        # Return empty data if error
        empty_data = get_empty_dashboard_data()
        return render_template('dash_recent.html', **empty_data)

@app.route('/dashboard')
def dashboard():
    """Dashboard route (compatibility with template)."""
    return dashboard_optimized()

@app.route('/mta_releases')
def mta_releases():
    """MTA releases route with CONCURRENT data loading."""
    try:
        # Get filters from request args
        filters = {
            'build_version': request.args.get('build_version'),
            'automation_lob': request.args.get('automation_lob'),
            'app_platform': request.args.get('app_platform'),
            'result': request.args.get('result'),
            'bug_type': request.args.get('bug_type'),
            'method_type': request.args.get('method_type'),
            'start_date': request.args.get('start_date'),
            'end_date': request.args.get('end_date'),
            'search': request.args.get('search')
        }
        
        # Remove None values
        filters = {k: v for k, v in filters.items() if v}
        
        # Get pagination parameters
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        
        # 🚀 CONCURRENT EXECUTION for MTA releases
        start_time = time.time()
        logger.info("🚀 Starting CONCURRENT MTA releases data loading...")
        
        with ThreadPoolExecutor(max_workers=3) as thread_executor:
            # Submit tasks concurrently
            future_filter_options = thread_executor.submit(get_mta_releases_filter_options_optimized)
            future_data = thread_executor.submit(get_mta_releases_data_optimized, filters, page, per_page)
            future_refresh_log = thread_executor.submit(get_last_refresh_log_optimized, 'mta_releases')
            
            # Collect results
            try:
                filter_options = future_filter_options.result(timeout=20)
                logger.info("✅ MTA filter options completed")
            except Exception as e:
                logger.error(f"❌ MTA filter options failed: {str(e)}")
                filter_options = {'build_version': [], 'automation_lob': [], 'app_platform': [], 'result': [], 'bug_type': [], 'method_type': []}
            
            try:
                data_result = future_data.result(timeout=30)
                logger.info("✅ MTA data retrieval completed")
            except Exception as e:
                logger.error(f"❌ MTA data retrieval failed: {str(e)}")
                data_result = {'data': [], 'total': 0, 'page': page, 'per_page': per_page, 'total_pages': 0}
            
            try:
                last_refresh = future_refresh_log.result(timeout=10)
                logger.info("✅ MTA refresh log completed")
            except Exception as e:
                logger.error(f"❌ MTA refresh log failed: {str(e)}")
                last_refresh = None
        
        total_time = time.time() - start_time
        logger.info(f"🎯 CONCURRENT MTA releases loading completed in {total_time:.2f} seconds")
        
        # Combine all data for template
        template_data = {
            **filter_options,
            **data_result,
            'filters': filters,
            'last_refresh': last_refresh
        }
        
        return render_template('mta_releases.html', **template_data)
        
    except Exception as e:
        logger.error(f"Error in optimized MTA releases route: {str(e)}")
        # Return empty data if error
        return render_template('mta_releases.html', 
                             build_version=[], automation_lob=[], app_platform=[], 
                             result=[], bug_type=[], method_type=[],
                             data=[], total=0, page=1, per_page=50, total_pages=0)

@app.route('/api/metrics')
def api_metrics():
    """Main API endpoint for metrics calculation (optimized version)."""
    try:
        filters = {
            'date_from': request.args.get('date_from'),
            'date_to': request.args.get('date_to'),
            'automation_lob': request.args.get('automation_lob', 'All'),
            'app_platform': request.args.get('app_platform', 'All'),
            'bug_type': request.args.get('bug_type', 'All')
        }
        
        metrics = calculate_optimized_metrics(filters)
        return jsonify(metrics)
    
    except Exception as e:
        logger.error(f"Error in metrics API: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/metrics_optimized')
def api_metrics_optimized():
    """API endpoint for optimized metrics calculation (alternative route)."""
    return api_metrics()

@app.route('/api/filter_options')
def api_filter_options():
    """Get filter options from optimized database (main route)."""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor()
        
        # Get unique values for filters (optimized queries)
        options = {}
        
        # Automation LOBs
        cursor.execute("SELECT DISTINCT automation_lob FROM terra_test_data_optimized WHERE automation_lob IS NOT NULL ORDER BY automation_lob")
        options['automation_lobs'] = [row[0] for row in cursor.fetchall()]
        
        # App Platforms
        cursor.execute("SELECT DISTINCT app_platform FROM terra_test_data_optimized WHERE app_platform IS NOT NULL ORDER BY app_platform")
        options['app_platforms'] = [row[0] for row in cursor.fetchall()]
        
        # Bug Types
        cursor.execute("SELECT DISTINCT bug_type FROM terra_test_data_optimized WHERE bug_type IS NOT NULL ORDER BY bug_type")
        options['bug_types'] = [row[0] for row in cursor.fetchall()]
        
        # Date range
        cursor.execute("SELECT MIN(exe_date), MAX(exe_date) FROM terra_test_data_optimized")
        date_range = cursor.fetchone()
        options['date_range'] = {
            'min_date': date_range[0],
            'max_date': date_range[1]
        }
        
        conn.close()
        return jsonify(options)
        
    except Exception as e:
        logger.error(f"Error getting filter options: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/filter_options_optimized')
def api_filter_options_optimized():
    """Get filter options from optimized database (alternative route)."""
    return api_filter_options()

def run_manual_sync():
    logger.info("Manual sync triggered...")

    test_success = sync_test_data_optimized()
    feature_success = sync_feature_data_optimized()

    return test_success, feature_success


@app.route('/sync')
def sync():
    """Manual sync trigger for optimized database (main route)."""
    try:
        test_success, feature_success = run_manual_sync()

        if test_success and feature_success:
            return jsonify({
                'status': 'success',
                'message': 'Optimized data sync completed successfully'
            })
        else:
            return jsonify({
                'status': 'partial',
                'message': 'Some optimized data syncs failed'
            }), 207

    except Exception as e:
        logger.error(f"Error in manual sync: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/sync_optimized')
def sync_optimized():
    """Manual sync trigger for optimized database (alternative route)."""
    return sync()

@app.route('/status')
def status():
    """Check optimized database status and row counts (main status route)."""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"status": "error", "message": "Optimized database connection failed"}), 500
        
        cursor = conn.cursor()
        
        # Get status of optimized tables
        cursor.execute("""
            SELECT 
                'test_optimized' as table_type,
                COUNT(*) as row_count,
                MAX(exe_date) as latest_date,
                MAX(last_updated) as last_updated
            FROM terra_test_data_optimized
            
            UNION ALL
            
            SELECT 
                'feature_optimized' as table_type,
                COUNT(*) as row_count,
                MAX(exe_date) as latest_date,
                MAX(last_updated) as last_updated
            FROM terra_feature_data_optimized
        """)
        
        results = cursor.fetchall()
        conn.close()
        
        status_data = {}
        for row in results:
            status_data[row[0]] = {
                'row_count': row[1],
                'latest_date': row[2],
                'last_updated': row[3]
            }
        
        return jsonify({
            'status': 'success',
            'database': 'optimized',
            'tables': status_data,
            'optimization': '53% size reduction (18 vs 38 columns)'
        })
        
    except Exception as e:
        logger.error(f"Error checking optimized database status: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/status_optimized')
def status_optimized():
    """Check optimized database status and row counts (alternative route)."""
    return status()

# Scheduled jobs for optimized data sync
@scheduler.task('cron', id='sync_optimized_data', minute=0)  # Run every hour at minute 0
def scheduled_sync_optimized():
    """Scheduled sync for optimized database (every hour)."""
    start_time = time.time()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    logger.info("=" * 60)
    logger.info(f"🔄 SCHEDULED SYNC STARTED at {current_time}")
    logger.info(f"📋 Sync Type: Hourly Automated Sync")
    logger.info(f"🎯 Target: Optimized Database")
    logger.info("=" * 60)
    
    try:
        # Sync test data
        logger.info("🧪 Phase 1: Starting test data sync...")
        test_start_time = time.time()
        test_success = sync_test_data_optimized()
        test_duration = time.time() - test_start_time
        
        if test_success:
            logger.info(f"✅ Phase 1: Test data sync completed successfully in {test_duration:.2f}s")
        else:
            logger.error(f"❌ Phase 1: Test data sync failed after {test_duration:.2f}s")
        
        # Sync feature data
        logger.info("🎨 Phase 2: Starting feature data sync...")
        feature_start_time = time.time()
        feature_success = sync_feature_data_optimized()
        feature_duration = time.time() - feature_start_time
        
        if feature_success:
            logger.info(f"✅ Phase 2: Feature data sync completed successfully in {feature_duration:.2f}s")
        else:
            logger.error(f"❌ Phase 2: Feature data sync failed after {feature_duration:.2f}s")
        
        # Summary
        total_duration = time.time() - start_time
        success_count = sum([test_success, feature_success])
        
        logger.info("=" * 60)
        logger.info(f"📊 SCHEDULED SYNC SUMMARY")
        logger.info(f"⏱️  Total Duration: {total_duration:.2f} seconds")
        logger.info(f"✅ Successful Syncs: {success_count}/2")
        logger.info(f"❌ Failed Syncs: {2 - success_count}/2")
        logger.info(f"🏁 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if success_count == 2:
            logger.info("🎉 All syncs completed successfully!")
        elif success_count == 1:
            logger.warning("⚠️  Partial sync completed - some data may be stale")
        else:
            logger.error("🚨 All syncs failed - data may be significantly outdated")
        
        logger.info("=" * 60)
        
        # Next sync info
        next_sync = datetime.now() + timedelta(hours=1)
        logger.info(f"⏰ Next scheduled sync: {next_sync.strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        total_duration = time.time() - start_time
        logger.error("=" * 60)
        logger.error(f"🚨 CRITICAL ERROR in scheduled sync after {total_duration:.2f}s")
        logger.error(f"❌ Error: {str(e)}")
        logger.error(f"💥 Exception Type: {type(e).__name__}")
        logger.error("=" * 60)
        raise

# Initialize optimized database on startup
def init_optimized_db():
    """Initialize optimized database with reduced schema and performance indexes."""
    logger.info("Initializing optimized database...")
    try:
        success = create_optimized_tables(DB_PATH_OPTIMIZED)
        if success:
            logger.info("✅ Optimized database initialized successfully")
            
            # Add performance indexes
            add_performance_indexes()
        else:
            logger.error("❌ Failed to initialize optimized database")
    except Exception as e:
        logger.error(f"❌ Error initializing optimized database: {str(e)}")

def add_performance_indexes():
    """Add indexes to improve query performance."""
    logger.info("Adding performance indexes...")
    try:
        conn = get_db_connection(use_row_factory=False)
        if not conn:
            logger.warning("Cannot add indexes - no database connection")
            return
        
        cursor = conn.cursor()
        
        # Performance indexes for commonly queried columns
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_test_build_version ON terra_test_data_optimized(build_version)",
            "CREATE INDEX IF NOT EXISTS idx_test_automation_lob ON terra_test_data_optimized(automation_lob)", 
            "CREATE INDEX IF NOT EXISTS idx_test_app_platform ON terra_test_data_optimized(app_platform)",
            "CREATE INDEX IF NOT EXISTS idx_test_os_version ON terra_test_data_optimized(os_version)",
            "CREATE INDEX IF NOT EXISTS idx_test_exe_date ON terra_test_data_optimized(exe_date)",
            "CREATE INDEX IF NOT EXISTS idx_test_bug_id ON terra_test_data_optimized(bug_id)",
            "CREATE INDEX IF NOT EXISTS idx_test_runs ON terra_test_data_optimized(no_of_runs)",
            
            "CREATE INDEX IF NOT EXISTS idx_feature_build_version ON terra_feature_data_optimized(build_version)",
            "CREATE INDEX IF NOT EXISTS idx_feature_automation_lob ON terra_feature_data_optimized(automation_lob)",
            "CREATE INDEX IF NOT EXISTS idx_feature_app_platform ON terra_feature_data_optimized(app_platform)",
            "CREATE INDEX IF NOT EXISTS idx_feature_exe_date ON terra_feature_data_optimized(exe_date)",
            "CREATE INDEX IF NOT EXISTS idx_feature_scenario ON terra_feature_data_optimized(scenarioname)",
            
            # Composite indexes for common filter combinations
            "CREATE INDEX IF NOT EXISTS idx_test_composite ON terra_test_data_optimized(build_version, automation_lob, app_platform)",
            "CREATE INDEX IF NOT EXISTS idx_feature_composite ON terra_feature_data_optimized(build_version, automation_lob, app_platform)"
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
            
        conn.commit()
        conn.close()
        logger.info("✅ Performance indexes added successfully")
        
    except Exception as e:
        logger.error(f"❌ Error adding performance indexes: {str(e)}")

@app.route('/api/bug_data')
def api_bug_data():
    """API endpoint for bug level data with filtering."""
    try:
        # Get filters from request args
        filters = {
            
            'build_version': request.args.get('build_version'),
            
            'project_category': request.args.get('project_category'),
            'project_category_name': request.args.get('project_category_name')
            

        }
        
        # Remove None values
        filters = {k: v for k, v in filters.items() if v}
        
        # Get pagination parameters
        # page = int(request.args.get('page', 1))
        page = request.args.get("page", default=1, type=int)
        per_page = int(request.args.get('per_page', 10))
        
        # Get bug data
        bug_data = get_bug_level_data_optimized(filters, page, per_page)
        
        return jsonify(bug_data)
        
    except Exception as e:
        logger.error(f"Error in api_bug_data: {str(e)}")
        return jsonify({'bugs': [], 'total': 0, 'pages': 0, 'current_page': 1, 'per_page': 10}), 500

@app.route('/api/sync_status')
def api_sync_status():
    """API endpoint to get current sync status."""
    try:
        sync_type = request.args.get('sync_type')  # Optional: filter by specific sync type
        
        if sync_type:
            status = get_sync_status(sync_type)
        else:
            status = get_sync_status()  # Get all sync statuses
        
        # Also include overall sync progress flag
        in_progress = is_sync_in_progress()
        
        return jsonify({
            'sync_status': status,
            'is_sync_in_progress': in_progress,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
    except Exception as e:
        logger.error(f"Error in api_sync_status: {str(e)}")
        return jsonify({
            'sync_status': None,
            'is_sync_in_progress': False,
            'error': str(e)
        }), 500


# scheduled_sync_optimized()
if __name__ == '__main__':
    import sys

    with app.app_context():
        if len(sys.argv) > 1 and sys.argv[1] == "sync":
            # ✅ Run manual sync only
            test_success, feature_success = run_manual_sync()
            if test_success and feature_success:
                print("✅ Sync successful.")
            else:
                print("⚠️ Some parts of sync failed.")
        else:
            # ✅ Default behavior: init, sync, start scheduler, run app
            init_optimized_db()
            sync_optimized()
            scheduler.start()
            app.run(host='0.0.0.0', port=5005)




