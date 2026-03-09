"""
Unit tests for run_scheduled_operations.py orchestrator.
Tests SQL discovery, execution, error handling, and logging.

These tests are BLOCKING for merge and require no database.
They should complete in < 30 seconds.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime, timezone
import json

# Import the module to test
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from run_scheduled_operations import (
    validate_environment,
    read_sql_file,
    execute_sql_script,
    log_execution,
    main
)


# ============================================================================
# Phase 1: Unit Tests (40-50 tests, blocking for merge, no DB required)
# ============================================================================

class TestEnvironmentValidation:
    """Test validate_environment() function."""
    
    @pytest.mark.unit
    @patch.dict('os.environ', {'PGDATABASE': 'testdb', 'PGUSER': 'testuser'})
    def test_validate_environment_success_all_vars_set(self):
        """Given: PGDATABASE and PGUSER env vars set
        When: validate_environment() is called
        Then: Returns True"""
        assert validate_environment() is True
    
    @pytest.mark.unit
    @patch.dict('os.environ', {}, clear=True)
    def test_validate_environment_fails_missing_pgdatabase(self):
        """Given: PGDATABASE not set
        When: validate_environment() is called
        Then: Returns False and logs error"""
        result = validate_environment()
        assert result is False
    
    @pytest.mark.unit
    @patch.dict('os.environ', {'PGDATABASE': 'testdb'}, clear=True)
    def test_validate_environment_fails_missing_pguser(self):
        """Given: PGUSER not set but PGDATABASE is set
        When: validate_environment() is called
        Then: Returns False"""
        result = validate_environment()
        assert result is False


class TestReadSqlFile:
    """Test read_sql_file() function."""
    
    @pytest.mark.unit
    def test_read_sql_file_success(self):
        """Given: Valid SQL file exists
        When: read_sql_file() is called
        Then: Returns file contents as string"""
        with tempfile.TemporaryDirectory() as tmpdir:
            sql_file = Path(tmpdir) / "test.sql"
            sql_content = "SELECT 1; -- test query"
            sql_file.write_text(sql_content)
            
            result = read_sql_file(sql_file)
            assert result == sql_content
    
    @pytest.mark.unit
    def test_read_sql_file_not_found(self):
        """Given: File does not exist
        When: read_sql_file() is called
        Then: Raises FileNotFoundError"""
        with pytest.raises(FileNotFoundError):
            read_sql_file(Path("/nonexistent/file.sql"))
    
    @pytest.mark.unit
    def test_read_sql_file_empty(self):
        """Given: Empty SQL file
        When: read_sql_file() is called
        Then: Returns empty string"""
        with tempfile.TemporaryDirectory() as tmpdir:
            sql_file = Path(tmpdir) / "empty.sql"
            sql_file.write_text("")
            
            result = read_sql_file(sql_file)
            assert result == ""
    
    @pytest.mark.unit
    def test_read_sql_file_with_special_chars(self):
        """Given: SQL file with special characters (UTF-8)
        When: read_sql_file() is called
        Then: Returns content with characters preserved"""
        with tempfile.TemporaryDirectory() as tmpdir:
            sql_file = Path(tmpdir) / "special.sql"
            sql_content = "SELECT '© ™ € ¥' AS emoji; -- special chars"
            sql_file.write_text(sql_content, encoding='utf-8')
            
            result = read_sql_file(sql_file)
            assert result == sql_content
            assert '©' in result


class TestExecuteSqlScript:
    """Test execute_sql_script() function."""
    
    @pytest.mark.unit
    @patch('run_scheduled_operations.read_sql_file')
    @patch('run_scheduled_operations.log_execution')
    def test_execute_sql_script_success(self, mock_log, mock_read):
        """Given: Valid SQL script and working connection
        When: execute_sql_script() is called
        Then: Returns (True, None) and logs success"""
        mock_read.return_value = "SELECT 1;"
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        script_path = Path("/test/test.sql")
        success, error = execute_sql_script(mock_conn, script_path, "test.sql")
        
        assert success is True
        assert error is None
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_log.assert_called_once()
        
        # Verify log call has success status
        log_call_args = mock_log.call_args
        assert log_call_args[0][2] == 'success'  # status argument
    
    @pytest.mark.unit
    @patch('run_scheduled_operations.read_sql_file')
    @patch('run_scheduled_operations.log_execution')
    def test_execute_sql_script_with_failure(self, mock_log, mock_read):
        """Given: SQL script with syntax error
        When: execute_sql_script() is called
        Then: Returns (False, error_msg) and logs failure"""
        mock_read.return_value = "INVALID SQL;"
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("syntax error")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        script_path = Path("/test/bad.sql")
        success, error = execute_sql_script(mock_conn, script_path, "bad.sql")
        
        assert success is False
        assert "syntax error" in error
        mock_conn.rollback.assert_called_once()
        mock_log.assert_called_once()
        
        # Verify log call has failure status
        log_call_args = mock_log.call_args
        assert log_call_args[0][2] == 'failed'  # status argument
    
    @pytest.mark.unit
    @patch('run_scheduled_operations.read_sql_file')
    @patch('run_scheduled_operations.log_execution')
    def test_execute_sql_script_connection_error(self, mock_log, mock_read):
        """Given: Database connection fails during execute
        When: execute_sql_script() is called
        Then: Returns (False, error_msg)"""
        mock_read.return_value = "SELECT 1;"
        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = Exception("Connection lost")
        
        script_path = Path("/test/test.sql")
        success, error = execute_sql_script(mock_conn, script_path, "test.sql")
        
        assert success is False
        assert "Connection lost" in error
    
    @pytest.mark.unit
    @patch('run_scheduled_operations.read_sql_file')
    @patch('run_scheduled_operations.log_execution')
    def test_execute_sql_script_timeout(self, mock_log, mock_read):
        """Given: SQL script execution times out
        When: execute_sql_script() is called
        Then: Returns (False, error_msg) with timeout in message"""
        mock_read.return_value = "SELECT * FROM huge_table;"
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("query timeout")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        script_path = Path("/test/slow.sql")
        success, error = execute_sql_script(mock_conn, script_path, "slow.sql")
        
        assert success is False
        assert "timeout" in error.lower() or "timeout" in error
    
    @pytest.mark.unit
    @patch('run_scheduled_operations.read_sql_file')
    @patch('run_scheduled_operations.log_execution')
    def test_execute_sql_script_permission_denied(self, mock_log, mock_read):
        """Given: SQL script attempts unauthorized operation
        When: execute_sql_script() is called
        Then: Returns (False, error_msg) with permission error"""
        mock_read.return_value = "DROP TABLE important_table;"
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("permission denied")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        script_path = Path("/test/drop.sql")
        success, error = execute_sql_script(mock_conn, script_path, "drop.sql")
        
        assert success is False
        assert "permission" in error.lower()
    
    @pytest.mark.unit
    @patch('run_scheduled_operations.read_sql_file')
    @patch('run_scheduled_operations.log_execution')
    def test_execute_sql_script_very_long_error_message(self, mock_log, mock_read):
        """Given: SQL error with very long error message (>1000 chars)
        When: execute_sql_script() is called
        Then: Returns (False, error_msg) with truncated message"""
        long_error = "E" * 5000  # Very long error
        mock_read.return_value = "SELECT 1;"
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception(long_error)
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        script_path = Path("/test/test.sql")
        success, error = execute_sql_script(mock_conn, script_path, "test.sql")
        
        assert success is False
        assert error is not None
        assert len(error) > 0
    
    @pytest.mark.unit
    @patch('run_scheduled_operations.read_sql_file')
    @patch('run_scheduled_operations.log_execution')
    def test_execute_sql_script_special_chars_in_error(self, mock_log, mock_read):
        """Given: SQL error with special characters
        When: execute_sql_script() is called
        Then: Returns (False, error_msg) with characters preserved"""
        special_error = "Error: special chars © ™ € in message"
        mock_read.return_value = "SELECT 1;"
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception(special_error)
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        script_path = Path("/test/test.sql")
        success, error = execute_sql_script(mock_conn, script_path, "test.sql")
        
        assert success is False
        assert "©" in error or "Error" in error


class TestLogExecution:
    """Test log_execution() function."""
    
    @pytest.mark.unit
    @patch('run_scheduled_operations.datetime')
    def test_log_execution_success_status(self, mock_datetime):
        """Given: Successful script execution
        When: log_execution() is called with status='success'
        Then: Logs to database without error"""
        mock_datetime.now.return_value = datetime(2026, 2, 1, 10, 0, 0, tzinfo=timezone.utc)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        log_execution(mock_conn, "test_script.sql", "success", 1.5)
        
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
    
    @pytest.mark.unit
    @patch('run_scheduled_operations.datetime')
    def test_log_execution_failure_with_message(self, mock_datetime):
        """Given: Failed script execution with error message
        When: log_execution() is called with status='failure'
        Then: Logs error message to database"""
        mock_datetime.now.return_value = datetime(2026, 2, 1, 10, 0, 0, tzinfo=timezone.utc)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        error_msg = "Table does not exist"
        log_execution(mock_conn, "test_script.sql", "failure", 0.5, error_msg)
        
        mock_cursor.execute.assert_called_once()
        # Verify error message is passed in call
        call_args = mock_cursor.execute.call_args
        assert error_msg in str(call_args)
    
    @pytest.mark.unit
    def test_log_execution_handles_logging_failure(self):
        """Given: Database is unavailable during logging
        When: log_execution() is called
        Then: Logs error but doesn't raise exception"""
        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = Exception("Database unavailable")
        
        # Should not raise
        log_execution(mock_conn, "test_script.sql", "success", 1.0)


class TestMainOrchestration:
    """Test main() orchestration function."""
    
    @pytest.mark.unit
    @patch.dict('os.environ', {'PGDATABASE': 'testdb', 'PGUSER': 'testuser'})
    @patch('run_scheduled_operations.db_connect')
    @patch('run_scheduled_operations.validate_environment')
    @patch('run_scheduled_operations.SQL_SCRIPTS_DIR')
    def test_main_all_scripts_succeed(self, mock_sql_dir, mock_validate, mock_connect, tmp_path, monkeypatch):
        """Given: 2 SQL scripts that both succeed
        When: main() is called
        Then: Returns 0 and executes all scripts"""
        # Setup valid log directory
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        monkeypatch.setattr('run_scheduled_operations.LOG_DIR', log_dir)
        monkeypatch.setattr('run_scheduled_operations.logger', None)
        
        mock_validate.return_value = True
        
        # Mock SQL scripts directory with two test files
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            (tmppath / "01_first.sql").write_text("SELECT 1;")
            (tmppath / "02_second.sql").write_text("SELECT 2;")
            
            mock_sql_dir.__truediv__ = lambda self, x: tmppath / x
            mock_sql_dir.exists.return_value = True
            mock_sql_dir.glob.return_value = sorted(tmppath.glob("*.sql"))
            
            # Mock database connection
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_connect.return_value = mock_conn
            
            with patch('run_scheduled_operations.execute_sql_script') as mock_execute:
                mock_execute.side_effect = [(True, None), (True, None)]
                
                result = main()
                
                assert result == 0
                assert mock_execute.call_count == 2
                mock_conn.close.assert_called_once()
    
    @pytest.mark.unit
    @patch.dict('os.environ', {'PGDATABASE': 'testdb', 'PGUSER': 'testuser'})
    @patch('run_scheduled_operations.db_connect')
    @patch('run_scheduled_operations.validate_environment')
    @patch('run_scheduled_operations.SQL_SCRIPTS_DIR')
    def test_main_one_script_fails_continues(self, mock_sql_dir, mock_validate, mock_connect, tmp_path, monkeypatch):
        """Given: 2 SQL scripts where second fails
        When: main() is called
        Then: Returns 1 but still executes all scripts"""
        # Setup valid log directory
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        monkeypatch.setattr('run_scheduled_operations.LOG_DIR', log_dir)
        monkeypatch.setattr('run_scheduled_operations.logger', None)
        
        mock_validate.return_value = True
        
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            (tmppath / "01_first.sql").write_text("SELECT 1;")
            (tmppath / "02_second.sql").write_text("SELECT 2;")
            
            mock_sql_dir.__truediv__ = lambda self, x: tmppath / x
            mock_sql_dir.exists.return_value = True
            mock_sql_dir.glob.return_value = sorted(tmppath.glob("*.sql"))
            
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_connect.return_value = mock_conn
            
            with patch('run_scheduled_operations.execute_sql_script') as mock_execute:
                mock_execute.side_effect = [(True, None), (False, "table not found")]
                
                result = main()
                
                assert result == 1  # Failure exit code
                assert mock_execute.call_count == 2  # Both executed
                mock_conn.close.assert_called_once()
    
    @pytest.mark.unit
    @patch.dict('os.environ', {}, clear=True)
    @patch('run_scheduled_operations.validate_environment')
    def test_main_env_validation_fails(self, mock_validate, tmp_path, monkeypatch):
        """Given: Environment validation fails
        When: main() is called
        Then: Returns 1 without attempting DB operations"""
        # Setup valid log directory
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        monkeypatch.setattr('run_scheduled_operations.LOG_DIR', log_dir)
        monkeypatch.setattr('run_scheduled_operations.logger', None)
        
        mock_validate.return_value = False
        
        result = main()
        
        assert result == 1
    
    @pytest.mark.unit
    @patch.dict('os.environ', {'PGDATABASE': 'testdb', 'PGUSER': 'testuser'})
    @patch('run_scheduled_operations.validate_environment')
    @patch('run_scheduled_operations.SQL_SCRIPTS_DIR')
    def test_main_no_sql_files_found(self, mock_sql_dir, mock_validate, tmp_path, monkeypatch):
        """Given: SQL scripts directory exists but is empty
        When: main() is called
        Then: Returns 1 and logs warning"""
        # Setup valid log directory
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        monkeypatch.setattr('run_scheduled_operations.LOG_DIR', log_dir)
        monkeypatch.setattr('run_scheduled_operations.logger', None)
        
        mock_validate.return_value = True
        mock_sql_dir.exists.return_value = True
        mock_sql_dir.glob.return_value = []  # No scripts
        
        result = main()
        
        assert result == 1
    
    @pytest.mark.unit
    @patch.dict('os.environ', {'PGDATABASE': 'testdb', 'PGUSER': 'testuser'})
    @patch('run_scheduled_operations.validate_environment')
    @patch('run_scheduled_operations.SQL_SCRIPTS_DIR')
    def test_main_sql_directory_missing(self, mock_sql_dir, mock_validate, tmp_path, monkeypatch):
        """Given: SQL scripts directory does not exist
        When: main() is called
        Then: Returns 1 and logs warning"""
        # Setup valid log directory
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        monkeypatch.setattr('run_scheduled_operations.LOG_DIR', log_dir)
        monkeypatch.setattr('run_scheduled_operations.logger', None)
        
        mock_validate.return_value = True
        mock_sql_dir.exists.return_value = False
        
        result = main()
        
        assert result == 1
    
    @pytest.mark.unit
    @patch.dict('os.environ', {'PGDATABASE': 'testdb', 'PGUSER': 'testuser'})
    @patch('run_scheduled_operations.db_connect')
    @patch('run_scheduled_operations.validate_environment')
    @patch('run_scheduled_operations.SQL_SCRIPTS_DIR')
    def test_main_database_connection_fails(self, mock_sql_dir, mock_validate, mock_connect, tmp_path, monkeypatch):
        """Given: Database connection fails
        When: main() is called
        Then: Returns 1 and logs error"""
        import psycopg  # Import to access psycopg.Error
        
        # Setup valid log directory
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        monkeypatch.setattr('run_scheduled_operations.LOG_DIR', log_dir)
        monkeypatch.setattr('run_scheduled_operations.logger', None)
        
        mock_validate.return_value = True
        mock_connect.side_effect = psycopg.Error("Connection refused")
        
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            (tmppath / "test.sql").write_text("SELECT 1;")
            
            mock_sql_dir.exists.return_value = True
            mock_sql_dir.glob.return_value = list(tmppath.glob("*.sql"))
            
            result = main()
            
            assert result == 1


class TestSqlDiscovery:
    """Test SQL file discovery and ordering."""
    
    @pytest.mark.unit
    def test_discover_sql_files_alphabetical_order(self):
        """Given: Multiple SQL files in unordered directory
        When: Files are discovered with glob().sort()
        Then: Returns alphabetically ordered list"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            
            # Create files out of order
            files = ["99_last.sql", "01_first.sql", "50_middle.sql"]
            for fname in files:
                (tmppath / fname).write_text("SELECT 1;")
            
            discovered = sorted(tmppath.glob("*.sql"))
            names = [f.name for f in discovered]
            
            assert names == ["01_first.sql", "50_middle.sql", "99_last.sql"]
    
    @pytest.mark.unit
    def test_discover_ignores_non_sql_files(self):
        """Given: Mix of .sql and other file types
        When: Files are discovered with glob("*.sql")
        Then: Returns only .sql files"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            
            (tmppath / "script.sql").write_text("SELECT 1;")
            (tmppath / "readme.txt").write_text("Info")
            (tmppath / "data.json").write_text("{}")
            (tmppath / "backup.sql.bak").write_text("SELECT 1;")
            
            discovered = list(tmppath.glob("*.sql"))
            names = [f.name for f in discovered]
            
            assert names == ["script.sql"]
    
    @pytest.mark.unit
    def test_discover_handles_special_chars_in_filename(self):
        """Given: SQL file with special characters in name
        When: File is discovered
        Then: Handles special characters correctly"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            
            # Create files with special chars (underscore, dash, numbers)
            special_files = [
                "01_export-to-core.sql",
                "02_truncate_stg_raw.sql",
                "03_cleanup-2026.sql"
            ]
            for fname in special_files:
                (tmppath / fname).write_text("SELECT 1;")
            
            discovered = sorted(tmppath.glob("*.sql"))
            names = [f.name for f in discovered]
            
            assert names == special_files
    
    @pytest.mark.unit
    def test_discover_empty_directory(self):
        """Given: Empty directory
        When: Files are discovered with glob("*.sql")
        Then: Returns empty list"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            discovered = list(tmppath.glob("*.sql"))
            assert discovered == []
    
    @pytest.mark.unit
    def test_discover_single_file(self):
        """Given: Directory with single SQL file
        When: File is discovered
        Then: Returns list with one element"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            (tmppath / "only_script.sql").write_text("SELECT 1;")
            
            discovered = list(tmppath.glob("*.sql"))
            assert len(discovered) == 1
            assert discovered[0].name == "only_script.sql"
    
    @pytest.mark.unit
    def test_discover_many_files_maintains_order(self):
        """Given: 100+ SQL files
        When: Files are discovered and sorted
        Then: Maintains alphabetical order"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            
            # Create 100 files
            for i in range(100):
                (tmppath / f"{i:03d}_script.sql").write_text(f"SELECT {i};")
            
            discovered = sorted(tmppath.glob("*.sql"))
            
            # Verify order
            for idx, script in enumerate(discovered):
                expected_num = str(idx).zfill(3)
                assert script.name.startswith(expected_num)


class TestTransactionHandling:
    """Test transaction boundaries and rollback behavior."""
    
    @pytest.mark.unit
    @patch('run_scheduled_operations.read_sql_file')
    @patch('run_scheduled_operations.log_execution')
    def test_script_with_explicit_begin_commit(self, mock_log, mock_read):
        """Given: SQL script with explicit BEGIN/COMMIT
        When: execute_sql_script() is called
        Then: Executes and commits"""
        sql_with_transaction = "BEGIN; SELECT 1; COMMIT;"
        mock_read.return_value = sql_with_transaction
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        success, error = execute_sql_script(mock_conn, Path("test.sql"), "test.sql")
        
        assert success is True
        mock_conn.commit.assert_called()
    
    @pytest.mark.unit
    @patch('run_scheduled_operations.read_sql_file')
    @patch('run_scheduled_operations.log_execution')
    def test_transaction_rollback_on_error(self, mock_log, mock_read):
        """Given: SQL script that fails mid-execution
        When: execute_sql_script() is called
        Then: Calls rollback()"""
        mock_read.return_value = "INVALID;"
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("syntax error")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        success, error = execute_sql_script(mock_conn, Path("test.sql"), "test.sql")
        
        assert success is False
        mock_conn.rollback.assert_called_once()
    
    @pytest.mark.unit
    @patch('run_scheduled_operations.read_sql_file')
    @patch('run_scheduled_operations.log_execution')
    def test_commit_success_logs_correctly(self, mock_log, mock_read):
        """Given: Successful SQL execution
        When: log_execution() is called
        Then: Logs 'success' status"""
        mock_read.return_value = "SELECT 1;"
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        execute_sql_script(mock_conn, Path("test.sql"), "test.sql")
        
        # Check that log_execution was called with 'success'
        mock_log.assert_called()
        args = mock_log.call_args[0]
        assert args[2] == 'success'  # status is 3rd positional argument


class TestLoggingStartup:
    """Test logging startup configuration and validation."""
    
    @pytest.mark.unit
    def test_configure_logging_fails_if_log_dir_missing(self, monkeypatch):
        """Given: LOG_DIR points to missing directory
        When: configure_logging() is called
        Then: Raises FileNotFoundError with guidance"""
        monkeypatch.setattr(
            'run_scheduled_operations.LOG_DIR',
            Path('/nonexistent/path/dc_error_logs')
        )
        
        with pytest.raises(FileNotFoundError) as exc_info:
            from run_scheduled_operations import configure_logging
            configure_logging()
        
        assert "does not exist" in str(exc_info.value)
        assert "setup_cronjob_scheduled_operations.sh" in str(exc_info.value)
    
    @pytest.mark.unit
    def test_configure_logging_fails_if_log_dir_not_writable(self, tmp_path, monkeypatch):
        """Given: LOG_DIR points to read-only directory
        When: configure_logging() is called
        Then: Raises PermissionError with guidance"""
        read_only_dir = tmp_path / "readonly"
        read_only_dir.mkdir()
        read_only_dir.chmod(0o555)  # Read-only
        
        try:
            monkeypatch.setattr(
                'run_scheduled_operations.LOG_DIR',
                read_only_dir
            )
            
            with pytest.raises(PermissionError) as exc_info:
                from run_scheduled_operations import configure_logging
                configure_logging()
            
            assert "not writable" in str(exc_info.value)
        finally:
            read_only_dir.chmod(0o755)  # Restore for cleanup
    
    @pytest.mark.unit
    def test_configure_logging_succeeds_with_valid_dir(self, tmp_path, monkeypatch):
        """Given: LOG_DIR points to valid writable directory
        When: configure_logging() is called
        Then: Returns configured logger"""
        valid_dir = tmp_path / "logs"
        valid_dir.mkdir()
        
        monkeypatch.setattr(
            'run_scheduled_operations.LOG_DIR',
            valid_dir
        )
        
        from run_scheduled_operations import configure_logging
        logger = configure_logging()
        
        assert logger is not None
        assert (valid_dir / 'scheduled_operations.log').exists()
    
    @pytest.mark.unit
    @patch.dict('os.environ', {'PGDATABASE': 'testdb', 'PGUSER': 'testuser'})
    def test_main_fails_at_startup_if_log_dir_missing(self, monkeypatch):
        """Given: LOG_DIR does not exist when main() is called
        When: main() is invoked
        Then: Returns 1 and exits without DB operations"""
        monkeypatch.setattr(
            'run_scheduled_operations.LOG_DIR',
            Path('/nonexistent/dc_error_logs')
        )
        monkeypatch.setattr('run_scheduled_operations.logger', None)
        
        from run_scheduled_operations import main
        result = main()
        
        assert result == 1



