import unittest
import tempfile
import os
from unittest.mock import Mock, patch
from dbt_wrapper.wrapper import Commands


class TestCommands(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_console = Mock()
        self.commands = Commands(console=self.mock_console)
        
        # Create a temporary directory for testing
        self.test_dir = tempfile.mkdtemp()
        self.commands.dbt_project_dir = self.test_dir
        
        # Create logs directory
        self.logs_dir = os.path.join(self.test_dir, "logs")
        os.makedirs(self.logs_dir, exist_ok=True)
        
    def tearDown(self):
        """Clean up after each test method."""
        # Clean up the temporary directory
        import shutil
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_clear_dbt_log_file_exists(self):
        """Test that ClearDbtLog clears an existing log file."""
        # Create a log file with some content
        log_file_path = os.path.join(self.logs_dir, "dbt.log")
        with open(log_file_path, 'w') as f:
            f.write("Some previous log content\nMore log lines\n")
        
        # Verify the file has content before clearing
        with open(log_file_path, 'r') as f:
            content_before = f.read()
        self.assertTrue(len(content_before) > 0)
        
        # Clear the log
        self.commands.ClearDbtLog()
        
        # Verify the file is now empty
        with open(log_file_path, 'r') as f:
            content_after = f.read()
        self.assertEqual(content_after, "")
        
        # Verify console was called with success message
        self.mock_console.print.assert_called_with(
            f"Cleared dbt log file: {log_file_path}", 
            style="info"
        )

    def test_clear_dbt_log_file_not_exists(self):
        """Test that ClearDbtLog handles case when log file doesn't exist."""
        # Ensure log file doesn't exist
        log_file_path = os.path.join(self.logs_dir, "dbt.log")
        if os.path.exists(log_file_path):
            os.remove(log_file_path)
        
        # Clear the log (should not raise an error)
        self.commands.ClearDbtLog()
        
        # Verify no console message was printed (since file didn't exist)
        self.mock_console.print.assert_not_called()

    def test_clear_dbt_log_permission_error(self):
        """Test that ClearDbtLog handles permission errors gracefully."""
        log_file_path = os.path.join(self.logs_dir, "dbt.log")
        
        # Create the log file
        with open(log_file_path, 'w') as f:
            f.write("Some content")
        
        # Mock open to raise a permission error
        with patch('builtins.open', side_effect=PermissionError("Permission denied")):
            # Clear the log (should not raise an error)
            self.commands.ClearDbtLog()
            
            # Verify warning message was printed
            self.mock_console.print.assert_called_with(
                f"Warning: Could not clear dbt log file {log_file_path}: Permission denied",
                style="warning"
            )


if __name__ == '__main__':
    unittest.main()