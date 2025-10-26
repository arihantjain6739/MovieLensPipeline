"""
HDFS Test Script

Quick script to verify HDFS setup and test basic operations.
"""

import subprocess
import sys

def run_command(cmd, description):
    """Run a command and print results."""
    print(f"\n{'='*60}")
    print(f"Testing: {description}")
    print(f"{'='*60}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"✓ Success!")
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed: {e.stderr}")
        return False

def main():
    """Run HDFS tests."""
    print("=" * 60)
    print("  HDFS VERIFICATION TEST")
    print("=" * 60)
    
    tests = [
        (
            ['docker', 'ps', '--filter', 'name=namenode', '--format', '{{.Names}}\t{{.Status}}'],
            "Check if namenode container is running"
        ),
        (
            ['docker', 'exec', 'namenode', 'hdfs', 'dfsadmin', '-report'],
            "Check HDFS cluster health"
        ),
        (
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '/'],
            "List HDFS root directory"
        ),
        (
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '/movielens'],
            "List MovieLens directories in HDFS"
        ),
    ]
    
    passed = 0
    failed = 0
    
    for cmd, description in tests:
        if run_command(cmd, description):
            passed += 1
        else:
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"  TEST RESULTS: {passed} passed, {failed} failed")
    print("=" * 60)
    
    if failed == 0:
        print("\n✓ HDFS is ready! You can now run: python main.py")
        return 0
    else:
        print("\n✗ HDFS setup incomplete. Please run: docker-compose up -d")
        return 1

if __name__ == "__main__":
    sys.exit(main())
