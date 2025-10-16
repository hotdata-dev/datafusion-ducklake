#!/bin/bash
# Script to generate test data for delete filter tests

set -e

echo "Generating test data for delete filter tests..."

# Check if duckdb is installed
if ! command -v duckdb &> /dev/null; then
    echo "Error: duckdb command not found. Please install DuckDB first."
    echo "Installation: https://duckdb.org/docs/installation/"
    exit 1
fi

# Create test data directory
mkdir -p tests/test_data

# Run the SQL script to generate test databases
echo "Running setup_test_data_v2.sql..."
duckdb < tests/setup_test_data_v2.sql

echo ""
echo "✅ Test data generated successfully!"
echo ""
echo "Generated test databases:"
echo "  - tests/test_data/no_deletes.ducklake (table without delete files)"
echo "  - tests/test_data/with_deletes.ducklake (table with DELETE operations)"
echo "  - tests/test_data/with_updates.ducklake (table with UPDATE operations)"
echo ""
echo "Run tests with: cargo test --test delete_filter_tests"
