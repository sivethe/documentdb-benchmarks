#!/usr/bin/env python3
"""
Test script to validate the GitHub Pages website structure
"""

import json
import os
import sys
from pathlib import Path

def test_file_exists(filepath, description):
    """Test if a file exists"""
    if not filepath.exists():
        print(f"❌ FAIL: {description} not found at {filepath}")
        return False
    print(f"✅ PASS: {description} exists")
    return True

def test_json_valid(filepath, description):
    """Test if a JSON file is valid"""
    if not filepath.exists():
        print(f"❌ FAIL: {description} not found at {filepath}")
        return False
    
    try:
        with open(filepath, 'r') as f:
            json.load(f)
        print(f"✅ PASS: {description} is valid JSON")
        return True
    except json.JSONDecodeError as e:
        print(f"❌ FAIL: {description} has invalid JSON: {e}")
        return False

def main():
    """Run all tests"""
    base_dir = Path.cwd()
    print(f"Testing GitHub Pages website in: {base_dir}\n")
    
    tests_passed = 0
    tests_failed = 0
    
    # Test HTML file
    if test_file_exists(base_dir / "index.html", "index.html"):
        tests_passed += 1
    else:
        tests_failed += 1
    
    # Test CSS file
    if test_file_exists(base_dir / "css" / "style.css", "CSS file"):
        tests_passed += 1
    else:
        tests_failed += 1
    
    # Test JavaScript files
    js_files = ["app.js", "charts.js", "compare.js", "dashboard.js"]
    for js_file in js_files:
        if test_file_exists(base_dir / "js" / js_file, f"JavaScript file {js_file}"):
            tests_passed += 1
        else:
            tests_failed += 1
    
    # Test data directory structure
    if test_json_valid(base_dir / "data" / "index.json", "Index JSON"):
        tests_passed += 1
        
        # Load index and test benchmark data files
        try:
            with open(base_dir / "data" / "index.json", 'r') as f:
                index_data = json.load(f)
            
            benchmarks = index_data.get("benchmarks", [])
            engines = index_data.get("engines", [])
            
            print(f"\n📊 Found {len(benchmarks)} benchmarks and {len(engines)} engines")
            
            # Test a sample benchmark data file
            if benchmarks and engines:
                sample_benchmark = benchmarks[0]
                sample_engine = engines[0]
                data_file = base_dir / "data" / sample_benchmark / f"{sample_engine}.json"
                
                if test_json_valid(data_file, f"Sample benchmark data ({sample_benchmark}/{sample_engine})"):
                    tests_passed += 1
                    
                    # Validate structure
                    with open(data_file, 'r') as f:
                        data = json.load(f)
                    
                    if isinstance(data, list) and len(data) > 0:
                        entry = data[0]
                        required_fields = ["timestamp", "benchmark_name", "database_engine", "summary"]
                        all_present = all(field in entry for field in required_fields)
                        
                        if all_present:
                            print(f"✅ PASS: Data structure has required fields")
                            tests_passed += 1
                        else:
                            print(f"❌ FAIL: Data structure missing required fields")
                            tests_failed += 1
                    else:
                        print(f"❌ FAIL: Data file is not a non-empty array")
                        tests_failed += 1
                else:
                    tests_failed += 1
        except Exception as e:
            print(f"❌ FAIL: Error processing index data: {e}")
            tests_failed += 1
    else:
        tests_failed += 1
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"Test Summary")
    print(f"{'='*60}")
    print(f"✅ Tests Passed: {tests_passed}")
    print(f"❌ Tests Failed: {tests_failed}")
    print(f"{'='*60}")
    
    if tests_failed > 0:
        print("\n⚠️  Some tests failed. Please fix the issues above.")
        sys.exit(1)
    else:
        print("\n🎉 All tests passed! Website structure is valid.")
        print("\n📝 Next steps:")
        print("   1. Start local server: python3 -m http.server 8000")
        print("   2. Open http://localhost:8000 in your browser")
        print("   3. Test all pages: graphs, compare, dashboard, status, help")
        sys.exit(0)

if __name__ == "__main__":
    main()
