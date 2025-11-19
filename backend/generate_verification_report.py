#!/usr/bin/env python3
"""
Generate verification reports in markdown and JSON formats.

This module generates comprehensive reports from verification results,
including executive summaries, detailed comparisons, and recommendations.
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generate verification reports"""
    
    def __init__(self, output_dir: Optional[str] = None):
        """
        Initialize report generator
        
        Args:
            output_dir: Directory to save reports (default: ./verification_reports)
        """
        if output_dir is None:
            output_dir = Path(__file__).parent / "verification_reports"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_markdown_report(
        self,
        results: Dict[str, Any],
        filename: Optional[str] = None
    ) -> str:
        """
        Generate markdown report
        
        Args:
            results: Verification results dictionary
            filename: Optional filename (default: verification_report_{cycle}_{timestamp}.md)
        
        Returns:
            Path to generated report file
        """
        cycle = results.get('cycle', 'unknown')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if filename is None:
            filename = f"verification_report_{cycle}_{timestamp}.md"
        
        filepath = self.output_dir / filename
        
        with open(filepath, 'w') as f:
            f.write(self._generate_markdown_content(results))
        
        logger.info(f"Markdown report saved to {filepath}")
        return str(filepath)
    
    def _generate_markdown_content(self, results: Dict[str, Any]) -> str:
        """Generate markdown content from results"""
        cycle = results.get('cycle', 'unknown')
        timestamp = results.get('timestamp', datetime.now().isoformat())
        summary = results.get('summary', {})
        comparisons = results.get('comparisons', {})
        integrity_validation = results.get('integrity_validation', {})
        
        lines = []
        lines.append("# Bulk Data Import Verification Report")
        lines.append("")
        lines.append(f"**Cycle:** {cycle}")
        lines.append(f"**Generated:** {timestamp}")
        lines.append("")
        
        # Executive Summary
        lines.append("## Executive Summary")
        lines.append("")
        lines.append(f"- **Total Data Types:** {summary.get('total_data_types', 0)}")
        lines.append(f"- **Passed:** {summary.get('passed', 0)}")
        lines.append(f"- **Failed:** {summary.get('failed', 0)}")
        lines.append(f"- **Warnings:** {summary.get('warnings', 0)}")
        lines.append("")
        total_file = summary.get('total_file_records', 0)
        total_db = summary.get('total_database_records', 0)
        accuracy = summary.get('overall_accuracy_percent', 0)
        lines.append(f"- **Total File Records:** {total_file:,}" if isinstance(total_file, int) else f"- **Total File Records:** {total_file}")
        lines.append(f"- **Total Database Records:** {total_db:,}" if isinstance(total_db, int) else f"- **Total Database Records:** {total_db}")
        lines.append(f"- **Overall Accuracy:** {accuracy:.2f}%")
        lines.append("")
        
        # Detailed Comparisons
        lines.append("## Detailed Comparisons")
        lines.append("")
        lines.append("| Data Type | File Count | Database Count | Difference | Status |")
        lines.append("|-----------|------------|----------------|------------|--------|")
        
        for data_type, comp in sorted(comparisons.items()):
            status_symbol = {
                'pass': '✓',
                'fail': '✗',
                'warning': '⚠'
            }.get(comp['status'], '?')
            
            file_count = comp.get('file_count', 0)
            expected_count = comp.get('expected_count', 0)
            difference = comp.get('difference', 0)
            
            file_str = f"{file_count:,}" if isinstance(file_count, int) else str(file_count)
            expected_str = f"{expected_count:,}" if isinstance(expected_count, int) else str(expected_count)
            diff_str = f"{difference:,}" if isinstance(difference, int) else str(difference)
            
            lines.append(
                f"| {data_type} | {file_str} | {expected_str} | "
                f"{diff_str} | {status_symbol} {comp['status']} |"
            )
        
        lines.append("")
        
        # Failed Verifications
        failed = {k: v for k, v in comparisons.items() if v['status'] == 'fail'}
        if failed:
            lines.append("## Failed Verifications")
            lines.append("")
            for data_type, comp in sorted(failed.items()):
                lines.append(f"### {data_type}")
                lines.append("")
                file_count = comp.get('file_count', 0)
                expected_count = comp.get('expected_count', 0)
                difference = comp.get('difference', 0)
                tolerance = comp.get('tolerance', 0)
                percent_diff = comp.get('percent_difference', 0)
                
                file_str = f"{file_count:,}" if isinstance(file_count, int) else str(file_count)
                expected_str = f"{expected_count:,}" if isinstance(expected_count, int) else str(expected_count)
                diff_str = f"{difference:,}" if isinstance(difference, int) else str(difference)
                tol_str = f"{tolerance:,}" if isinstance(tolerance, int) else str(tolerance)
                
                lines.append(f"- **File Count:** {file_str}")
                lines.append(f"- **Database Count:** {expected_str}")
                lines.append(f"- **Difference:** {diff_str} ({percent_diff:.2f}%)")
                lines.append(f"- **Tolerance:** {tol_str}")
                lines.append("")
                lines.append("**Recommendation:** Investigate why records are missing from database.")
                lines.append("")
        
        # Warnings
        warnings = {k: v for k, v in comparisons.items() if v['status'] == 'warning'}
        if warnings:
            lines.append("## Warnings")
            lines.append("")
            for data_type, comp in sorted(warnings.items()):
                lines.append(f"### {data_type}")
                lines.append("")
                file_count = comp.get('file_count', 0)
                expected_count = comp.get('expected_count', 0)
                difference = comp.get('difference', 0)
                percent_diff = comp.get('percent_difference', 0)
                
                file_str = f"{file_count:,}" if isinstance(file_count, int) else str(file_count)
                expected_str = f"{expected_count:,}" if isinstance(expected_count, int) else str(expected_count)
                diff_str = f"{difference:,}" if isinstance(difference, int) else str(difference)
                
                lines.append(f"- **File Count:** {file_str}")
                lines.append(f"- **Database Count:** {expected_str}")
                lines.append(f"- **Difference:** {diff_str} ({percent_diff:.2f}%)")
                lines.append("")
                if comp['file_count'] == 0 and comp['expected_count'] == 0:
                    lines.append("**Note:** Both file and database counts are zero. This may be expected for some data types.")
                else:
                    lines.append("**Note:** Significant difference detected. Review import logs for skipped records.")
                lines.append("")
        
        # Data Integrity Validation
        if integrity_validation:
            lines.append("## Data Integrity Validation")
            lines.append("")
            for data_type, validation in sorted(integrity_validation.items()):
                lines.append(f"### {data_type}")
                lines.append("")
                
                if 'error' in validation:
                    lines.append(f"**Error:** {validation['error']}")
                    lines.append("")
                    continue
                
                field_val = validation.get('field_validation', {})
                db_verif = validation.get('database_verification', {})
                
                lines.append(f"- **Sample Size:** {validation.get('sample_size', 0)}")
                lines.append(f"- **Field Validation:** {'✓ Valid' if field_val.get('valid') else '✗ Invalid'}")
                if field_val.get('errors'):
                    lines.append(f"  - Errors: {len(field_val['errors'])}")
                if field_val.get('warnings'):
                    lines.append(f"  - Warnings: {len(field_val['warnings'])}")
                
                lines.append(f"- **Database Verification:**")
                lines.append(f"  - Found: {db_verif.get('found', 0)}")
                lines.append(f"  - Missing: {db_verif.get('missing', 0)}")
                
                if db_verif.get('missing', 0) > 0:
                    lines.append("  - **Warning:** Some sampled records were not found in database")
                
                lines.append("")
        
        # Recommendations
        lines.append("## Recommendations")
        lines.append("")
        
        if summary.get('failed', 0) > 0:
            lines.append("1. **Investigate Failed Imports:** Review import logs for data types with failed verifications.")
            lines.append("2. **Check Import Status:** Verify that imports completed successfully in the database.")
            lines.append("3. **Review Skipped Records:** Check if records were skipped due to validation errors.")
            lines.append("")
        
        if warnings:
            lines.append("4. **Review Warnings:** Investigate data types with significant count differences.")
            lines.append("5. **Check Data Quality:** Some records may have been skipped due to data quality issues.")
            lines.append("")
        
        if integrity_validation:
            missing_records = sum(
                v.get('database_verification', {}).get('missing', 0)
                for v in integrity_validation.values()
            )
            if missing_records > 0:
                lines.append("6. **Data Integrity Issues:** Some sampled records were not found in database.")
                lines.append("   Consider re-importing affected data types.")
                lines.append("")
        
        if summary.get('overall_accuracy_percent', 100) < 95:
            lines.append("7. **Overall Accuracy Below 95%:** Review import process and data quality.")
            lines.append("")
        
        lines.append("---")
        lines.append("")
        lines.append(f"*Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
        
        return "\n".join(lines)
    
    def generate_json_report(
        self,
        results: Dict[str, Any],
        filename: Optional[str] = None
    ) -> str:
        """
        Generate JSON report
        
        Args:
            results: Verification results dictionary
            filename: Optional filename (default: verification_report_{cycle}_{timestamp}.json)
        
        Returns:
            Path to generated report file
        """
        cycle = results.get('cycle', 'unknown')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if filename is None:
            filename = f"verification_report_{cycle}_{timestamp}.json"
        
        filepath = self.output_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"JSON report saved to {filepath}")
        return str(filepath)
    
    def generate_reports(
        self,
        results: Dict[str, Any],
        markdown_filename: Optional[str] = None,
        json_filename: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Generate both markdown and JSON reports
        
        Args:
            results: Verification results dictionary
            markdown_filename: Optional markdown filename
            json_filename: Optional JSON filename
        
        Returns:
            Dictionary with 'markdown' and 'json' keys containing file paths
        """
        markdown_path = self.generate_markdown_report(results, markdown_filename)
        json_path = self.generate_json_report(results, json_filename)
        
        return {
            'markdown': markdown_path,
            'json': json_path
        }


async def main():
    """Test the report generator"""
    import sys
    import json
    
    # Load results from JSON file if provided
    if len(sys.argv) > 1:
        with open(sys.argv[1], 'r') as f:
            results = json.load(f)
    else:
        # Sample results for testing
        results = {
            'cycle': 2026,
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_data_types': 12,
                'passed': 10,
                'failed': 1,
                'warnings': 1,
                'total_file_records': 5000000,
                'total_database_records': 4950000,
                'overall_accuracy_percent': 99.0
            },
            'comparisons': {}
        }
    
    generator = ReportGenerator()
    paths = generator.generate_reports(results)
    
    print(f"Reports generated:")
    print(f"  Markdown: {paths['markdown']}")
    print(f"  JSON: {paths['json']}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

