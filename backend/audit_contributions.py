#!/usr/bin/env python3
"""
Audit script to analyze contribution data completeness and quality.
Identifies missing fields, data quality issues, and raw_data completeness.
"""
import asyncio
import json
import sys
from datetime import datetime
from sqlalchemy import select, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

# Add parent directory to path
sys.path.insert(0, '/home/ls-jacob/Documents/Personal/GitHub/FEC_Query/backend')

from app.db.database import AsyncSessionLocal, Contribution


async def audit_contributions():
    """Audit contribution data quality and completeness"""
    print("=" * 80)
    print("CONTRIBUTION DATA AUDIT")
    print("=" * 80)
    print()
    
    async with AsyncSessionLocal() as session:
        # Total contributions
        total_result = await session.execute(select(func.count(Contribution.id)))
        total_count = total_result.scalar()
        print(f"Total Contributions: {total_count:,}")
        print()
        
        if total_count == 0:
            print("No contributions found in database.")
            return
        
        # Field completeness analysis
        print("FIELD COMPLETENESS ANALYSIS")
        print("-" * 80)
        
        fields_to_check = [
            ('contribution_id', 'Contribution ID'),
            ('candidate_id', 'Candidate ID'),
            ('committee_id', 'Committee ID'),
            ('contributor_name', 'Contributor Name'),
            ('contributor_city', 'Contributor City'),
            ('contributor_state', 'Contributor State'),
            ('contributor_zip', 'Contributor ZIP'),
            ('contributor_employer', 'Contributor Employer'),
            ('contributor_occupation', 'Contributor Occupation'),
            ('contribution_amount', 'Contribution Amount'),
            ('contribution_date', 'Contribution Date'),
            ('contribution_type', 'Contribution Type'),
            ('amendment_indicator', 'Amendment Indicator'),
            ('report_type', 'Report Type'),
            ('transaction_id', 'Transaction ID'),
            ('entity_type', 'Entity Type'),
            ('other_id', 'Other ID'),
            ('file_number', 'File Number'),
            ('memo_code', 'Memo Code'),
            ('memo_text', 'Memo Text'),
            ('raw_data', 'Raw Data'),
        ]
        
        field_stats = {}
        for field_name, display_name in fields_to_check:
            # Count non-null values
            field = getattr(Contribution, field_name)
            
            if field_name == 'contribution_amount':
                # For amount, check > 0
                result = await session.execute(
                    select(func.count()).where(and_(field.isnot(None), field > 0))
                )
                non_null_count = result.scalar()
            elif field_name == 'raw_data':
                # For raw_data, check if it's a dict with content
                result = await session.execute(
                    select(func.count()).where(field.isnot(None))
                )
                non_null_count = result.scalar()
            else:
                result = await session.execute(
                    select(func.count()).where(field.isnot(None))
                )
                non_null_count = result.scalar()
            
            percentage = (non_null_count / total_count * 100) if total_count > 0 else 0
            field_stats[field_name] = {
                'count': non_null_count,
                'percentage': percentage,
                'missing': total_count - non_null_count
            }
            
            print(f"{display_name:30} {non_null_count:>10,} ({percentage:>6.2f}%)")
        
        print()
        
        # Data quality issues
        print("DATA QUALITY ISSUES")
        print("-" * 80)
        
        # Contributions with zero or negative amounts
        zero_amount_result = await session.execute(
            select(func.count()).where(
                or_(
                    Contribution.contribution_amount.is_(None),
                    Contribution.contribution_amount <= 0
                )
            )
        )
        zero_amount_count = zero_amount_result.scalar()
        print(f"Contributions with zero/negative/missing amount: {zero_amount_count:,} ({zero_amount_count/total_count*100:.2f}%)")
        
        # Contributions without dates
        no_date_result = await session.execute(
            select(func.count()).where(Contribution.contribution_date.is_(None))
        )
        no_date_count = no_date_result.scalar()
        print(f"Contributions without date: {no_date_count:,} ({no_date_count/total_count*100:.2f}%)")
        
        # Contributions without contributor name
        no_name_result = await session.execute(
            select(func.count()).where(Contribution.contributor_name.is_(None))
        )
        no_name_count = no_name_result.scalar()
        print(f"Contributions without contributor name: {no_name_count:,} ({no_name_count/total_count*100:.2f}%)")
        
        # Contributions without candidate_id
        no_candidate_result = await session.execute(
            select(func.count()).where(Contribution.candidate_id.is_(None))
        )
        no_candidate_count = no_candidate_result.scalar()
        print(f"Contributions without candidate_id: {no_candidate_count:,} ({no_candidate_count/total_count*100:.2f}%)")
        
        # Contributions without committee_id
        no_committee_result = await session.execute(
            select(func.count()).where(Contribution.committee_id.is_(None))
        )
        no_committee_count = no_committee_result.scalar()
        print(f"Contributions without committee_id: {no_committee_count:,} ({no_committee_count/total_count*100:.2f}%)")
        
        print()
        
        # Raw data analysis
        print("RAW_DATA ANALYSIS")
        print("-" * 80)
        
        # Sample raw_data to see what fields are available
        sample_result = await session.execute(
            select(Contribution.raw_data)
            .where(Contribution.raw_data.isnot(None))
            .limit(100)
        )
        samples = sample_result.scalars().all()
        
        if samples:
            # Collect all unique keys from raw_data
            all_keys = set()
            for sample in samples:
                if isinstance(sample, dict):
                    all_keys.update(sample.keys())
            
            print(f"Unique fields found in raw_data: {len(all_keys)}")
            print("\nFields in raw_data (sample):")
            for key in sorted(all_keys)[:30]:  # Show first 30
                print(f"  - {key}")
            if len(all_keys) > 30:
                print(f"  ... and {len(all_keys) - 30} more")
            
            # Check for specific FEC fields - all 20 Schedule A fields
            print("\nChecking for all Schedule A fields in raw_data (should have all 20):")
            fec_fields = [
                'CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRAN_ID', 'ENTITY_TP',
                'NAME', 'CITY', 'STATE', 'ZIP_CODE', 'EMPLOYER', 'OCCUPATION',
                'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID', 'CAND_ID',
                'TRAN_TP', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT', 'SUB_ID'
            ]
            
            field_counts = {}
            for field in fec_fields:
                count = 0
                for sample in samples:
                    if isinstance(sample, dict) and field in sample and sample[field]:
                        count += 1
                field_counts[field] = count
                if count > 0:
                    print(f"  {field:20} found in {count:>3} of {len(samples)} samples ({count/len(samples)*100:.1f}%)")
        
        print()
        
        # Date range analysis
        print("DATE RANGE ANALYSIS")
        print("-" * 80)
        
        date_range_result = await session.execute(
            select(
                func.min(Contribution.contribution_date).label('min_date'),
                func.max(Contribution.contribution_date).label('max_date')
            )
        )
        date_range = date_range_result.first()
        
        if date_range and date_range.min_date and date_range.max_date:
            print(f"Earliest contribution: {date_range.min_date}")
            print(f"Latest contribution: {date_range.max_date}")
            
            # Contributions by year
            print("\nContributions by year:")
            year_result = await session.execute(
                select(
                    func.extract('year', Contribution.contribution_date).label('year'),
                    func.count().label('count')
                )
                .where(Contribution.contribution_date.isnot(None))
                .group_by(func.extract('year', Contribution.contribution_date))
                .order_by(func.extract('year', Contribution.contribution_date).desc())
                .limit(10)
            )
            for row in year_result:
                print(f"  {int(row.year)}: {row.count:,} contributions")
        else:
            print("No date information available")
        
        print()
        
        # Amount statistics
        print("AMOUNT STATISTICS")
        print("-" * 80)
        
        amount_stats_result = await session.execute(
            select(
                func.min(Contribution.contribution_amount).label('min'),
                func.max(Contribution.contribution_amount).label('max'),
                func.avg(Contribution.contribution_amount).label('avg'),
                func.sum(Contribution.contribution_amount).label('total')
            )
            .where(and_(
                Contribution.contribution_amount.isnot(None),
                Contribution.contribution_amount > 0
            ))
        )
        amount_stats = amount_stats_result.first()
        
        if amount_stats and amount_stats.min is not None:
            print(f"Minimum amount: ${amount_stats.min:,.2f}")
            print(f"Maximum amount: ${amount_stats.max:,.2f}")
            print(f"Average amount: ${amount_stats.avg:,.2f}")
            print(f"Total amount: ${amount_stats.total:,.2f}")
        
        print()
        
        # Summary recommendations
        print("SUMMARY & RECOMMENDATIONS")
        print("-" * 80)
        
        recommendations = []
        
        if field_stats['contribution_date']['percentage'] < 95:
            recommendations.append(f"⚠️  {field_stats['contribution_date']['missing']:,} contributions missing dates ({100-field_stats['contribution_date']['percentage']:.1f}%)")
        
        if field_stats['contributor_name']['percentage'] < 95:
            recommendations.append(f"⚠️  {field_stats['contributor_name']['missing']:,} contributions missing contributor names ({100-field_stats['contributor_name']['percentage']:.1f}%)")
        
        if field_stats['candidate_id']['percentage'] < 90:
            recommendations.append(f"⚠️  {field_stats['candidate_id']['missing']:,} contributions missing candidate_id ({100-field_stats['candidate_id']['percentage']:.1f}%)")
        
        if field_stats['contributor_employer']['percentage'] < 50:
            recommendations.append(f"ℹ️  Only {field_stats['contributor_employer']['percentage']:.1f}% have employer data (consider extracting from raw_data)")
        
        # Check raw_data completeness - should have all 20 Schedule A fields
        expected_schedule_a_fields = 20
        if samples:
            sample = samples[0] if isinstance(samples[0], dict) else {}
            found_fields = len([f for f in fec_fields if f in sample])
            if found_fields < expected_schedule_a_fields:
                recommendations.append(f"⚠️  raw_data has {found_fields} of {expected_schedule_a_fields} expected Schedule A fields - missing fields may indicate incomplete extraction")
            elif found_fields == expected_schedule_a_fields:
                recommendations.append(f"✓ raw_data contains all {expected_schedule_a_fields} Schedule A fields")
        
        # Check if new structured fields are populated
        if field_stats.get('amendment_indicator', {}).get('count', 0) > 0:
            recommendations.append(f"✓ Amendment indicator field is populated ({field_stats['amendment_indicator']['count']:,} records)")
        if field_stats.get('memo_code', {}).get('count', 0) > 0:
            recommendations.append(f"✓ Memo code field is populated ({field_stats['memo_code']['count']:,} records) - useful for fraud detection")
        if field_stats.get('entity_type', {}).get('count', 0) > 0:
            recommendations.append(f"✓ Entity type field is populated ({field_stats['entity_type']['count']:,} records) - useful for categorization")
        
        if recommendations:
            for rec in recommendations:
                print(rec)
        else:
            print("✓ Data quality looks good!")
        
        print()
        print("=" * 80)
        print("Audit complete!")
        print("=" * 80)


if __name__ == "__main__":
    asyncio.run(audit_contributions())

