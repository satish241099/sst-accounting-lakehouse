"""Generate realistic Malaysia SST accounting data for portfolio showcase."""
import argparse
import csv
import random
import os
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

# Malaysia states for SST filings
STATES = [
    'Johor', 'Kedah', 'Kelantan', 'Malacca', 'Negeri Sembilan',
    'Pahang', 'Penang', 'Perak', 'Perlis', 'Sabah', 'Sarawak',
    'Selangor', 'Terengganu', 'KL', 'Labuan', 'Putrajaya'
]

# Business sectors for SST registration
SECTORS = [
    'Manufacturing', 'Wholesale', 'Retail', 'Construction',
    'Professional Services', 'Hospitality', 'Transportation',
    'E-commerce', 'Food & Beverage', 'Technology'
]

# SST registration types
REG_TYPES = ['Standard', 'Small', 'Not Applicable']


def generate_suppliers(n=200):
    """Generate supplier master data with SCD2-friendly fields."""
    rows = []
    for i in range(n):
        supplier_id = f'SUP-{i+1:05d}'
        # Some suppliers get updated later (SCD2 demo)
        rows.append({
            'supplier_id': supplier_id,
            'name': fake.company(),
            'sst_number': f'SST-{random.randint(100000, 999999)}',
            'sector': random.choice(SECTORS),
            'state': random.choice(STATES),
            'registration_type': random.choice(REG_TYPES),
            'is_active': random.choice([True, False]),
            'created_date': fake.date_between(start_date='-3y', end_date='-1y'),
            'updated_date': fake.date_between(start_date='-6m', end_date='today'),
            'email': fake.email(),
            'phone': fake.phone_number(),
        })
    return rows


def generate_sst_filings(n=3000):
    """Generate SST-03 filing records with realistic amounts."""
    rows = []
    for i in range(n):
        filing_date = fake.date_between(start_date='-2y', end_date='today')
        period = f"{filing_date.year}-Q{(filing_date.month - 1) // 3 + 1}"
        gross_sales = round(random.uniform(10000, 5000000), 2)
        taxable_sales = round(gross_sales * random.uniform(0.3, 0.8), 2)
        sst_rate = random.choice([6.0, 5.0, 0.0])
        sst_payable = round(taxable_sales * sst_rate / 100, 2)
        rows.append({
            'filing_id': f'FIL-{i+1:06d}',
            'supplier_id': f'SUP-{random.randint(1, 200):05d}',
            'filing_date': filing_date.isoformat(),
            'period': period,
            'state': random.choice(STATES),
            'sector': random.choice(SECTORS),
            'gross_sales': gross_sales,
            'taxable_sales': taxable_sales,
            'sst_rate': sst_rate,
            'sst_payable': sst_payable,
            'filing_status': random.choices(
                ['Submitted', 'Pending', 'Overdue', 'Rejected'],
                weights=[70, 15, 10, 5]
            )[0],
            'submission_reference': f'REF-{fake.uuid4()[:8].upper()}',
        })
    return rows


def write_csv(filename, rows, fieldnames):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f'Wrote {len(rows)} rows to {filename}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--rows', type=int, default=3000)
    args = parser.parse_args()

    base = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
    base = os.path.abspath(base)

    suppliers = generate_suppliers(200)
    write_csv(
        os.path.join(base, 'suppliers.csv'),
        suppliers,
        list(suppliers[0].keys())
    )

    filings = generate_sst_filings(args.rows)
    write_csv(
        os.path.join(base, 'sst_filings.csv'),
        filings,
        list(filings[0].keys())
    )

    print('Done. Run the Databricks notebooks to process this data.')