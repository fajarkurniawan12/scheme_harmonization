# MegaMart Dataset - Level 3 Assessment

This dataset supports the Level 3 (Senior) technical assessment for Data Engineer (DE), Data Governance & Quality Analyst (DGQA), and Business Intelligence Analyst (BI) roles.

## Scenario Overview

MegaMart Indonesia is the country's largest omnichannel retail conglomerate with:
- **850+ stores** across 34 provinces (sample: 239 stores)
- **12 million loyalty members** (sample: 10,000 customers)
- **$2.8B annual GMV**

Operating formats:
- MegaMart Hypermarket (large-format stores)
- MegaMart Express (convenience stores)
- MegaMart Online (e-commerce)
- MegaMart Wholesale (B2B)
- MegaMart Fresh (dark stores)

**Dataset Period:** August 2025 - January 2026 (6 months)

---

## Files Included

### 1. stores.csv
- **Rows:** 239 stores
- **Columns:** store_id, store_name, store_type, city, province, region, latitude, longitude, opening_date, store_size_sqm, monthly_rent, manager_id
- **Store Types:** HYPERMARKET, EXPRESS, ONLINE, WHOLESALE, FRESH
- **Purpose:** Store master data for profitability analysis

### 2. products.csv
- **Rows:** 950 products
- **Columns:** sku_id, product_name, brand, category_l1, category_l2, category_l3, supplier_id, unit_cost, unit_price, is_private_label, is_perishable, shelf_life_days
- **Categories:** Grocery, Fresh, Household, Electronics, Fashion
- **Purpose:** Product master for margin and inventory analysis

### 3. customers.csv
- **Rows:** 10,000 customers
- **Columns:** customer_id, full_name, nik, phone, email, city, province, registration_date, registration_channel, loyalty_tier, lifetime_value, preferred_store_id, consent_marketing, consent_analytics, consent_third_party, churn_risk_score, first_purchase_date, last_purchase_date
- **Registration Channels:** ORGANIC, REFERRAL, PAID_SOCIAL, PAID_SEARCH, PARTNERSHIP
- **Loyalty Tiers:** BRONZE, SILVER, GOLD, PLATINUM, DIAMOND
- **Purpose:** Customer 360, churn analysis, PII compliance testing

### 4. transactions.csv
- **Rows:** ~137,000 transactions
- **Columns:** txn_id, store_id, customer_id, txn_timestamp, txn_date, channel, sub_channel, total_amount, gross_revenue, discount_amount, net_revenue, cogs, gross_profit, tax_amount, basket_size, payment_method, basket_id, cashier_id, pos_id
- **Channels:** IN_STORE, ONLINE, MARKETPLACE, WHOLESALE
- **Payment Methods:** CASH, CARD, EWALLET, QRIS, CREDIT, COD
- **Purpose:** Revenue analysis, profitability, tax reconciliation

### 5. transaction_items.csv
- **Rows:** ~1.1M items
- **Columns:** item_id, txn_id, sku_id, quantity, unit_price, unit_cost, discount_amount, promo_id, gross_profit
- **Purpose:** Product-level analysis, promotion effectiveness

### 6. inventory_daily.csv
- **Rows:** ~900,000 records
- **Columns:** snapshot_date, store_id, sku_id, opening_stock, received, sold, transferred_in, transferred_out, waste, closing_stock
- **Purpose:** Inventory health, waste analysis, balance reconciliation

### 7. promotions.csv
- **Rows:** 50 promotions
- **Columns:** promo_id, promo_name, promo_type, start_date, end_date, target_category, target_brand, discount_pct, budget_amount, actual_spend, incremental_revenue, roi
- **Promo Types:** DISCOUNT, BOGO, BUNDLE, LOYALTY_POINTS
- **Purpose:** Campaign ROI, incrementality analysis

### 8. clickstream.csv
- **Rows:** ~17,000 events
- **Columns:** event_id, customer_id, session_id, event_timestamp, event_date, event_type, page_url, product_id, device_type, platform, utm_source, utm_campaign, txn_id
- **Event Types:** page_view, product_view, add_to_cart, checkout, purchase
- **UTM Sources:** ORGANIC, PAID_SEARCH, PAID_SOCIAL, EMAIL, DIRECT, REFERRAL
- **Purpose:** Attribution modeling, customer journey analysis

### 9. bank_settlements.csv
- **Rows:** ~1,100 records
- **Columns:** settlement_date, payment_method, settlement_amount, transaction_count, bank_reference
- **Purpose:** Revenue reconciliation, audit queries

### 10. acquisition_channels.csv
- **Rows:** 10,000 records
- **Columns:** customer_id, channel, acquisition_date, campaign_id
- **Purpose:** Customer acquisition cost analysis, cohort analysis

---

## Intentional Data Quality Issues (for DGQA Testing)

The dataset includes intentional DQ issues:

1. **NIK Validation:** ~2% of customers have invalid NIK (not 16 digits)
2. **Phone Duplicates:** ~3% of customers share phone numbers
3. **Tax Calculation Errors:** ~2% of transactions have tax_amount ≠ net_revenue × 0.11
4. **Inventory Balance Errors:** ~2% of inventory records don't balance (closing ≠ opening + received - sold - waste)
5. **Settlement Discrepancies:** ~5% small discrepancies, ~2% large discrepancies vs. transaction totals
6. **Missing Emails:** ~5% of customers have NULL email
7. **Churned Customers:** ~35% of customers have no activity in 180+ days

---

## Key Business Rules

### Profitability Calculations
- **Net Revenue** = gross_revenue - discount_amount
- **Gross Profit** = net_revenue - cogs
- **Tax Amount** = net_revenue × 0.11 (VAT)

### Customer Acquisition Costs (by registration_channel)
| Channel | Cost (Rp) |
|---------|-----------|
| ORGANIC | 0 |
| REFERRAL | 25,000 |
| PAID_SOCIAL | 75,000 |
| PAID_SEARCH | 100,000 |
| PARTNERSHIP | 50,000 |

### Fulfillment Costs
- Online orders: Rp 15,000 per order
- In-store: Rp 0

### Store Operating Costs
- Labor: Rp 500 per transaction
- Utilities: Rp 50 per sqm per day
- Shrinkage: 1.5% of COGS

### Churn Definition
- Customer is "churned" if: CURRENT_DATE - last_purchase_date > 180 days

### Inventory Alerts
- Out of Stock: closing_stock <= 0
- Overstock: days_of_stock > 60
- High Waste: waste_rate > 10%
- Reorder Point: days_of_stock < 7

---

## Assessment Coverage

This dataset is designed to test:

### DE Level 3
- Platform consolidation (schema harmonization)
- Customer 360 pipeline (PySpark)
- SCD Type 2 implementation
- Inventory health pipeline (SQL window functions)
- Pipeline optimization

### BI Level 3
- Customer profitability analysis
- Product profitability matrix
- Store contribution margin
- Churn cohort analysis
- Multi-touch attribution
- Cannibalization analysis

### DGQA Level 3
- DQ rule catalog (25+ rules)
- DQ scoring system
- Tax reconciliation queries
- PII compliance (NIK, phone, email validation)
- Inventory balance checks
- Duplicate detection
- SCD Type 2 for audit trail

---

## Usage Notes

- All currency values are in Indonesian Rupiah (Rp)
- Dates in YYYY-MM-DD format
- Timestamps in YYYY-MM-DD HH:MM:SS format
- NIK (National ID) is 16 digits - must be masked for PII compliance
- Phone numbers have mixed formats (+62, 62, 08) - normalize before duplicate detection

---

*Generated: February 2026*
*Version: 3.0*
*For Senior Level (Level 3) Candidates*
