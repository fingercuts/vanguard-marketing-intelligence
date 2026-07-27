# Executive Summary: Vanguard Marketing Intelligence (VMI) Platform
## Enterprise Cross-Channel Attribution, Budget Optimization, and Data Governance Engine

**Date**: Q2 2026  
**System Class**: Multi-Touch Attribution (MTA) Analytics Lakehouse  
**Primary Stakeholders**: Chief Marketing Officer (CMO), VP of Acquisition, Lead Analytics Engineer  
**Enterprise Entity**: Vanguard Commerce Group  

---

### 1. Strategic Context & Business Opportunity

Vanguard Commerce Group drives customer acquisition through multiple paid media networks (Google Ads, Facebook Ads, TikTok Ads) and organic retention programs. Historically, the marketing analytics team faced severe operational hurdles:
1. **Attribution Fragmentation**: Each network reported performance within its own platform (walled gardens), leading to double-counting of conversions. A single user clicking a TikTok ad, then a Google search ad, resulted in both platforms claiming 100% conversion attribution.
2. **"Dark Marketing" & UTM Leakage**: Approximately 5% of paid traffic conversions were misclassified as organic search or direct traffic due to URL redirects stripping tracking query parameters (`utm_source`, `utm_medium`, `campaign_id`).
3. **Data Privacy (PII) Compliance**: Storing user clickstream event logs containing cleartext IP addresses or email addresses violated global data protection acts (GDPR) and the Indonesian Personal Data Protection (PDP) Law.

The **Vanguard Marketing Intelligence (VMI)** platform consolidates cross-channel data points, enforces strict schema data quality contracts, implements PII hashing, and runs multi-touch attribution models to provide a single, compliant source of truth.

---

### 2. Multi-Touch Attribution & ROI Framework

To measure the incremental value of ad spend, the platform computes key marketing efficiency variables:

$$\text{CTR (Click-Through Rate)} = \frac{\text{Clicks}}{\text{Impressions}}$$

$$\text{CPC (Cost Per Click)} = \frac{\text{Spend}}{\text{Clicks}}$$

$$\text{CPA (Cost Per Acquisition)} = \frac{\text{Spend}}{\text{Conversions}}$$

$$\text{ROAS (Return on Ad Spend)} = \frac{\text{Revenue}}{\text{Spend}}$$

#### Attribution Modeling Framework
The platform supports three standard attribution configurations in the Gold analytical layer:
* **First-Touch Attribution**: Attributes 100% of the conversion value to the initial campaign that introduced the user to the brand. Useful for top-of-funnel brand awareness audits.
* **Last-Touch Attribution**: Attributes 100% of the conversion value to the final campaign clicked before purchase. Useful for quick-cycle transactional attribution.
* **Linear Multi-Touch Attribution**: Evenly distributes conversion credits across all touchpoints in the user's session history, ensuring middle-of-funnel assistance is accurately evaluated.

---

### 3. Data Lakehouse Architecture & PII Masking

The VMI engine operates on a three-tier medallion architecture running over a vectorized OLAP engine (DuckDB + Parquet):

1. **Bronze Layer (Raw Ingestion)**: Standardizes CSV/JSON logs into columnar Parquet format.
2. **Silver Layer (Cleaning & Enrichment)**:
   * **PII Governance**: Applies a cryptographic SHA-256 hash to individual `user_id` and IP strings combined with a rotating salt. This ensures clickstream data remains fully PDP/GDPR compliant.
   * **Calculated Metrics**: Derives ROAS, CTR, and CPA metrics.
3. **Gold Layer (Analytical Marts)**: Computes pre-aggregated tables (`gold_campaign_daily`, `gold_channel_daily`, `gold_funnel_summary`) for sub-second dashboard performance.

---

### 4. Quantified Business Impact & Strategic Recommendations

Analysis of the unified datasets indicates key optimization opportunities:
1. **TikTok Cost Efficiency**: TikTok Ads demonstrate a **1.6x higher average ROAS** compared to Facebook Ads, driven by lower CPC ($0.10–$0.60 vs $0.15–$0.90) and higher CTR. 
2. **Evening Traffic Peak**: A statistical peak in conversion volume is validated between 18:00 and 20:00 (local timezone). We recommend allocating 15% more budget weighting during this peak to maximize conversion probability.
3. **Attribution Gap Recovery**: Implementing standard tracking UTM templates will recover the 5% of untracked conversions, shifting them out of the "Direct" classification back into their correct paid campaign dimensions.
