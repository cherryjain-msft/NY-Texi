# GM Data Analysis - Implementation Checklist

## 📋 Pre-Execution Checklist

### Environment Setup
- [ ] Databricks workspace configured
- [ ] Unity Catalog enabled and accessible
- [ ] Cluster created with appropriate node type
- [ ] Python environment set up (if running locally)
- [ ] Required libraries installed (pyspark, delta-spark)

### Data Access
- [ ] Volume `/Volumes/gm_demo/gm_test_schema/gm_volume/2023/` exists
- [ ] Data for 2023 is present in the volume
- [ ] Appropriate read permissions granted
- [ ] Volume format verified (Parquet/CSV/JSON)

### Unity Catalog Setup
- [ ] Catalog `gm_demo` exists or will be created
- [ ] Schema `gm_test_schema` exists or will be created
- [ ] User has CREATE TABLE permissions
- [ ] User has CREATE VOLUME permissions

---

## 🚀 Execution Checklist

### Phase 1: Discovery (Required First Time)
- [ ] Run `notebooks/01_data_discovery.py` with year=2023
- [ ] Review file formats identified
- [ ] Note schema structure
- [ ] Identify key columns (for deduplication)
- [ ] Identify date/timestamp columns (for partitioning)
- [ ] Identify metric columns (numeric fields)
- [ ] Identify dimension columns (categorical fields)
- [ ] Document data quality issues found

### Phase 2: Configuration Updates
Based on data discovery, update:

- [ ] `run_pipeline.py` line 75-80:
  - `key_columns`: Replace with actual unique identifier columns
  - `string_columns`: Add your string columns for standardization
  - `partition_cols`: Set to your date column for partitioning

- [ ] `run_pipeline.py` line 105-109:
  - `date_column`: Set to your date/timestamp column
  - `metric_columns`: List your numeric columns to aggregate
  - `dimension_columns`: List your categorical columns for grouping

- [ ] `src/gm_analysis/sql/validate_delta.py` line 212-217:
  - Update `column_thresholds` with your critical columns
  - Set acceptable null percentage for each column

### Phase 3: Bronze Layer
- [ ] Run Bronze ingestion: `python run_pipeline.py --year 2023 --step bronze`
- [ ] Verify table created: `SELECT COUNT(*) FROM gm_demo.gm_test_schema.bronze_gm_data_2023`
- [ ] Check metadata columns exist: `_ingestion_timestamp`, `_source_file`
- [ ] Review table properties: `DESCRIBE DETAIL gm_demo.gm_test_schema.bronze_gm_data_2023`

### Phase 4: Silver Layer
- [ ] Run Silver transformation: `python run_pipeline.py --year 2023 --step silver`
- [ ] Verify table created: `SELECT COUNT(*) FROM gm_demo.gm_test_schema.silver_gm_data_2023`
- [ ] Check quality columns exist: `_is_valid`, `_completeness_score`
- [ ] Verify duplicates removed (count should be ≤ Bronze)
- [ ] Review data quality metrics

### Phase 5: Data Quality Validation
- [ ] Run validation: `python run_pipeline.py --year 2023 --step validation`
- [ ] Check validation results table created
- [ ] Review validation failures (if any)
- [ ] Verify row count consistency
- [ ] Confirm schema consistency
- [ ] Check null percentages acceptable

### Phase 6: Gold Layer
- [ ] Run Gold aggregation: `python run_pipeline.py --year 2023 --step gold`
- [ ] Verify daily table: `SELECT COUNT(*) FROM gm_demo.gm_test_schema.gold_gm_data_2023_daily`
- [ ] Verify monthly table: `SELECT COUNT(*) FROM gm_demo.gm_test_schema.gold_gm_data_2023_monthly`
- [ ] Verify dimension table (if created)
- [ ] Spot-check aggregation accuracy

### Phase 7: Optimization
- [ ] Run optimization: `python run_pipeline.py --year 2023 --step optimization`
- [ ] Verify OPTIMIZE completed on all tables
- [ ] Check Z-ordering applied
- [ ] Review table statistics updated
- [ ] (Optional) Run VACUUM: `--vacuum` flag

### Phase 8: Analytics
- [ ] Run analytics: `python run_pipeline.py --year 2023 --step analytics`
- [ ] Review dashboard notebook: `notebooks/02_analytics_dashboard.py`
- [ ] Verify visualizations generated
- [ ] Check data volume trends
- [ ] Review quality metrics
- [ ] Validate business metrics

---

## 🔐 Security & Governance Checklist

### Unity Catalog Registration
- [ ] All Bronze tables registered in Unity Catalog
- [ ] All Silver tables registered in Unity Catalog
- [ ] All Gold tables registered in Unity Catalog
- [ ] Table comments added
- [ ] Column comments added (for key fields)

### Access Control
- [ ] Run `src/gm_analysis/sql/grants.sql`
- [ ] Verify Data Engineering team has full access
- [ ] Verify Analytics team has read access to Silver/Gold
- [ ] Verify Business Users have read access to Gold only
- [ ] Test access with different user roles

### Table Tagging
- [ ] Bronze tables tagged with `layer=bronze`
- [ ] Silver tables tagged with `layer=silver`
- [ ] Gold tables tagged with `layer=gold`
- [ ] Data classification tags applied
- [ ] PII flags set (if applicable)

---

## 📊 Workflow Deployment Checklist

### Databricks Asset Bundle
- [ ] Review `resources/workflows.yml` configuration
- [ ] Update cluster configuration if needed
- [ ] Update email notifications
- [ ] Test workflow locally: `databricks bundle validate`
- [ ] Deploy to development: `databricks bundle deploy --target dev`
- [ ] Test workflow: `databricks bundle run gm_medallion_pipeline_2023 --target dev`
- [ ] Deploy to production: `databricks bundle deploy --target production`

### Scheduling
- [ ] Review cron schedule (default: daily 2 AM UTC)
- [ ] Adjust schedule if needed
- [ ] Enable schedule: Set `pause_status: "UNPAUSED"` in workflows.yml
- [ ] Test schedule triggers correctly
- [ ] Set up maintenance job schedule (weekly)

### Monitoring
- [ ] Configure email notifications
- [ ] Set up alerting for failures
- [ ] Enable job metrics collection
- [ ] Create monitoring dashboard (optional)

---

## ✅ Post-Deployment Checklist

### Day 1 - Initial Validation
- [ ] Pipeline completed successfully
- [ ] No data quality violations
- [ ] All tables populated
- [ ] Access control working
- [ ] Dashboards accessible

### Week 1 - Monitoring
- [ ] Daily pipeline runs completing
- [ ] Data freshness within SLA
- [ ] No accumulating errors
- [ ] Performance acceptable
- [ ] Stakeholders can access Gold layer

### Month 1 - Optimization
- [ ] Review query performance
- [ ] Adjust Z-ordering if needed
- [ ] Review and adjust partitioning
- [ ] Optimize cluster sizing
- [ ] Review cost optimization opportunities

### Ongoing
- [ ] Weekly VACUUM to clean old files
- [ ] Monthly review of data quality trends
- [ ] Quarterly access review
- [ ] Regular documentation updates

---

## 🆘 Troubleshooting Checklist

### If Pipeline Fails
- [ ] Check logs: `pipeline_YYYYMMDD_HHMMSS.log`
- [ ] Review error messages
- [ ] Verify data source accessible
- [ ] Check cluster status
- [ ] Verify permissions
- [ ] Check for schema changes

### If Data Quality Issues
- [ ] Review validation results table
- [ ] Check null percentages
- [ ] Verify duplicate counts
- [ ] Review business rules
- [ ] Examine source data quality

### If Performance Issues
- [ ] Check table file counts: `DESCRIBE DETAIL table`
- [ ] Run OPTIMIZE if many small files
- [ ] Review partition strategy
- [ ] Check Z-ordering applied
- [ ] Verify statistics up-to-date
- [ ] Consider cluster scaling

---

## 📝 Documentation Checklist

- [ ] README_GM_ANALYSIS.md reviewed
- [ ] QUICKSTART.md followed
- [ ] PROJECT_SUMMARY.md understood
- [ ] Inline code comments reviewed
- [ ] Custom configurations documented
- [ ] Team trained on pipeline usage

---

## 🎯 Success Metrics

After implementation, verify these metrics:

- [ ] ✅ Bronze layer: 100% of source data ingested
- [ ] ✅ Silver layer: >95% data quality score
- [ ] ✅ Gold layer: <1s query response time
- [ ] ✅ Pipeline: <2 hour total execution time
- [ ] ✅ Validation: All checks passing
- [ ] ✅ Stakeholder access: All users can query Gold layer
- [ ] ✅ Documentation: Complete and accessible

---

## 📞 Support Contacts

- **Data Engineering**: data-engineering@company.com
- **Alerts/Incidents**: alerts@company.com
- **Documentation**: Link to team wiki/confluence

---

**Last Updated**: December 2025
**Status**: Ready for Execution ✅
