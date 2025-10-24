# Lakeflow Parallel Web Search Agents

A Databricks Asset Bundle (DAB) project that demonstrates parallel processing of web search tasks using Claude API and Databricks Lakeflow workflows.

## Overview

This project creates an automated workflow that:

1. Maintains a Unity Catalog table of self-driving car features with Change Data Feed (CDF) enabled
2. Retrieves feature records from the table
3. Processes each feature in parallel using Claude API with web search to find latest news and updates
4. Updates the table with new information when found
5. Maintains a historical SCD Type 2 table using Change Data Feed

## Architecture

### Data Flow

```
Unity Catalog Table (feature_comparison)
    ↓
Get Feature IDs → Pass to For-Each Loop
    ↓
[Parallel Processing with Claude API Web Search]
    ↓
Update Feature News in UC Table
    ↓
Read Change Data Feed → Create SCD Type 2 History Table
```

### Components

#### Notebooks

1. **01_initialize_table.py** (Setup Only - Not in Workflow)
   - Creates Unity Catalog table: `this_is_for_tahir_only.robocars.feature_comparison`
   - Enables Change Data Feed
   - Populates with 100 rows of self-driving car feature data

2. **02_get_feature_ids.py**
   - Reads all `core_feature_id` values from the Unity Catalog table
   - Passes the list to `dbutils.jobs.taskValues` for the for-each loop

3. **03_process_feature_with_claude.py**
   - Takes `core_feature_id` as a parameter
   - Reads the corresponding row from the table
   - Uses Claude API with web search tool to check for new news
   - Updates the row if new information is found

4. **04_create_scd_type2_table.py**
   - Reads Change Data Feed from the source table
   - Creates/updates SCD Type 2 table: `this_is_for_tahir_only.robocars.feature_comparison_history`
   - Maintains historical records with effective dates and current flags

#### Lakeflow Job

The `databricks.yml` defines a Lakeflow job with:
- **Task 1**: Get Feature IDs
- **Task 2**: Process Features in Parallel (for-each loop)
- **Task 3**: Create SCD Type 2 History

## Prerequisites

### Databricks Workspace Setup

1. **Unity Catalog**
   - Catalog: `this_is_for_tahir_only`
   - Schema: `robocars`

2. **Claude API Key**
   - Create a secret scope: `claude-api`
   - Add your Claude API key with key name: `api-key`

   ```bash
   databricks secrets create-scope --scope claude-api
   databricks secrets put --scope claude-api --key api-key
   ```

3. **Databricks CLI**
   - Install and authenticate with your workspace
   - Use OAuth authentication (see CLAUDE.md for details)

### Required Libraries

- `anthropic` Python package (automatically installed by the job)

## Setup Instructions

### 1. Initialize the Feature Table

Run the initialization notebook manually (only once):

```bash
databricks workspace import ./src/01_initialize_table.py /Workspace/Users/tahir.fayyaz@databricks.com/lakeflow_parallel_web_search_agents/01_initialize_table.py
```

Or run it directly in the Databricks workspace to create and populate the table.

### 2. Deploy the DAB

```bash
cd lakeflow_parallel_web_search_agents

# Validate the bundle
databricks bundle validate

# Deploy to development
databricks bundle deploy -t dev

# Run the workflow
databricks bundle run feature_news_update_workflow -t dev
```

### 3. Monitor the Workflow

View the job in the Databricks workspace:
- Go to Workflows
- Find "[dev] Feature News Update Workflow"
- Monitor task execution and view logs

## Table Schemas

### feature_comparison

| Column | Type | Description |
|--------|------|-------------|
| core_feature_id | INT | Unique identifier for the feature |
| car_manufacturer | STRING | Manufacturer name (e.g., Tesla, Waymo) |
| car_name | STRING | Full car name |
| car_model | STRING | Specific model |
| core_feature | STRING | Feature name (e.g., "Autopilot") |
| core_feature_news | STRING | Latest news about the feature |
| news_updated | TIMESTAMP | When the news was last updated |

**Table Properties**: `delta.enableChangeDataFeed = true`

### feature_comparison_history (SCD Type 2)

| Column | Type | Description |
|--------|------|-------------|
| core_feature_id | INT | Feature identifier |
| car_manufacturer | STRING | Manufacturer name |
| car_name | STRING | Full car name |
| car_model | STRING | Specific model |
| core_feature | STRING | Feature name |
| core_feature_news | STRING | News content (historical) |
| news_updated | TIMESTAMP | Original update timestamp |
| effective_start_date | TIMESTAMP | When this version became effective |
| effective_end_date | TIMESTAMP | When this version was superseded (NULL for current) |
| is_current | BOOLEAN | True for the current version |
| surrogate_key | BIGINT | Unique key for each historical record |

## Configuration

### Environment Variables

The `databricks.yml` supports multiple environments:

- **dev** (default): Development environment
- **prod**: Production environment

### Job Schedule

The workflow is configured to run daily at 2 AM UTC but starts in PAUSED status. Unpause when ready:

```bash
databricks jobs update --job-id <job-id> --pause-status UNPAUSED
```

### Parallelization

The for-each loop processes features with a maximum concurrency of 10 tasks. Adjust in `databricks.yml`:

```yaml
max_concurrent_runs: 10
```

## Claude API Integration

The workflow uses Claude 3.5 Sonnet with the web search tool:

```python
response = client.messages.create(
    model="claude-3-5-sonnet-20241022",
    max_tokens=1024,
    tools=[
        {
            "type": "web_search_20241022",
            "name": "web_search",
            "max_uses": 5
        }
    ],
    messages=[...]
)
```

## Troubleshooting

### Common Issues

1. **Secret not found**: Ensure the `claude-api` scope and `api-key` are set up correctly
2. **Table not found**: Run the initialization notebook (01_initialize_table.py) first
3. **Permission errors**: Verify Unity Catalog permissions for the catalog and schema

### Viewing Change Data Feed

```sql
SELECT * FROM table_changes('this_is_for_tahir_only.robocars.feature_comparison', 0)
```

### Checking SCD Type 2 History

```sql
-- View all historical records for a feature
SELECT *
FROM this_is_for_tahir_only.robocars.feature_comparison_history
WHERE core_feature_id = 1
ORDER BY effective_start_date DESC
```

## Development

### Project Structure

```
lakeflow_parallel_web_search_agents/
├── databricks.yml          # DAB configuration
├── README.md               # This file
└── src/
    ├── 01_initialize_table.py           # Initial setup (run manually)
    ├── 02_get_feature_ids.py            # Task 1: Get feature IDs
    ├── 03_process_feature_with_claude.py # Task 2: Process with Claude API
    └── 04_create_scd_type2_table.py     # Task 3: Create SCD Type 2
```

### Making Changes

1. Modify notebooks in `src/`
2. Update `databricks.yml` if changing workflow structure
3. Validate and deploy:

```bash
databricks bundle validate
databricks bundle deploy -t dev
```

## Cost Considerations

- **Claude API**: Each feature query uses the web search tool (costs apply per search)
- **Databricks**: Compute costs based on cluster size and runtime
- **Parallelization**: Higher concurrency = faster completion but higher peak compute costs

## Future Enhancements

- [ ] Add retry logic for failed Claude API calls
- [ ] Implement incremental CDF processing (track last processed version)
- [ ] Add data quality checks and validation
- [ ] Create dashboard for monitoring feature updates
- [ ] Add alerting for significant news updates

## License

Internal Databricks project for demonstration purposes.

## Contact

For questions or issues, contact: tahir.fayyaz@databricks.com
