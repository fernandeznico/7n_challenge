# Yalo Challenge

## Task 1 | Data Lake | Iowa Liquor Retail Sales 

### Data Lineage

 * Implemented

```mermaid
flowchart LR;
    subgraph BigQuery
        sales[[iowa_liquor_sales.sales]]
    end
    subgraph S3:
        RAW[\RAW/]
    end
    subgraph Snowflake:
        STAGING[[STAGING]]
        ANALYTICS[[ANALYTICS]]
    end
    sales --Airflow--> RAW
    RAW --Snowflake--> STAGING
    STAGING --Snowflake--> ANALYTICS
```

 * Recommended Alternative: Use [DataSync](https://docs.aws.amazon.com/datasync/latest/userguide/tutorial_transfer-google-cloud-storage.html)

```mermaid
flowchart LR;
    subgraph BigQuery
        direction TB
        sales[[iowa_liquor_sales.sales]]
        Bucket[\Bucket/]
    end
    subgraph S3:
        RAW[\RAW/]
    end
    sales --> Bucket
    Bucket --DataSync-->RAW
```

### Data Layers
```mermaid
flowchart LR;
    RAW --> STAGING --> ANALYTICS
```

 * RAW
   * Ingested data without transformations
 * STAGING
   * Working zone for transformations like
     * Cleaning
     * Standardization
     * Create new data
 * ANALYTICS
   * Exposed layer
   * Business tables

## Task 3 | Chuck Norris Jokes

 * [Chuck Norris Jokes DAG code →](dags/dag_chuck_norris_jokes/chuck_norris_jokes.py)