# Real-Time Flight Data Ingestion & ETL Pipeline Using Databricks

## Project Overview

This project explores how to ingest open-source flight tracking data into Databricks using PySpark and a custom OpenSky data source. The objective is to understand how continuously updated flight data can enter a data pipeline, be processed incrementally, and be analyzed using aggregation-based statistics.

The project focuses on the ingestion stage of an ETL workflow and the initial exploration of data quality and pipeline monitoring metrics.

As part of my ongoing learning in Databricks, I am building this project to understand how data engineering workflows support data analytics, from source ingestion to transformation and reporting.

## Objectives

- Understand how to ingest flight data from an open-source data source into Databricks.
- Explore streaming ingestion using Spark Structured Streaming.
- Register and use a custom PySpark data source.
- Build a declarative data pipeline using the Databricks Pipelines API.
- Calculate descriptive statistics to understand the ingested dataset.
- Explore data quality checks and monitoring metrics for downstream analytics.

## Technology Stack

| Technology | Purpose |
|---|---|
| Databricks | Data platform and pipeline development environment |
| PySpark | Distributed data processing and transformations |
| Spark Structured Streaming | Streaming data ingestion |
| Databricks Pipelines API | Declarative pipeline table definition |
| OpenSky data source | Flight-data ingestion source |
| GitHub | Version control and project documentation |

## Pipeline Architecture

The current workflow begins with ingestion and proceeds to an aggregation-based statistics layer.

```text
OpenSky Flight Data Source
          |
          v
Custom OpenSky Data Source
          |
          v
Databricks Streaming Ingestion
          |
          v
ingest_flights
          |
          v
PySpark Aggregations
          |
          v
Flight Pipeline Statistics
```

**Note:** The diagram represents the intended logical workflow. The statistics layer is being developed separately from the ingestion layer.

## 1. Data Ingestion

The ingestion script registers the custom OpenSky data source and defines a streaming table.

```python
from pyspark import pipelines as dp
from pyspark_datasources import OpenSkyDataSource

spark.dataSource.register(OpenSkyDataSource)

@dp.table
def ingest_flights():
    return spark.readStream.format("opensky").load()
```

### How it works

1. **Import pipeline functionality:** The Databricks Pipelines API is used to define a managed pipeline table.
2. **Register the data source:** `OpenSkyDataSource` is registered with Spark so the custom `opensky` format can be used.
3. **Read as a stream:** `spark.readStream` configures the source for streaming ingestion rather than a conventional batch read.
4. **Define the ingestion table:** The `@dp.table` decorator declares `ingest_flights` as a pipeline-managed table.

The script relies on the custom OpenSky data source being available and correctly configured in the Databricks environment. Actual refresh frequency, source polling, checkpointing, and data availability depend on the source implementation and pipeline configuration.

## 2. Data Transformation and Statistics

The next stage calculates summary statistics from the ingested flight data.

The initial metrics include:

- Total number of flight records.
- Number of distinct aircraft using `icao24`.
- Maximum observed velocity.

The statistics layer can be extended to include:

- Minimum, maximum, average, and standard deviation of numeric measurements.
- Null values in important fields.
- Missing aircraft identifiers.
- Flight measurement anomalies.
- Earliest and latest event timestamps.
- Geographic coordinate validation.
- Duplicate event detection and data freshness checks.

These metrics help assess the characteristics and quality of the data before it is used for analytical reporting.

**Implementation note:** The exact statistics and their execution behavior depend on the current transformation code and the pipeline's streaming or materialized-view configuration.

## 3. Why This Project Matters for Data Analytics

Although the project introduces concepts commonly associated with data engineering, the workflow is also relevant to data analytics.

Understanding ingestion and transformation helps an analyst investigate:

- Where the data originates.
- How frequently the source is updated.
- Whether the dataset contains missing or suspicious values.
- How summary metrics can reveal anomalies.
- Whether the data is suitable for downstream analysis and visualization.

For flight data, these concepts can eventually support analyses of aircraft activity, observed flight speeds, geographic distributions, and changes in flight activity over time, subject to the fields and coverage available in the source.

## Current Scope

This is a learning project focused on understanding Databricks and the flow of data through an ETL pipeline.

The current work covers:

- [x] Custom data-source registration.
- [x] Streaming ingestion table definition.
- [x] Initial PySpark aggregation design.
- [ ] Expanded data-quality and validation checks.
- [ ] Duplicate-event detection.
- [ ] Data freshness monitoring.
- [ ] Downstream analytical visualizations.

The checklist reflects the intended project scope; update each item as the corresponding functionality is implemented and tested.

## Future Enhancements

Potential next steps include:

1. **Medallion architecture:** Explore Bronze, Silver, and Gold layers for raw ingestion, cleaned data, and analytical outputs.
2. **Data quality:** Add validation rules for aircraft identifiers, timestamps, geographic coordinates, and measurement values.
3. **Incremental processing:** Investigate how new records are processed and how the pipeline handles repeated events.
4. **Pipeline monitoring:** Track record counts, missing values, event-time coverage, and processing failures.
5. **Analytics:** Build dashboards or summary reports to explore flight activity patterns.

## Learning Disclaimer

This project is part of my ongoing learning journey with Databricks, PySpark, and data pipeline development. It is intended to demonstrate my understanding of the ingestion workflow and foundational transformation concepts.

It is not presented as a production-ready flight tracking system. Source availability, streaming behavior, data quality, and operational reliability require further validation.

## Author

**Nikita Ravindra Albela**

Aspiring Data Analyst | Exploring Databricks, PySpark, ETL Workflows, and Data Quality

GitHub: [Nikita-Ravindra-Albela](https://github.com/Nikita-Ravindra-Albela)
