# Architecture

## Layers

- **Extract**: Paginated API pulls using incremental watermark.
- **Land**: JSONL files persisted locally then uploaded to GCS raw prefix.
- **Load**: BigQuery raw table keeps full payload (`JSON`) + metadata columns.
- **Transform**: SQL scripts produce typed staging and BI mart tables.
- **Consume**: self-hosted Metabase connects to mart tables in BigQuery for dashboards.

## Diagram

```mermaid
flowchart LR
    subgraph Sources["External Sources"]
        WB["World Bank API"]
        OMF["Open-Meteo Forecast API"]
        OMA["Open-Meteo Archive API"]
    end

    subgraph Runtime["Python Pipeline"]
        MAIN["src.main.run()"]
        EXT["Extractors"]
        API["APIClient"]
        LAND["Local JSONL landing"]
        DEAD["Dead-letter files"]
    end

    subgraph Storage["Storage + Warehouse"]
        GCS["GCS raw bucket"]
        RAW["analytics_raw"]
        STG["analytics_staging"]
        MART["analytics_mart"]
    end

    subgraph Ops["Production Ops"]
        CB["Cloud Build"]
        CR["Cloud Run Job"]
        SM["Secret Manager"]
        CS["Cloud Scheduler"]
    end

    BI["Metabase"]

    WB --> API
    OMF --> API
    OMA --> API
    API --> EXT
    EXT --> MAIN
    MAIN --> LAND
    MAIN --> DEAD
    LAND --> GCS
    GCS --> RAW
    MAIN --> STG
    STG --> MART
    MART --> BI
    CB --> CR
    SM --> CR
    CS --> CR
    CR -. executes .-> MAIN
```

## Dataset convention

- `raw`: immutable-ish ingestion history.
- `staging`: typed, deduped, latest-state entities.
- `mart`: BI-friendly aggregates.

## Naming convention

- Raw tables: `api_<entity>_raw`
- Staging tables: `stg_<entity>`
- Mart tables: `<business_subject>_<grain>`
