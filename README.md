# Fabric Wind Power - Wind Turbine Analytics & Monitoring

> An **ELT** (Extract, Load, Transform) project leveraging **Microsoft Fabric** with a **medallion architecture** (Bronze → Silver → Gold) for analyzing and monitoring wind power generation data.

---

## 📋 Overview

This project collects, transforms, and analyzes wind power generation data in real-time. It uses a modern data architecture built on Microsoft Fabric's lakehouse platform, enabling optimal scalability and performance.

### Architecture

```
DATA FLOW: Raw Data → Cleansing → Aggregation → Semantic Model → Power BI Report
             ↓           ↓            ↓              ↓                ↓
         🥉 BRONZE    ⚙️ SILVER   ✨ GOLD    📊 SEMANTIC MODEL   📈 REPORT
       (Raw Data) (Structured) (Optimized)    (Dimensions &      (Final)
                                               Fact Tables)    Deliverable
```

---

## 🏗️ Project Structure

### 📦 **Lakehouses (3-Tier)**

| Tier | Lakehouse | Description |
|------|-----------|-------------|
| 🥉 **Bronze** | `LH_WindPower_Bronze` | Raw data extracted from sources (Delta tables) |
| ⚙️ **Silver** | `LH_WindPower_Silver` | Cleaned, enriched, and structured data |
| ✨ **Gold** | `LH_WindPower_Gold` | Aggregated data optimized for analytics (DirectLake) |

### 📓 **Transformation Notebooks**

| Notebook | Input | Output | Purpose |
|----------|-------|--------|---------|
| `Bronze_to_Silver` | Bronze Lakehouse | Silver Lakehouse | Data cleaning, validation, and enrichment |
| `Silver_to_Gold` | Silver Lakehouse | Gold Lakehouse | Data aggregation and analytics preparation |

**Technology Stack**: PySpark

### 📊 **Semantic Model**

The semantic layer (`WindPower_semantic.SemanticModel`) connects the Gold Lakehouse data to Power BI:

```
Fact_WindPower (Fact Table)
├── Dimensions
│   ├── dim_date       (Date hierarchy for time analysis)
│   ├── dim_time       (Intra-day time tracking)
│   ├── dim_turbine    (Turbine properties & metadata)
│   └── dim_operational_status (Turbine operational states)
└── Measures
    ├── production_id  (Record identifier)
    ├── wind_speed     (Aggregated wind velocity)
    ├── wind_direction (Directional analysis)
    └── energy_produced (Key business metric)
```


### 📈 **Final Report & Visualizations**

**`WindPower_Analysis_reporting.Report`** - Power BI interactive dashboard (PBIR format):

| Feature | Description |
|---------|-------------|
| 📊 Production Monitoring | Real-time energy output tracking by turbine |
| 🔧 Performance Analytics | Per-turbine efficiency and KPI analysis |
| 🌬️ Weather Correlation | Wind speed/direction vs. energy output correlation |
| 📈 Trend Analysis | Historical patterns and performance forecasting |
| 🎨 Interactive Visuals | Custom Power BI visualizations with drill-down capability |

---

## 🚀 Getting Started

### Prerequisites
- Active **Microsoft Fabric** workspace
- **Access** to Bronze, Silver, and Gold Lakehouses
- **Permissions** to execute Notebooks

### Complete Data Pipeline Flow

```
Raw Data → 🥉 BRONZE → ⚙️ SILVER → ✨ GOLD → 📊 SEMANTIC MODEL → 📈 REPORT
         Load      Cleanse    Aggregate   DirectLake      Visualize
```

### Step-by-Step Execution

1. **Create Lakehouses** (if not already done)
   - Create `LH_WindPower_Bronze`, `LH_WindPower_Silver`, and `LH_WindPower_Gold` in your Fabric workspace

2. **Load Raw Data** → Bronze Lakehouse (`windpower` table)
   - **Manual Load**: Upload raw wind power data files to the Bronze Lakehouse
   - Supported formats: CSV, Parquet, Delta
   - Target table: `dbo.windpower`
   - Data source location: Check the project's `data_source/` folder for sample data or existing datasets

3. **Execute**: `Bronze_to_Silver` Notebook
   - Data validation and cleaning
   - Schema enforcement
   - Quality checks

4. **Validate**: Tables in Silver Lakehouse (`wind_power` table)
   - Verify record counts match Bronze (accounting for filtered records)
   - Check data quality metrics

5. **Execute**: `Silver_to_Gold` Notebook
   - Data aggregation
   - Deduplication
   - Performance optimization

6. **Build**: Semantic Model (connects Gold Lakehouse)
   - Creates fact/dimension relationships
   - Defines measures and hierarchies

7. **Visualize**: Power BI Report (FINAL OUTPUT)
   - Interactive dashboards
   - Real-time monitoring
   - Business insights and KPIs

---

## 📐 Data Schema

### Fact Table: `Fact_WindPower`

| Column | Type | Description |
|--------|------|-------------|
| `production_id` | INT64 | Unique production record identifier |
| `date_id` | DateTime | Date of the reading |
| `time_id` | STRING | Time of the reading |
| `turbine_id` | INT64 | Turbine identifier |
| `status_id` | INT64 | Turbine operational status |
| `wind_speed` | DOUBLE | Wind speed (m/s) |
| `wind_direction` | STRING | Wind direction (N, NE, E, etc.) |
| `energy_produced` | DOUBLE | Energy produced (kWh) |

---

## 🔧 Configuration & Maintenance

### Transformation Execution

Notebooks use **automated Delta Lake paths**:

```python
# Bronze → Silver
bronze_path = "abfss://portfolio_WindPower@onelake.dfs.fabric.microsoft.com/.../LH_WindPower_Bronze/.../windpower"

# Silver → Gold
silver_path = "abfss://portfolio_WindPower@onelake.dfs.fabric.microsoft.com/.../LH_WindPower_Silver/.../wind_power"
```

### Monitoring

- **Track** Notebook execution runs in Fabric workspace
- **Validate** row counts across tiers
- **Monitor** Power BI report performance

---

## 📝 Development Notes

- ✅ Display statements active during development (remove in production)
- ✅ Scalable architecture with DirectLake for optimal performance
- ✅ Complete end-to-end pipeline: Data → Lakehouse → Semantic Model → Power BI Report
- ✅ Clear separation of concerns across Bronze, Silver, and Gold layers
- 📊 Semantic model enables real-time analytics without data duplication
- 🔄 To implement: Automated Notebook scheduling and incremental refresh policies

---

## 📚 Resources

- [Microsoft Fabric Documentation](https://learn.microsoft.com/fabric)
- [Lakehouse Architecture Pattern](https://learn.microsoft.com/fabric/onelake/onelake-lakehouse)
- [PySpark on Fabric](https://learn.microsoft.com/fabric/data-engineering/spark-compute)

---

