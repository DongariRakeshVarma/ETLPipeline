# 🚀 AWS ETL Pipeline

> End-to-end data pipeline: **S3 → PySpark Transformations → Amazon Redshift**
> Built by [Rakesh Varma Dongari](https://github.com/DongariRakeshVarma)

![Python](https://img.shields.io/badge/Python-3.10-blue?style=flat&logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?style=flat&logo=apachespark)
![AWS](https://img.shields.io/badge/AWS-Glue%20%7C%20S3%20%7C%20Redshift-yellow?style=flat&logo=amazonaws)
![License](https://img.shields.io/badge/License-MIT-green?style=flat)

---

## 📌 Overview

This project implements a production-grade **ETL (Extract, Transform, Load)** pipeline using:

- **AWS S3** as the data source (CSV / JSON / Parquet)
- **Apache Spark / PySpark** for large-scale data transformation
- **AWS Glue** for serverless job orchestration
- **Amazon Redshift** as the data warehouse destination

Key results from production use at Amazon:
- ⚡ Reduced data processing time by **40%**
- 📈 Improved data availability by **60%** via real-time workflows

---

## 🏗️ Architecture

```
S3 (Raw Data)
     │
     ▼
PySpark / AWS Glue
  ├── Drop nulls
  ├── Standardize text
  ├── Parse dates
  ├── Handle negatives
  └── Add audit columns
     │
     ├──▶ S3 (Processed Parquet)
     └──▶ Amazon Redshift
```

---

## 📁 Project Structure

```
etl-pipeline/
├── src/
│   ├── etl_pipeline.py     # Core ETL logic (local / EMR)
│   └── glue_job.py         # AWS Glue serverless job
├── config/
│   └── settings.py         # Config loaded from env vars
├── tests/
│   └── test_etl_pipeline.py # Unit tests (pytest + PySpark)
├── .env.example             # Environment variable template
├── requirements.txt
└── README.md
```

---

## ⚙️ Setup & Installation

### 1. Clone the repo
```bash
git clone https://github.com/DongariRakeshVarma/etl-pipeline.git
cd etl-pipeline
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure environment variables
```bash
cp .env.example .env
# Edit .env with your AWS credentials and Redshift details
```

### 4. Run the pipeline
```bash
python src/etl_pipeline.py
```

---

## 🧪 Run Tests

```bash
pytest tests/ -v
```

---

## ☁️ Deploy to AWS Glue

1. Upload `src/glue_job.py` to your S3 bucket
2. Create a new Glue Job in AWS Console
3. Point the script path to your S3 location
4. Set job parameters:

| Parameter | Value |
|-----------|-------|
| `--S3_INPUT` | `s3://your-bucket/raw/data.csv` |
| `--S3_OUTPUT` | `s3://your-bucket/processed/` |
| `--REDSHIFT_TABLE` | `public.etl_output` |

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.10 |
| Processing | Apache Spark 3.5 / PySpark |
| Orchestration | AWS Glue |
| Storage | Amazon S3 |
| Warehouse | Amazon Redshift |
| Testing | pytest |
| Config | python-dotenv |

---

## 👤 Author

**Rakesh Varma Dongari**
- GitHub: [@DongariRakeshVarma](https://github.com/DongariRakeshVarma)
- LinkedIn: [rakesh-varma-11a80221a](https://www.linkedin.com/in/rakesh-varma-11a80221a/)
- Email: rakeshsuryavarma26@gmail.com

---

## 📄 License

MIT License — feel free to use and adapt.
