# Logo Similarity Challenge

The goal of the task was to extract company logos from a list of domains and group similar ones, without using ML clustering algorithms (e.g. DBSCAN, K-Means).

## Approach

**1. Logo Acquisition**
- sources: Clearbit + Google favicon fallback
- domain normalization
- caching + skip for already downloaded files
- concurrency and rate limiting

**2. Similarity & Clustering**
- perceptual hashing (aHash)
- Hamming distance
- VP-Tree for nearest-neighbor search
- threshold-based cluster building (non-ML)

## Project Structure
- `downloader.py` — fetch logos
- `cheker.py` — hashing + clustering
- `main.py` — pipeline orchestration
- `logo_clusters.csv/json` — final output
- `logos.snappy.parquet` — dataset

## Running
- python3 -m venv .venv
- source .venv/bin/activate
- python3 main.py
