import asyncio
import aiohttp
import os
import logging
import pandas as pd
from aiohttp import ClientTimeout

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("downloader.log"), logging.StreamHandler()]
)

CONCURRENT_LIMIT = 10
TIMEOUT = ClientTimeout(total=15, connect=5)

async def fetch_logo(session, domain, semaphore, output_dir)->str:
    """
    fetcher with rate limiting and
    checkpointing
    """
    file_path = os.path.join(output_dir, f"{domain}.png")
    
    if os.path.exists(file_path):
        return "skipped"

    sources = [
    f"https://www.google.com/s2/favicons?sz=128&domain={domain}",
    f"https://logo.clearbit.com/{domain}",
]

    async with semaphore:
        for url in sources:
            try:
                async with session.get(url, timeout=TIMEOUT) as response:
                    if response.status == 200:
                        content = await response.read()
                        if len(content) > 500:
                            with open(file_path, "wb") as f:
                                f.write(content)
                            return "success"
                    elif response.status == 429:
                        logging.warning(f"Rate limited by {url}")
                        await asyncio.sleep(5)
            except Exception as e:
                logging.debug(f"Failed {url}: {str(e)}")
                continue
    return "failed"

async def download_manager(domains, output_dir):
    semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)
    headers = {"User-Agent": "VeridionChallengeBot/1.0"}
    
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [fetch_logo(session, d, semaphore, output_dir) for d in domains]
        
        results = {"success": 0, "failed": 0, "skipped": 0}
        for task in asyncio.as_completed(tasks):
            status = await task
            results[status] += 1
            
            total_done = sum(results.values())
            if total_done % 50 == 0:
                logging.info(f"Progress: {total_done}/{len(domains)} (S:{results['success']} F:{results['failed']} Skip:{results['skipped']})")

def main(input_file: str, output_dir: str)->list[str]:
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        df = pd.read_parquet(input_file)
        raw = df['domain'].dropna().astype(str).str.strip()
        domains = raw[raw != ""].unique().tolist()

        logging.info(f"Loaded {len(domains)} domains from {input_file}")
        
        asyncio.run(download_manager(domains, output_dir))
        
        return domains
    except Exception as e:
        logging.error(f"Critical Error in main: {e}")
        return []

if __name__ == "__main__":
    main("logos.snappy.parquet", "downloaded_logos")
    
