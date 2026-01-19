import sys
import os
import logging
import downloader
import cheker

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

INPUT_FILE = "logos.snappy.parquet"
OUTPUT_DIR = "downloaded_logos"

def main()->None:
    
    input_path = sys.argv[1] if len(sys.argv) > 1 else INPUT_FILE
    
    if not os.path.exists(input_path):
        logging.error(f"Input file {input_path} not found.")
        sys.exit(1)
        
    logging.info("--- Phase 1: Image Acquisition ---")
    domains = downloader.main(input_path, OUTPUT_DIR)

    logging.info("--- Phase 2: Similarity Analysis ---")
    cheker.main(OUTPUT_DIR, domains)
    
    logging.info("--- Process Complete ---")
    
if __name__ == "__main__":
    main()