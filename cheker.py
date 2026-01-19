import os
import logging
from PIL import Image
import imagehash
from concurrent.futures import ProcessPoolExecutor
import json
import csv
import vptree 
from typing import Optional, Tuple
import warnings


logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
# warnings.filterwarnings("ignore", category=UserWarning, module="PIL.Image")

def export_results(clusters, all_domains, output_prefix="logo_clusters")->None:
    """
    Exports clusters to JSON and CSV
    Assigns a unique cluster_id to every domain
    """
    results_list = []
    domain_to_cluster = {}
    
    for cluster_id, filenames in enumerate(clusters):
        for filename in filenames:
            domain = os.path.splitext(filename)[0]
            domain_to_cluster[domain] = f"cluster_{cluster_id}"
            results_list.append({
                "domain": domain,
                "cluster_id": f"cluster_{cluster_id}",
                "is_singleton": False
            })

    clustered_domains = set(domain_to_cluster.keys())
    singleton_id_start = len(clusters)
    
    for domain in all_domains:
        if domain not in clustered_domains:
            uid = f"singleton_{singleton_id_start}"
            results_list.append({
                "domain": domain,
                "cluster_id": uid,
                "is_singleton": True
            })
            singleton_id_start += 1

    with open(f"{output_prefix}.json", "w") as f:
        json.dump(results_list, f, indent=4)
    
    keys = results_list[0].keys() if results_list else []
    with open(f"{output_prefix}.csv", "w", newline="") as f:
        dict_writer = csv.DictWriter(f, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(results_list)

    logging.info(f"Successfully exported results to {output_prefix}.json and {output_prefix}.csv")

def process_single_image(args)-> Tuple[str, Optional[imagehash.ImageHash]]:
    filename, directory = args
    path = os.path.join(directory, filename)
    try:
        with Image.open(path) as img:
            """
            Some images have a Palette encoding
            And breaks some images
            """
            if img.mode != 'RGBA':
                img = img.convert('RGBA')

            h = imagehash.phash(img)
            return filename, h
    except Exception:
        return filename, None

def hamming_dist(h1, h2):
    return h1 - h2


def find_clusters_production(logos_dir: str, threshold=4)->list[list[str]]:
    files = [f for f in os.listdir(logos_dir) if f.endswith(('.png', '.jpg', '.ico'))]
    
    tasks = [(f, logos_dir) for f in files]
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(process_single_image, tasks))
    
    hash_to_filenames = {}
    valid_hashes = []
    
    for filename, h in results:
        if h is not None:
            if h not in hash_to_filenames:
                hash_to_filenames[h] = []
                valid_hashes.append(h)
            hash_to_filenames[h].append(filename)

    logging.info(f"Building VP-Tree with {len(valid_hashes)} unique hashes")
    tree = vptree.VPTree(valid_hashes, hamming_dist)
    
    final_clusters = []
    visited_hashes = set()

    for h in valid_hashes:
        if h in visited_hashes:
            continue
            
        matches = tree.get_all_in_range(h, threshold)
        
        current_cluster_files = []
        for _, matched_hash in matches:
            if matched_hash not in visited_hashes:
                current_cluster_files.extend(hash_to_filenames[matched_hash])
                visited_hashes.add(matched_hash)
        
        if len(current_cluster_files) > 1:
            final_clusters.append(current_cluster_files)
            
    return final_clusters

def main(logos_dir: str, domains: list[str])->None:
    
    if not os.path.exists(logos_dir):
        print(f"Directory {logos_dir} not found")
    else:
        clusters = find_clusters_production(logos_dir)
        
        print(f"Found {len(clusters)} clusters of similar logos:")
        # print on stdout if needed
        # for i, group in enumerate(clusters):
            # print(f"Group {i+1}: {group}")
        export_results(clusters, domains)

if __name__ == "__main__":
    main("downloaded_logos", [])
        