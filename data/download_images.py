import requests
import os
from multiprocessing import Pool
import pandas as pd
import argparse
from tqdm import tqdm

def download_image(data):
    
    image_dir='images'
    url, key = data
    image_path = os.path.join(image_dir, f'{key}.jpg')
    
    if not os.path.exists(image_path):
        try:
            response = requests.get(url)
        except: 
            print('Could not get the image.')
            return 
        
        if response.status_code == 200:
            with open(image_path, 'wb') as file:
                file.write(response.content)
        else:
            print(f'wtf?, {response.status_code}')

def main():
    
    parser = argparse.ArgumentParser(description='Write the number of images to download')
    parser.add_argument('--img_count', type=int, default=10000, required=False, help='an integer for the accumulator')
    parser.add_argument('--parquet_path', type=str, default='./laion-pop/laion_pop.parquet', required=False , help='Path to laion-pop parquet')
    parser.add_argument('--workers', type=int, default=32, required=False , help='Number of cpus involved')
    args = parser.parse_args()
    
    df = pd.read_parquet(args.parquet_path)
    
    image_dir = 'images'
    os.makedirs(image_dir, exist_ok=True)
    
    data_to_download = df[6900:args.img_count]
    data_to_download = [(url, key) for url, key in zip(data_to_download['url'], data_to_download['key'])]
    del df
    
    with Pool(args.workers) as pool:
        list(tqdm(pool.imap_unordered(download_image, data_to_download), total=args.img_count-6900))
    
if __name__ == '__main__':
    main()
    
    
