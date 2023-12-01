from datasets import load_dataset


def main():
    dataset = load_dataset("laion/laion-pop", "laion_pop.parquet")
    del dataset
    
if __name__=='__main__':
    main()