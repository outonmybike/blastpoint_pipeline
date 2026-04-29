import ingest
import clean
import enrich
import aggregate


if __name__ == "__main__":
    # Stage 1
    ingest.ingest_to_raw() 
    clean.raw_to_bronze()

    # Stage 2
    enrich.silver()    

    # Stage 3
    aggregate.gold()