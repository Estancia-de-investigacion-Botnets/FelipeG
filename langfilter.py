import os
import sys
import argparse
from typing import Optional

import pandas as pd
import langid


def is_english_text(text: Optional[str]) -> bool:
    """Return True if text is confidently classified as English.

    Empty, null, or non-string values are treated as non-English.
    """
    if not isinstance(text, str):
        return False
    candidate = text.strip()
    if not candidate:
        return False
    try:
        # Use a substring to reduce processing time on very long tweets
        language, score = langid.classify(candidate[:512])
        return language == "en"
    except Exception:
        return False


def filter_csv_to_english(input_csv: str, output_csv: str, chunksize: int = 100_000) -> None:
    """Stream filter input_csv keeping only rows whose `tweet_text` is English.

    Writes the filtered rows to output_csv.
    """
    output_dir = os.path.dirname(output_csv)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    total_rows = 0
    kept_rows = 0
    write_header = True

    reader = pd.read_csv(
        input_csv,
        chunksize=chunksize,
        dtype=str,  # keep all columns as string to avoid dtype inference overhead
        encoding="utf-8",
        encoding_errors="ignore",
        low_memory=False,
    )

    for chunk in reader:
        total_rows += len(chunk)

        if "tweet_text" not in chunk.columns:
            raise KeyError(
                "Column 'tweet_text' not found in input CSV. Available columns: "
                + ", ".join(chunk.columns.astype(str))
            )

        mask_english = chunk["tweet_text"].apply(is_english_text)
        filtered = chunk.loc[mask_english]
        kept_rows += len(filtered)

        mode = "w" if write_header else "a"
        filtered.to_csv(
            output_csv,
            index=False,
            mode=mode,
            header=write_header,
            encoding="utf-8",
        )
        write_header = False

    print(f"Completed. Input rows: {total_rows:,} | Kept English rows: {kept_rows:,}")
    if total_rows > 0 and kept_rows == 0:
        print(
            "Warning: No English tweets detected. Verify the 'tweet_text' column content.",
            file=sys.stderr,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Filter 'user_tweets_full_enriched.csv' to keep only tweets in English "
            "based on the 'tweet_text' column."
        )
    )
    default_input = os.path.join(
        os.path.dirname(__file__),
        "PhDBotnetsDB",
        "User_Tweet",
        "_outputs",
        "user_tweets_full_enriched.csv",
    )
    default_output = os.path.join(
        os.path.dirname(__file__),
        "PhDBotnetsDB",
        "User_Tweet",
        "_outputs",
        "user_tweets_full_enriched_english_only.csv",
    )
    parser.add_argument(
        "--input",
        default=default_input,
        help="Ruta al CSV de entrada (por defecto: _outputs/user_tweets_full_enriched.csv)",
    )
    parser.add_argument(
        "--output",
        default=default_output,
        help=(
            "Ruta de salida del CSV filtrado (por defecto: "
            "_outputs/user_tweets_full_enriched_english_only.csv)"
        ),
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=100_000,
        help="Número de filas por bloque al procesar en streaming",
    )

    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"No se encontró el archivo de entrada: {args.input}", file=sys.stderr)
        sys.exit(1)

    filter_csv_to_english(args.input, args.output, args.chunksize)


if __name__ == "__main__":
    main()


