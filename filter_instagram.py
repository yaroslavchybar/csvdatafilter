import csv
import re
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Union

from clean_data import clean_csv, detect_csv_separator

BASE_DIR = Path(__file__).parent


def load_names_from_file(filename: str | Path):
    """Loads a list of keywords from a file, ignoring comments. Resolves paths relative to this file."""
    file_path = (BASE_DIR / filename) if isinstance(filename, (str, Path)) else filename
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return {line.strip().lower() for line in f if line.strip() and not line.startswith('#')}
    except FileNotFoundError:
        print(f"Warning: The required file '{file_path}' was not found. This will reduce accuracy.")
        return set()

# --- LOAD ALL EXTERNAL DATABASES ---
# The script now depends entirely on these files for its accuracy.
MALE_NAMES_EXCEPTIONS = load_names_from_file('males_names.txt')
FEMALE_BUSINESS_KEYWORDS = load_names_from_file('female_business_keywords.txt')

# Combine the Russian and Ukrainian female name lists from the txt files.
# Make sure these files contain both Cyrillic and Latin names for best results.
FEMALE_NAMES = load_names_from_file('ukrainian_female_names.txt') | load_names_from_file('russian_female_names.txt')

# Female name endings (used as a last resort)
FEMALE_ENDINGS = {'a', 'ya', 'ia', 'ina', 'ova', 'eva', 'skaya', 'ivna', 'yivna', 'ovna'}


# --- FONT NORMALIZATION FUNCTION ---
def normalize_text(text: str) -> str:
    """Converts common special font characters to standard Latin letters."""
    normalization_map = {
        'ᴀ': 'a', 'ʙ': 'b', 'ᴄ': 'c', 'ᴅ': 'd', 'ᴇ': 'e', 'ꜰ': 'f', 'ɢ': 'g', 'ʜ': 'h',
        'ɪ': 'i', 'ᴊ': 'j', 'ᴋ': 'k', 'ʟ': 'l', 'ᴍ': 'm', 'ɴ': 'n', 'ᴏ': 'o', 'ᴘ': 'p',
        'ǫ': 'q', 'ʀ': 'r', 'ꜱ': 's', 'ᴛ': 't', 'ᴜ': 'u', 'ᴠ': 'v', 'ᴡ': 'w', 'x': 'x',
        'ʏ': 'y', 'ᴢ': 'z'
    }
    for char, replacement in normalization_map.items():
        text = text.replace(char, replacement)
    return text


# --- HIGH-ACCURACY CLASSIFICATION LOGIC ---
def classify_gender(username: str, fullname: str) -> str:
    """
    Classifies a profile using a strict, multi-step priority system.
    Returns 'female' for removal, or 'keep' to keep the profile.
    """
    combined_text = f"{username} {fullname}".lower()
    if not combined_text.strip():
        return 'keep'

    # PRIORITY 1: Normalize the text to handle special fonts.
    normalized_text = normalize_text(combined_text)

    # PRIORITY 2: Clean and split the text into words.
    cleaned_text = re.sub(r'[^a-zа-яёїієґ]+', ' ', normalized_text)
    parts = set(cleaned_text.split())

    # PRIORITY 3: Check for female business keywords.
    if any(keyword in parts for keyword in FEMALE_BUSINESS_KEYWORDS):
        return 'female'

    # PRIORITY 4: Check for male name exceptions. If found, we MUST keep it.
    if any(male_name in parts for male_name in MALE_NAMES_EXCEPTIONS):
        return 'keep'

    # PRIORITY 5: Check for high-confidence female names from your txt files.
    if any(female_name in parts for female_name in FEMALE_NAMES):
        return 'female'

    # PRIORITY 6 (Last Resort): Check for female endings.
    for part in parts:
        if len(part) > 3 and part not in MALE_NAMES_EXCEPTIONS:
            for ending in FEMALE_ENDINGS:
                if part.endswith(ending):
                    return 'female'

    return 'keep'

def filter_instagram_data(input_file: str, output_file: str):
    """Reads, filters, and writes the Instagram data."""
    try:
        sep = detect_csv_separator(input_file)
        with open(input_file, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', newline='', encoding='utf-8') as outfile:

            reader = csv.DictReader(infile, delimiter=sep)
            if not reader.fieldnames:
                print(f"Error: Could not read headers from {input_file}.")
                return

            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames, delimiter=sep)
            writer.writeheader()

            total_processed, removed_count = 0, 0
            for row in reader:
                total_processed += 1
                username = row.get("user_name", "")
                fullname = row.get("full_name", "")

                classification = classify_gender(username, fullname)

                if classification == 'female':
                    removed_count += 1
                    print(f"Removed (Female/Business): user_name='{username}', full_name='{fullname}'")
                else:
                    writer.writerow(row)

            print("\n✅ Filtering complete.")
            print(f"   Total profiles processed: {total_processed}")
            print(f"   Profiles removed: {removed_count}")
            print(f"   Remaining profiles: {total_processed - removed_count}")
            print(f"   Filtered data saved to: {output_file}")

    except FileNotFoundError:
        print(f"Error: The input file '{input_file}' was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def preprocess_csv(input_path: Union[str, Path]) -> tuple[Path, TemporaryDirectory]:
    """Run the cleaning step and return (cleaned_csv_path, temp_dir).

    Caller is responsible for keeping/cleaning the TemporaryDirectory lifecycle.
    """
    input_path = Path(input_path)
    cleaned_df, sep = clean_csv(str(input_path))
    tmpdir = TemporaryDirectory()
    out_path = Path(tmpdir.name) / f"{input_path.stem}_cleaned.csv"
    cleaned_df.to_csv(out_path, index=False, sep=sep)
    return out_path, tmpdir


def filter_csv(input_path: Union[str, Path], output_path: Union[str, Path]) -> None:
    """Public API used by the Telegram bot; clean first, then filter."""
    cleaned_path, tmpdir = preprocess_csv(input_path)
    try:
        filter_instagram_data(str(cleaned_path), str(output_path))
    finally:
        # Ensure temporary directory is removed after filtering completes
        tmpdir.cleanup()

# --- SCRIPT EXECUTION ---
if __name__ == "__main__":
    input_csv_file = "instagram_data.csv"
    output_csv_file = "filtered_instagram_data_final.csv"
    filter_csv(input_csv_file, output_csv_file)