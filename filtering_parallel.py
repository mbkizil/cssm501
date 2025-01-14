import json
import ast
from tqdm import tqdm
from itertools import islice
import string


def safe_parse_matched_keywords(matched_kw):
    """
    Safely parse the `matched_keywords` field into a Python list of clean tokens.
    Removes trailing commas, periods, etc. from each token.
    """
    if matched_kw is None:
        return []

    if isinstance(matched_kw, list):
        # Already a list; just clean trailing punctuation
        return _clean_tokens(matched_kw)

    if isinstance(matched_kw, str):
        s = matched_kw.strip()
        if not s:
            return []
        try:
            # Example: "['Kürt,', 'PKK,']" -> literal_eval -> ['Kürt,', 'PKK,']
            parsed_value = ast.literal_eval(s)
            if isinstance(parsed_value, list):
                return _clean_tokens(parsed_value)
            else:
                # e.g. "Kürt," -> split() -> ['Kürt,']
                tokens = str(parsed_value).split()
                return _clean_tokens(tokens)
        except:
            # e.g. "Kürt PKK," -> split -> ['Kürt', 'PKK,']
            tokens = s.split()
            return _clean_tokens(tokens)

    # Fallback
    return []


def _clean_tokens(token_list):
    """
    Remove trailing commas or punctuation from each token in token_list.
    """
    cleaned = []
    for t in token_list:
        # Strip trailing punctuation like commas, periods, etc.
        # string.punctuation is: !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~
        cleaned_token = t.strip().strip(string.punctuation)
        # If you only want to remove commas, you could do: .rstrip(',')
        if cleaned_token:
            cleaned.append(cleaned_token)
    return cleaned


# -----------------------------
# Define your two keyword sets
# -----------------------------
kurdish_keywords = {
    "kürt",  # lower-case example
    "kürt",
    "kürtler",
    "hadep",
    "hdp",
    "bdp",
    "ysp",
    "dem",
    "demokratik islam kongresi",
    "sivil cuma",
    "özerklik",
    "barzani",
    "talabani",
    "rojava",
    "kobane",
    "kobani",
    "kdp",
    "ynk",
    "ypg",
    "terörist",
    "ermeni",
    "süryani",
    "kürt sorunu",
    "selahattin demirtaş",
    "apo",
    "selo",
    "öcalan",
    "kdp",
    "kurd",
    "Kurd",
    "kurdish",
    "pkk",
    "kck",
    "kürdistan",
    "bölücülük",
    "şeyh sait",
    "şeyh said",
    "terör örgütü",
    "terör propogandası",
    "terör" "kandil",
    "selo",
    "vatansız",
    # ... add other relevant Kurdish-related keywords
    "kürdistanlı",
    "kürdistan",
    "kobani",
    "direnişi" "rojava",
    "devrimi" "terörle",
    "mücadele",
    "kürd",
    "kürtler",
    "terörizm",
    "ilişkisi",
    "kürt siyaseti",
    "kürdistan",
}

islam_keywords = {
    "islam",
    "islam",
    "müslüman",
    "musluman",
    "din",
    "dinsiz",
    "dindar",
    "islamcı",
    "müslümanlar",
    "ümmet",
    "seküler",
    "şeriat",
    "müslümanız",
    "dinimiz bir",
    "dinimizden uzak",
    "hepimiz müslüman",
    "gerçek müslüman",
    "gerçek islam",
    "islamı satanlar",
    "islam düşmanı",
    "kafir",
    "dinimizden",
    "allahsız",
    "ateist",
    "şeyh",
    "molla",
    # ... add other relevant Islam-related keywords
    "islam",
    "İslam",
    "moslem",
    "müslüman",
    "musluman",
    "muselman",
    "müslüm",
    "din",
    "dinn",
    "dinim",
    "dinsiz",
    "dinsizz",
    "dindar",
    "dindarr",
    "islamcı",
    "islamcıl",
    "müslümanlar",
    "müslimanlar",
    "ümmet",
    "ümmett",
    "ümme",
    "seküler",
    "sekuler",
    "secular",
    "şeriat",
    "şeriaat",
    "müslümanız",
    "müslümünüz",
    "dinimiz bir",
    "dinimizden uzak",
    "hepimiz müslüman",
    "hepimiz musluman",
    "gerçek müslüman",
    "gerçek islam",
    "islamı satanlar",
    "islam düşmanı",
    "kafir",
    "kafer",
    "dinimizden",
    "allahsız",
    "allahsız",
    "ateist",
    "ateıst",
    "şeyh",
    "şeyyh",
    "molla",
    "mollaa",
    "imam",
    "hoca",
    "hocaa",
    "kuran",
    "kur'an",
    "quran",
    "peygamber",
    "peygamaber",
    "pbuh",
    "sav",
    "allah",
    "alllah",
    "allaaah",
    "cc",
    "c.c.",
    "inşallah",
    "inş",
    "maşallah",
    "maş",
    "mashallah",
}

# JSONL input and output files
INPUT_FILE = "kurdish_issue_data.jsonl"
OUTPUT_FILE = "filtered_kurdish_islam_v2.jsonl"

total_tweets = 0
kept_tweets = 0


def check_tweet(line):
    try:
        tweet = json.loads(line)
    except json.JSONDecodeError:
        # Satır bozuksa atla
        return {"pass": False}

    # Parse matched_keywords reliably (could be list or string)
    matched_raw = tweet.get("matched_keywords", None)
    matched_list = safe_parse_matched_keywords(matched_raw)
    for i in range(len(matched_list)):
        matched_list[i] = matched_list[i].lower()
    # Check if there's at least one Kurdish-related kw
    kurdish_match = any(k.lower() in matched_list for k in kurdish_keywords)
    # Check if there's at least one Islam-related kw
    islam_match = any(k.lower() in matched_list for k in islam_keywords)

    if kurdish_match and islam_match:
        # This tweet has at least one keyword from each set
        # -> write it to the new JSONL
        return {"pass": True, "line": line}

    tweet_txt = tweet.get("tweet_text", "").lower()
    kurdish_txt_match = any(k.lower() in tweet_txt for k in kurdish_keywords)
    islam_txt_match = any(k.lower() in tweet_txt for k in islam_keywords)

    if kurdish_txt_match and islam_txt_match:
        return {"pass": True, "line": line}

    return {"pass": False}


from tqdm import tqdm
import json
from multiprocessing import Pool

p = Pool(12)

with open(INPUT_FILE, "r", encoding="utf-8") as fin, open(
    OUTPUT_FILE, "w", encoding="utf-8"
) as fout, tqdm(total=25_000_000) as pbar:
    for linedct in p.imap_unordered(check_tweet, fin, chunksize=32):
        kept_tweets += 1
        pbar.update(1)

        if not linedct["pass"]:
            continue

        # Parse matched_keywords reliably (could be list or string)
        # This tweet has at least one keyword from each set
        # -> write it to the new JSONL
        fout.write(json.dumps(linedct["line"], ensure_ascii=False))
        fout.write("\n")

        kept_tweets += 1


print(f"Total tweets processed: {total_tweets}")
print(f"Tweets with at least one Kurdish kw + one Islam kw: {kept_tweets}")
print(f"Filtered data saved to {OUTPUT_FILE}")
# print("All keywords: ", allkwords)
