import typing as t
import re 

from functional import seq, pseq
from fuzzywuzzy import process
from fuzzywuzzy import fuzz


def _run_replace(queryString, res) -> str:
    return re.sub(f"\\b{res}\\b", "", queryString) \
            .replace("  ", " ") \
            .lstrip()

def rm_token(queryString: str, token: str, scorer) -> str:
    x = queryString.split(" ")
    substrs = []
    substrs.extend(x)
    if len(substrs) > 1:
        for i in range(len(x) - 1):
            substrs.append(" ".join(x[i:i+2]))

    ## Get scores
    ## Tiebreaker -> longest string
    res = process.extract(token.lower(), substrs, scorer=scorer, limit=5)
    res = sorted(res, key=lambda x: (x[1], len(x[0])), reverse=True)[0][0]
    return _run_replace(queryString, res)

