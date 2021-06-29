import meilisearch

from src.defs.postgres import PROJECT

SEARCH_URL = 'http://174.138.109.133/'
SEARCH_PSWD = "kian_is_on_fleek"
c = meilisearch.Client(SEARCH_URL, SEARCH_PSWD)
index = c.get_index(f"{PROJECT}_products")
ac_index = c.get_index(f"{PROJECT}_autocomplete")
trending_index = c.get_index(f"{PROJECT}_trending_searches")
label_index = c.get_index(f"{PROJECT}_labels")
