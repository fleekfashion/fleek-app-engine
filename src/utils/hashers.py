import pyhash
from src.defs import postgres as p

def apple_id_to_user_id_hash(apple_id):
    if p.DEV_MODE:
        return apple_id
    hasher = pyhash.farm_fingerprint_64()
    return hasher(apple_id) // 10 if apple_id is not None else None
