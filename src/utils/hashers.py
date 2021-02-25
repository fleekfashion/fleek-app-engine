import pyhash

def apple_id_to_user_id_hash(apple_id):
    hasher = pyhash.farm_fingerprint_64()
    return hasher(apple_id) // 10 if apple_id is not None else None
