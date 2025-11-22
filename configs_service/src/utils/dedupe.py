def dedupe_by_raw(items):
    seen = set()
    out = []
    for it in items:
        r = it.get('raw')
        if r in seen: continue
        seen.add(r)
        out.append(it)
    return out
