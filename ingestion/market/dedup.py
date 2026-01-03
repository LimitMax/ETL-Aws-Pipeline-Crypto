def deduplicate(records):
    seen = set()
    unique_records = []

    for r in records:
        key = (r["symbol"], r["datex"], r["hourx"])
        if key not in seen:
            seen.add(key)
            unique_records.append(r)

    return unique_records