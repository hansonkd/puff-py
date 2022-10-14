def query_and_params(queryset):
    filtered_table_query = queryset.query
    raw_query, params = filtered_table_query.sql_with_params()
    ix = 1
    params = list(params)
    while "%s" in raw_query:
        raw_query = raw_query.replace("%s", f"${ix}", 1)
        ix += 1
    return raw_query, params
