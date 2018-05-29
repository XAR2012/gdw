def compile_sql(query, engine):
    return str(query.compile(
        dialect=engine.dialect,
        compile_kwargs={"literal_binds": True}))
