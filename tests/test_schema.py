from sqlalchemy import inspect

async def test_all_tables_exist(engine):
    async with engine.connect() as conn:
        names = await conn.run_sync(lambda c: inspect(c).get_table_names())
    expected = {"buses", "drivers", "shifts", "routes", "stops", "route_stops",
                "signals", "events", "clips", "rule_configs", "users"}
    assert expected.issubset(set(names))
