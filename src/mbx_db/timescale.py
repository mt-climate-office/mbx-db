from sqlalchemy import text


make_flow_hypertable = text("""
    SELECT create_hypertable('flow.data', by_range('date', INTERVAL '1 year'));
""")


TIMESCALE_SETUP = [make_flow_hypertable]
