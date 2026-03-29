from faersdb.cli import quarter_pipeline_steps


def test_quarter_pipeline_steps_order_and_coverage():
    steps = quarter_pipeline_steps()

    assert steps[:7] == [
        ("load", "DEMO"),
        ("load", "DRUG"),
        ("load", "REAC"),
        ("load", "OUTC"),
        ("load", "THER"),
        ("load", "INDI"),
        ("load", "RPSR"),
    ]

    assert steps[7:] == [
        ("normalize", "DEMO"),
        ("normalize", "DRUG"),
        ("normalize", "REAC"),
        ("normalize", "OUTC"),
        ("normalize", "THER"),
        ("normalize", "INDI"),
        ("normalize", "RPSR"),
    ]
