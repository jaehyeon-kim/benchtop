from ecommerce.stores.kafka import own_topics


def test_only_this_projects_topics_are_chosen():
    """Verify that the clean-up picks Debezium's ecommerce. topics and the control topic, and nothing else."""
    topics = {
        "ecommerce.cdc.users",
        "ecommerce.cdc.orders",
        "ecommerce-control",
        "game-scores",
        "ecommerce-other",
        "connect-offsets",
    }
    assert own_topics(topics) == [
        "ecommerce-control",
        "ecommerce.cdc.orders",
        "ecommerce.cdc.users",
    ]
