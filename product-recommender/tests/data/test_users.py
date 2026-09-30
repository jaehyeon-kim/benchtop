from faker import Faker

from recommender.data.locations import load_locations
from recommender.data.users import new_user


def test_locations_are_the_city_postal_areas_weighted_by_population():
    """Verify that every area is in Melbourne and the weights add up to 1."""
    locations = load_locations()
    assert {a["city"] for a in locations.areas} == {"Melbourne"}
    assert abs(sum(locations.weights) - 1) < 1e-9


def test_the_same_seed_draws_the_same_user():
    """Verify that a user drawn twice with one seed is the same, and within the age range."""
    locations = load_locations()
    drawn = []
    for _ in range(2):
        Faker.seed(7)
        drawn.append(new_user(1, locations, Faker()))
    assert drawn[0] == drawn[1]
    assert 16 <= drawn[0].age <= 70 and drawn[0].city == "Melbourne"
