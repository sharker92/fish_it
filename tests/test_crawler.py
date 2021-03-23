from src.crawler.crawler import add_url_to_db
# export PYTHONPATH="${PYTHONPATH}:~/OneDrive/03Programming/01_fish_it"


def test_add_url_to_db():
    url = "asdfasdf"
    assert(add_url_to_db(url) == "asdf")
