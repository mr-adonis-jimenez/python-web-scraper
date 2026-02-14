"""Quick smoke tests that can be run without pytest."""
from parser import parse_items


def test_parse_items():
    html = """
    <div class="item">
        <span class="title">Sample</span>
        <span class="price">$10</span>
    </div>
    """
    results = parse_items(html)
    assert results[0]["title"] == "Sample"
    assert results[0]["price"] == "$10"
    print("test_parse_items passed")


if __name__ == "__main__":
    test_parse_items()
