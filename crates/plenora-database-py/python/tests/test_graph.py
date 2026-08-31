from decimal import Decimal

from plenora_database.graph import Edge, Path, Vertex, _decode_rows


def test_graph_decoder_preserves_age_types() -> None:
    encoded = [{
        "path": {
            "type": "path",
            "value": [
                {
                    "type": "vertex",
                    "value": {
                        "id": 1,
                        "label": "Person",
                        "properties": {
                            "name": {"type": "string", "value": "Alice"}
                        },
                    },
                },
                {
                    "type": "edge",
                    "value": {
                        "id": 2,
                        "label": "KNOWS",
                        "start_id": 1,
                        "end_id": 3,
                        "properties": {},
                    },
                },
            ],
        },
        "amount": {"type": "numeric", "value": "1.25"},
    }]
    decoded = _decode_rows(encoded)
    assert decoded[0]["amount"] == Decimal("1.25")
    path = decoded[0]["path"]
    assert isinstance(path, Path)
    assert isinstance(path.elements[0], Vertex)
    assert isinstance(path.elements[1], Edge)
