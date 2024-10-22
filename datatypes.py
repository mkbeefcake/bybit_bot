import json
from dataclasses import dataclass
from typing import List, Tuple

@dataclass
class OrderBookResponse:
    topic: str
    type: str
    ts: int
    data: 'OrderBookData'
    cts: int

@dataclass
class OrderBookData:
    s: str  
    b: List[Tuple[str, str]]
    a: List[Tuple[str, str]]
    u: int  # Update ID
    seq: int  # Sequence number

def serialize_order_book_response(message: any) -> OrderBookResponse:
    """Deserialize a JSON message into an OrderBookSnapshot instance."""
    try:
        order_book_json = message

        # Create OrderBookData instance
        order_book_data = OrderBookData(
            s=order_book_json['data']['s'],
            b=[[bid[0], bid[1]] for bid in order_book_json['data']['b']],
            a=[[ask[0], ask[1]] for ask in order_book_json['data']['a']],
            u=order_book_json['data']['u'],
            seq=order_book_json['data']['seq']
        )

        # Create OrderBookSnapshot instance
        order_book_snapshot = OrderBookResponse(
            topic=order_book_json['topic'],
            type=order_book_json['type'],
            ts=order_book_json['ts'],
            data=order_book_data,
            cts=order_book_json['cts']
        )

        return order_book_snapshot
    
    except json.JSONDecodeError as e:
        print(f"JSON Decode Error: {e}")
        return None
    except KeyError as e:
        print(f"Missing key in JSON data: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None



