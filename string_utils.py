"""
string_utils.py - String processing utility

A collection of utility functions for string processing.

Author: Fernando Karnagi <fkarnagi@gmail.com>
Created: 2025-06-21
Modified: 2025-06-21
Version: 1.0
"""

from typing import List, Dict, Any
import random
import string


def text_to_lines_no_empty(text):
    """
    Split text into an array of lines, removing empty lines.

    Args:
        text (str): The input text

    Returns:
        list: List of non-empty lines
    """
    return [line for line in text.split("\n") if line.strip()]


def array_to_lines(json_array):
    text_parts = []

    for item in json_array:
        if isinstance(item, dict):
            parts = [f"{v}" for k, v in item.items()]
            text_parts.append(", ".join(parts))
        else:
            text_parts.append(str(item))
        long_text = ". ".join(text_parts)

    return long_text


def extract_text_array(data: List[Dict[str, Any]], text_key: str = "text") -> List[str]:
    """
    Extract text values from an array of dictionaries.

    Args:
        data: List of dictionaries containing text data
        text_key: Key name that contains the text (default: "text")

    Returns:
        List of extracted text strings
    """
    return [item[text_key] for item in data]


def generate_random_str() -> str:

    # Alphanumeric (letters + digits)
    return "".join(random.choices(string.ascii_letters + string.digits, k=6))


def extract_field_to_string(data: list, field_name: str) -> str:
    """
    Simplified version - extract field values and join with comma-space.
    
    Args:
        data: List of dictionaries
        field_name: Name of the field to extract
    
    Returns:
        Comma-separated string of field values
    """
    return ", ".join(str(item.get(field_name, "")) for item in data if isinstance(item, dict) and item.get(field_name))


if __name__ == "__main__":

    array_object = [
        {
            "text": "Goods delivery issues have been clearly identified from Sellers which result in the Goods order not arriving;"
        },
        {
            "text": "The Buyer consents to the acceptance of partial Goods offered by the Seller;"
        },
        {
            "text": "Sellers are incapable of fulfilling orders due to out of stock, changes in the shipping fee, or other causes;"
        },
        {"text": "In the event of Free Promos/Discounted Shipping Fee:"},
        {
            "text": "if the Goods are out of stock, the Seller may reject the order and payment for the Goods in question shall be refunded to the Buyer."
        },
    ]

    lines = extract_field_to_string(array_object, field_name="text")
    print(lines)
