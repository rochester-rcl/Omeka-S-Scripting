#!/usr/bin/env python3
"""
Add all items linked in rcl:artist fields to a specified item set.

This script queries an Omeka S instance for all items in a source item set
that have the rcl:artist property, extracts the linked items from that property,
and adds those linked items to a user-specified target item set.
"""

import argparse
import os
import sys
from typing import Set, List, Dict, Any
import requests
from dotenv import load_dotenv


class OmekaArtistProcessor:
    """Process Omeka S items to add artists to an item set."""

    def __init__(self, api_url: str, key_identity: str, key_credential: str):
        """
        Initialize the Omeka S API client.

        Args:
            api_url: Base URL of the Omeka S API (e.g., 'https://example.com/api')
            key_identity: Omeka S key identity
            key_credential: Omeka S key credential
        """
        self.api_url = api_url.rstrip('/')
        self.key_identity = key_identity
        self.key_credential = key_credential
        self.session = requests.Session()

        # Set up authentication as query parameters (Omeka S style)
        self.session.params.update({
            'key_identity': key_identity,
            'key_credential': key_credential
        })

        # Set content type for API requests
        self.session.headers.update({
            'Content-Type': 'application/json'
        })

    def get_all_items(self, per_page: int = 100) -> List[Dict[str, Any]]:
        """
        Retrieve all items from the Omeka S instance.

        Args:
            per_page: Number of items to retrieve per page

        Returns:
            List of all items
        """
        items = []
        page = 1

        while True:
            url = f"{self.api_url}/items"
            params = {'page': page, 'per_page': per_page}

            try:
                print(f"DEBUG: Requesting {url} with params {params}")
                response = self.session.get(url, params=params)
                response.raise_for_status()
                page_items = response.json()

                if not page_items:
                    break

                items.extend(page_items)
                print(f"Retrieved page {page} ({len(page_items)} items)...")
                page += 1

            except requests.exceptions.RequestException as e:
                print(f"Error retrieving items on page {page}: {e}", file=sys.stderr)
                break

        return items

    def get_items_from_itemset(self, itemset_id: int, per_page: int = 100) -> List[Dict[str, Any]]:
        """
        Retrieve all items from a specific item set.

        Args:
            itemset_id: ID of the item set to retrieve items from
            per_page: Number of items to retrieve per page

        Returns:
            List of items in the item set
        """
        items = []
        page = 1

        while True:
            url = f"{self.api_url}/items"
            params = {
                'page': page,
                'per_page': per_page,
                'item_set_id': itemset_id
            }

            try:
                print(f"DEBUG: Requesting items from item set {itemset_id}, page {page}")
                print(f"DEBUG: URL: {url}")
                print(f"DEBUG: Params: {params}")
                response = self.session.get(url, params=params)
                print(f"DEBUG: Response status: {response.status_code}")
                response.raise_for_status()
                page_items = response.json()

                if not page_items:
                    print(f"DEBUG: No more items on page {page}")
                    break

                items.extend(page_items)
                print(f"Retrieved page {page} ({len(page_items)} items from item set {itemset_id})...")
                page += 1

            except requests.exceptions.RequestException as e:
                print(f"Error retrieving items on page {page}: {e}", file=sys.stderr)
                print(f"DEBUG: Response text: {response.text if 'response' in locals() else 'N/A'}", file=sys.stderr)
                break

        print(f"DEBUG: Total items retrieved from item set {itemset_id}: {len(items)}")
        return items

    def extract_artist_items(self, items: List[Dict[str, Any]]) -> Set[int]:
        """
        Extract all unique item IDs that are linked in rcl:artist fields.

        Args:
            items: List of Omeka S items to process

        Returns:
            Set of artist item IDs
        """
        artist_ids = set()
        items_with_artists = 0
        items_without_artists = 0

        print(f"\nDEBUG: Processing {len(items)} items to extract artists...")

        for idx, item in enumerate(items):
            item_id = item.get('o:id', 'unknown')
            item_title = item.get('o:title', 'Untitled')

            if (idx + 1) % 100 == 0:
                print(f"DEBUG: Processed {idx + 1}/{len(items)} items so far...")

            # Check if item has rcl:artist property
            if 'rcl:artist' in item:
                items_with_artists += 1
                artists = item['rcl:artist']

                print(f"\nDEBUG: Item {item_id} ('{item_title}') has rcl:artist field")
                print(f"DEBUG: rcl:artist type: {type(artists)}")
                print(f"DEBUG: rcl:artist value: {artists}")

                # rcl:artist can be a list of resources
                if isinstance(artists, list):
                    print(f"DEBUG: Found {len(artists)} artist(s) in list")
                    for artist_idx, artist in enumerate(artists):
                        print(f"DEBUG:   Artist {artist_idx}: type={type(artist)}, value={artist}")

                        # Each artist should be a resource reference
                        if isinstance(artist, dict):
                            artist_type = artist.get('type')
                            value_resource_id = artist.get('value_resource_id')

                            print(f"DEBUG:     Artist type field: {artist_type}")
                            print(f"DEBUG:     value_resource_id: {value_resource_id}")

                            if artist_type == 'resource:item' and value_resource_id:
                                artist_ids.add(value_resource_id)
                                print(f"DEBUG:     ✓ Added artist ID {value_resource_id} to set")
                            else:
                                print(f"DEBUG:     ✗ Skipped - type mismatch or missing ID")
                        else:
                            print(f"DEBUG:     ✗ Skipped - not a dict")
                else:
                    print(f"DEBUG: rcl:artist is not a list, it's a {type(artists)}")
            else:
                items_without_artists += 1

        print(f"\nDEBUG: Summary:")
        print(f"DEBUG:   Items with rcl:artist: {items_with_artists}")
        print(f"DEBUG:   Items without rcl:artist: {items_without_artists}")
        print(f"DEBUG:   Unique artist IDs found: {len(artist_ids)}")
        print(f"DEBUG:   Artist IDs: {sorted(artist_ids)}")

        return artist_ids

    def add_items_to_itemset(self, item_ids: Set[int], itemset_id: int) -> None:
        """
        Add items to the specified item set.

        Args:
            item_ids: Set of item IDs to add to the item set
            itemset_id: ID of the item set to add items to
        """
        success_count = 0
        error_count = 0
        already_in_set = 0

        print(f"\nDEBUG: Starting to add {len(item_ids)} items to item set {itemset_id}")

        for idx, item_id in enumerate(item_ids, 1):
            try:
                print(f"\n--- Processing item {idx}/{len(item_ids)}: ID {item_id} ---")

                # Get current item data
                item_url = f"{self.api_url}/items/{item_id}"
                print(f"DEBUG: GET request to: {item_url}")
                response = self.session.get(item_url)
                print(f"DEBUG: GET response status: {response.status_code}")
                response.raise_for_status()
                item_data = response.json()

                item_title = item_data.get('o:title', 'Untitled')
                print(f"DEBUG: Item title: '{item_title}'")

                # Check if item is already in the item set
                current_itemsets = item_data.get('o:item_set', [])
                itemset_ids = [s['o:id'] for s in current_itemsets]

                print(f"DEBUG: Current item sets for this item: {itemset_ids}")

                if itemset_id in itemset_ids:
                    print(f"DEBUG: Item {item_id} is already in target item set {itemset_id} - skipping")
                    already_in_set += 1
                    success_count += 1
                    continue

                # Add the new item set to the list with both @id and o:id
                # (matching the format of existing item sets)
                new_item_set = {
                    '@id': f"{self.api_url}/item_sets/{itemset_id}",
                    'o:id': itemset_id
                }
                current_itemsets.append(new_item_set)
                new_itemset_ids = [s['o:id'] for s in current_itemsets]

                print(f"DEBUG: Adding item set {itemset_id} to item {item_id}")
                print(f"DEBUG: New item set list will be: {new_itemset_ids}")
                print(f"DEBUG: New item set entry: {new_item_set}")

                # Update the item data with new item sets
                item_data['o:item_set'] = current_itemsets

                # Remove fields that shouldn't be sent in PUT request
                # These are read-only, computed, or JSON-LD context fields
                fields_to_remove = ['@context', '@id', '@type', 'o:id', 'o:owner',
                                    'o:created', 'o:modified', 'thumbnail_display_urls',
                                    'o:primary_media', 'o:media', 'o:site', '@reverse']

                update_data = {k: v for k, v in item_data.items() if k not in fields_to_remove}

                print(f"DEBUG: PUT data keys: {list(update_data.keys())}")
                print(f"DEBUG: o:item_set value: {update_data.get('o:item_set')}")
                print(f"DEBUG: PUT request to: {item_url}")
                print(f"DEBUG: Auth being used: key_identity={self.key_identity}")

                # Use PUT instead of PATCH (Omeka S uses PUT)
                response = self.session.put(
                    item_url,
                    json=update_data,
                    headers={'Content-Type': 'application/json'}
                )

                print(f"DEBUG: PUT response status: {response.status_code}")

                # Print response details if there's an error
                if response.status_code != 200:
                    print(f"ERROR: Response status: {response.status_code}", file=sys.stderr)
                    print(f"ERROR: Response headers: {dict(response.headers)}", file=sys.stderr)
                    print(f"ERROR: Response body: {response.text}", file=sys.stderr)

                response.raise_for_status()

                print(f"SUCCESS: Added item {item_id} ('{item_title}') to item set {itemset_id}")
                success_count += 1

            except requests.exceptions.RequestException as e:
                print(f"ERROR: Failed to add item {item_id} to item set: {e}", file=sys.stderr)
                if hasattr(e, 'response') and e.response is not None:
                    print(f"ERROR: Response status: {e.response.status_code}", file=sys.stderr)
                    print(f"ERROR: Response body: {e.response.text}", file=sys.stderr)
                error_count += 1

        print(f"\n=== FINAL SUMMARY ===")
        print(f"Total items processed: {len(item_ids)}")
        print(f"Successfully added: {success_count - already_in_set}")
        print(f"Already in set (skipped): {already_in_set}")
        print(f"Errors: {error_count}")

    def verify_itemset_exists(self, itemset_id: int) -> bool:
        """
        Verify that an item set exists.

        Args:
            itemset_id: ID of the item set to verify

        Returns:
            True if item set exists, False otherwise
        """
        try:
            url = f"{self.api_url}/item_sets/{itemset_id}"
            response = self.session.get(url)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException:
            return False


def main():
    """Main entry point for the script."""
    # Load environment variables from .env file
    load_dotenv()

    parser = argparse.ArgumentParser(
        description='Add all items linked in rcl:artist fields from a source item set to a target item set'
    )
    parser.add_argument(
        'source_itemset_id',
        type=int,
        help='ID of the source item set to get items from'
    )
    parser.add_argument(
        'target_itemset_id',
        type=int,
        help='ID of the target item set to add artist items to'
    )

    args = parser.parse_args()

    print(f"=== Configuration ===")
    print(f"Source item set ID: {args.source_itemset_id}")
    print(f"Target item set ID: {args.target_itemset_id}")

    # Get credentials from environment
    api_url = os.getenv('OMEKA_API_URL')
    key_identity = os.getenv('OMEKA_KEY_IDENTITY')
    key_credential = os.getenv('OMEKA_KEY_CREDENTIAL')

    print(f"API URL: {api_url}")
    print(f"Key Identity: {key_identity[:10]}..." if key_identity else "Not set")

    # Validate that all required environment variables are set
    if not all([api_url, key_identity, key_credential]):
        print("\nError: Missing required environment variables", file=sys.stderr)
        print("Please ensure .env file contains:", file=sys.stderr)
        print("  OMEKA_API_URL", file=sys.stderr)
        print("  OMEKA_KEY_IDENTITY", file=sys.stderr)
        print("  OMEKA_KEY_CREDENTIAL", file=sys.stderr)
        sys.exit(1)

    # Initialize processor
    print("\nInitializing Omeka API processor...")
    processor = OmekaArtistProcessor(
        api_url,
        key_identity,
        key_credential
    )

    # Verify both item sets exist
    print(f"\nVerifying source item set {args.source_itemset_id} exists...")
    if not processor.verify_itemset_exists(args.source_itemset_id):
        print(f"Error: Source item set {args.source_itemset_id} does not exist", file=sys.stderr)
        sys.exit(1)
    print(f"✓ Source item set {args.source_itemset_id} exists")

    print(f"\nVerifying target item set {args.target_itemset_id} exists...")
    if not processor.verify_itemset_exists(args.target_itemset_id):
        print(f"Error: Target item set {args.target_itemset_id} does not exist", file=sys.stderr)
        sys.exit(1)
    print(f"✓ Target item set {args.target_itemset_id} exists")

    # Get items from source item set
    print(f"\n=== Step 1: Retrieving items from source item set {args.source_itemset_id} ===")
    items = processor.get_items_from_itemset(args.source_itemset_id)
    print(f"Retrieved {len(items)} items from source item set")

    if not items:
        print("No items found in source item set. Nothing to do.")
        return

    # Extract artist item IDs
    print(f"\n=== Step 2: Extracting artists from rcl:artist fields ===")
    artist_ids = processor.extract_artist_items(items)

    if not artist_ids:
        print("\nNo artist items found. Nothing to do.")
        return

    # Add artists to target item set
    print(f"\n=== Step 3: Adding {len(artist_ids)} artist items to target item set {args.target_itemset_id} ===")
    processor.add_items_to_itemset(artist_ids, args.target_itemset_id)

    print("\n=== DONE! ===")


if __name__ == '__main__':
    main()