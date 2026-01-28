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
from typing import Set, List, Dict, Any, Optional
import requests
from dotenv import load_dotenv


# Contributor type configuration
CONTRIBUTOR_TYPES = {
    '1': {'name': 'Artists', 'property': 'rcl:artist'},
    '2': {'name': 'Authors', 'property': 'rcl:author'},
    '3': {'name': 'Composers', 'property': 'rcl:composer'},
    '4': {'name': 'Editors', 'property': 'rcl:editor'},
    '5': {'name': 'Essay Authors', 'property': 'rcl:essayAuthor'},
    '6': {'name': 'Photographers', 'property': 'rcl:photographer'},
    '7': {'name': 'Translators', 'property': 'rcl:translator'},
    '8': {'name': 'All contributor types', 'property': 'all'}
}


class OmekaArtistProcessor:
    """Process Omeka S items to add contributors to item sets."""

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

    def extract_contributor_items(self, items: List[Dict[str, Any]], property_name: str) -> Set[int]:
        """
        Extract all unique item IDs that are linked in a contributor property field.

        Args:
            items: List of Omeka S items to process
            property_name: Property name to extract (e.g., 'rcl:artist', 'rcl:author')

        Returns:
            Set of contributor item IDs
        """
        contributor_ids = set()
        items_with_contributors = 0
        items_without_contributors = 0

        print(f"\nDEBUG: Processing {len(items)} items to extract {property_name} contributors...")

        for idx, item in enumerate(items):
            item_id = item.get('o:id', 'unknown')
            item_title = item.get('o:title', 'Untitled')

            if (idx + 1) % 100 == 0:
                print(f"DEBUG: Processed {idx + 1}/{len(items)} items so far...")

            # Check if item has the specified property
            if property_name in item:
                items_with_contributors += 1
                contributors = item[property_name]

                print(f"\nDEBUG: Item {item_id} ('{item_title}') has {property_name} field")
                print(f"DEBUG: {property_name} type: {type(contributors)}")
                print(f"DEBUG: {property_name} value: {contributors}")

                # Property can be a list of resources
                if isinstance(contributors, list):
                    print(f"DEBUG: Found {len(contributors)} contributor(s) in list")
                    for contributor_idx, contributor in enumerate(contributors):
                        print(f"DEBUG:   Contributor {contributor_idx}: type={type(contributor)}, value={contributor}")

                        # Each contributor should be a resource reference
                        if isinstance(contributor, dict):
                            contributor_type = contributor.get('type')
                            value_resource_id = contributor.get('value_resource_id')

                            print(f"DEBUG:     Contributor type field: {contributor_type}")
                            print(f"DEBUG:     value_resource_id: {value_resource_id}")

                            if contributor_type == 'resource:item' and value_resource_id:
                                contributor_ids.add(value_resource_id)
                                print(f"DEBUG:     ✓ Added contributor ID {value_resource_id} to set")
                            else:
                                print(f"DEBUG:     ✗ Skipped - type mismatch or missing ID")
                        else:
                            print(f"DEBUG:     ✗ Skipped - not a dict")
                else:
                    print(f"DEBUG: {property_name} is not a list, it's a {type(contributors)}")
            else:
                items_without_contributors += 1

        print(f"\nDEBUG: Summary for {property_name}:")
        print(f"DEBUG:   Items with {property_name}: {items_with_contributors}")
        print(f"DEBUG:   Items without {property_name}: {items_without_contributors}")
        print(f"DEBUG:   Unique contributor IDs found: {len(contributor_ids)}")
        print(f"DEBUG:   Contributor IDs: {sorted(contributor_ids)}")

        return contributor_ids

    def add_items_to_itemset(self, item_ids: Set[int], itemset_id: int) -> Dict[str, int]:
        """
        Add items to the specified item set.

        Args:
            item_ids: Set of item IDs to add to the item set
            itemset_id: ID of the item set to add items to

        Returns:
            Dictionary with statistics: {'added': int, 'skipped': int, 'errors': int}
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

        print(f"\n=== PROCESSING SUMMARY ===")
        print(f"Total items processed: {len(item_ids)}")
        print(f"Successfully added: {success_count - already_in_set}")
        print(f"Already in set (skipped): {already_in_set}")
        print(f"Errors: {error_count}")

        return {
            'added': success_count - already_in_set,
            'skipped': already_in_set,
            'errors': error_count
        }

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


def display_menu():
    """Display the contributor type selection menu."""
    print("\n" + "="*60)
    print("OMEKA S CONTRIBUTOR LINKER")
    print("="*60)
    print("\nSelect contributor type to process:")
    print()
    for key, value in sorted(CONTRIBUTOR_TYPES.items()):
        print(f"  {key}. {value['name']}")
    print()


def get_contributor_choice() -> str:
    """Get and validate the contributor type choice from user."""
    while True:
        choice = input("Enter your choice (1-8): ").strip()
        if choice in CONTRIBUTOR_TYPES:
            return choice
        print("Invalid choice. Please enter a number between 1 and 8.")


def get_source_choice() -> Optional[int]:
    """Get source item set ID or None for full instance."""
    print("\n" + "-"*60)
    print("SOURCE SELECTION")
    print("-"*60)
    print("\nWhere should we look for items?")
    print("  1. Specific item set")
    print("  2. Entire instance (all items)")
    print()

    while True:
        choice = input("Enter your choice (1-2): ").strip()
        if choice == '1':
            while True:
                try:
                    itemset_id = int(input("Enter source item set ID: ").strip())
                    return itemset_id
                except ValueError:
                    print("Invalid input. Please enter a numeric item set ID.")
        elif choice == '2':
            confirm = input("Process entire instance? This may take a while. (y/n): ").strip().lower()
            if confirm == 'y':
                return None
        else:
            print("Invalid choice. Please enter 1 or 2.")


def get_target_itemset(contributor_name: str) -> int:
    """Get target item set ID for a contributor type."""
    print(f"\nEnter target item set ID for {contributor_name}: ", end='')
    while True:
        try:
            itemset_id = int(input().strip())
            return itemset_id
        except ValueError:
            print("Invalid input. Please enter a numeric item set ID: ", end='')


def process_contributor_type(
        processor: OmekaArtistProcessor,
        contributor_name: str,
        property_name: str,
        source_itemset_id: Optional[int],
        target_itemset_id: int
) -> Dict[str, int]:
    """
    Process a single contributor type.

    Returns:
        Dictionary with processing statistics
    """
    print("\n" + "="*60)
    print(f"PROCESSING: {contributor_name}")
    print("="*60)
    print(f"Property: {property_name}")
    print(f"Source: {'Entire instance' if source_itemset_id is None else f'Item set {source_itemset_id}'}")
    print(f"Target item set: {target_itemset_id}")

    # Verify target item set exists
    print(f"\nVerifying target item set {target_itemset_id} exists...")
    if not processor.verify_itemset_exists(target_itemset_id):
        print(f"ERROR: Target item set {target_itemset_id} does not exist", file=sys.stderr)
        return {'found': 0, 'added': 0, 'errors': 0, 'skipped': 0}
    print(f"✓ Target item set {target_itemset_id} exists")

    # Get items from source
    if source_itemset_id is None:
        print(f"\nRetrieving all items from Omeka S instance...")
        items = processor.get_all_items()
    else:
        print(f"\nRetrieving items from source item set {source_itemset_id}...")
        items = processor.get_items_from_itemset(source_itemset_id)

    print(f"Retrieved {len(items)} items")

    if not items:
        print("No items found in source. Nothing to do.")
        return {'found': 0, 'added': 0, 'errors': 0, 'skipped': 0}

    # Extract contributor IDs
    print(f"\nExtracting contributors from {property_name} fields...")
    contributor_ids = processor.extract_contributor_items(items, property_name)

    if not contributor_ids:
        print(f"\nNo {contributor_name.lower()} found. Nothing to do.")
        return {'found': 0, 'added': 0, 'errors': 0, 'skipped': 0}

    # Add contributors to target item set
    print(f"\nAdding {len(contributor_ids)} {contributor_name.lower()} to target item set {target_itemset_id}...")
    stats = processor.add_items_to_itemset(contributor_ids, target_itemset_id)

    return {
        'found': len(contributor_ids),
        'added': stats['added'],
        'errors': stats['errors'],
        'skipped': stats['skipped']
    }


def main():
    """Main entry point for the script."""
    # Load environment variables from .env file
    load_dotenv()

    # Get credentials from environment
    api_url = os.getenv('OMEKA_API_URL')
    key_identity = os.getenv('OMEKA_KEY_IDENTITY')
    key_credential = os.getenv('OMEKA_KEY_CREDENTIAL')

    # Validate that all required environment variables are set
    if not all([api_url, key_identity, key_credential]):
        print("\nERROR: Missing required environment variables", file=sys.stderr)
        print("Please ensure .env file contains:", file=sys.stderr)
        print("  OMEKA_API_URL", file=sys.stderr)
        print("  OMEKA_KEY_IDENTITY", file=sys.stderr)
        print("  OMEKA_KEY_CREDENTIAL", file=sys.stderr)
        sys.exit(1)

    # Initialize processor
    print("\nInitializing Omeka API processor...")
    print(f"API URL: {api_url}")
    print(f"Key Identity: {key_identity[:10]}...")

    processor = OmekaArtistProcessor(
        api_url,
        key_identity,
        key_credential
    )

    # Display menu and get choice
    display_menu()
    choice = get_contributor_choice()

    # Get source (item set or full instance)
    source_itemset_id = get_source_choice()

    if source_itemset_id is not None:
        # Verify source item set exists
        print(f"\nVerifying source item set {source_itemset_id} exists...")
        if not processor.verify_itemset_exists(source_itemset_id):
            print(f"ERROR: Source item set {source_itemset_id} does not exist", file=sys.stderr)
            sys.exit(1)
        print(f"✓ Source item set {source_itemset_id} exists")

    # Process based on choice
    if choice == '8':  # All contributor types
        print("\n" + "="*60)
        print("PROCESSING ALL CONTRIBUTOR TYPES")
        print("="*60)
        print("\nYou will be prompted for a target item set for each type.")

        targets = {}
        # Get target item sets for each contributor type
        for key in sorted(CONTRIBUTOR_TYPES.keys()):
            if key == '8':  # Skip "All" option
                continue
            contributor = CONTRIBUTOR_TYPES[key]
            targets[key] = get_target_itemset(contributor['name'])

        # Process each contributor type
        all_stats = {}
        for key in sorted(targets.keys()):
            contributor = CONTRIBUTOR_TYPES[key]
            stats = process_contributor_type(
                processor,
                contributor['name'],
                contributor['property'],
                source_itemset_id,
                targets[key]
            )
            all_stats[contributor['name']] = stats

        # Print overall summary
        print("\n" + "="*60)
        print("OVERALL SUMMARY")
        print("="*60)
        for name, stats in all_stats.items():
            print(f"\n{name}:")
            print(f"  Found: {stats['found']}")
            print(f"  Added: {stats['added']}")
            print(f"  Skipped: {stats['skipped']}")
            print(f"  Errors: {stats['errors']}")

    else:  # Single contributor type
        contributor = CONTRIBUTOR_TYPES[choice]
        target_itemset_id = get_target_itemset(contributor['name'])

        stats = process_contributor_type(
            processor,
            contributor['name'],
            contributor['property'],
            source_itemset_id,
            target_itemset_id
        )

        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        print(f"Found: {stats['found']}")
        print(f"Added: {stats['added']}")
        print(f"Skipped: {stats['skipped']}")
        print(f"Errors: {stats['errors']}")

    print("\n" + "="*60)
    print("DONE!")
    print("="*60)


if __name__ == '__main__':
    main()