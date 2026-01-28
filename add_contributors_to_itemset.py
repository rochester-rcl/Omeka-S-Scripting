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

    def __init__(self, api_url: str, key_identity: str, key_credential: str, verbose: bool = False):
        """
        Initialize the Omeka S API client.

        Args:
            api_url: Base URL of the Omeka S API (e.g., 'https://example.com/api')
            key_identity: Omeka S key identity
            key_credential: Omeka S key credential
            verbose: Enable verbose debug output (default: False)
        """
        self.api_url = api_url.rstrip('/')
        self.key_identity = key_identity
        self.key_credential = key_credential
        self.verbose = verbose
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

    def get_items_generator(self, itemset_id: Optional[int] = None, per_page: int = 100):
        """
        Generator that yields pages of items one at a time for memory-efficient processing.

        Args:
            itemset_id: Optional item set ID to filter by. If None, gets all items.
            per_page: Number of items to retrieve per page

        Yields:
            Tuple of (page_number, items_list) for each page
        """
        page = 1

        while True:
            url = f"{self.api_url}/items"
            params = {'page': page, 'per_page': per_page}

            if itemset_id is not None:
                params['item_set_id'] = itemset_id

            try:
                source_desc = f"item set {itemset_id}" if itemset_id else "all items"
                if self.verbose:
                    print(f"DEBUG: Retrieving page {page} from {source_desc}")

                response = self.session.get(url, params=params)
                response.raise_for_status()
                page_items = response.json()

                if not page_items:
                    if self.verbose:
                        print(f"DEBUG: No more items on page {page}")
                    break

                print(f"Retrieved page {page} ({len(page_items)} items)...")
                yield page, page_items
                page += 1

            except requests.exceptions.RequestException as e:
                print(f"Error retrieving items on page {page}: {e}", file=sys.stderr)
                break

    def process_contributors_streaming(
            self,
            source_itemset_id: Optional[int],
            property_to_target_map: Dict[str, int],
            per_page: int = 100
    ) -> Dict[str, Dict[str, int]]:
        """
        Stream-process contributors: fetch page, extract, add to target sets, discard, repeat.
        This is the most memory-efficient approach - never holds more than one page in memory.

        Args:
            source_itemset_id: Optional item set ID to filter by. If None, processes all items.
            property_to_target_map: Dict mapping property names to target item set IDs
                                   e.g., {'rcl:artist': 123, 'rcl:author': 456}
            per_page: Number of items per page

        Returns:
            Dictionary mapping property names to stats dicts
        """
        property_names = list(property_to_target_map.keys())

        # Track what we've already processed to avoid duplicates
        processed_contributors = {prop: set() for prop in property_names}

        # Statistics
        stats = {prop: {'found': 0, 'added': 0, 'skipped': 0, 'errors': 0}
                 for prop in property_names}

        total_items = 0

        print(f"\n{'='*60}")
        print(f"STREAMING PROCESSING (True page-by-page)")
        print(f"{'='*60}")
        print(f"Properties: {', '.join(property_names)}")
        print(f"Processing pages of {per_page} items each")
        print(f"Adding contributors immediately after extraction from each page\n")

        # Process one page at a time
        for page_num, items in self.get_items_generator(source_itemset_id, per_page):
            total_items += len(items)
            print(f"\n--- Processing Page {page_num} ({len(items)} items) ---")

            # Extract contributors from this page
            page_contributors = {prop: set() for prop in property_names}

            for item in items:
                for property_name in property_names:
                    if property_name in item:
                        contributors = item[property_name]

                        if isinstance(contributors, list):
                            for contributor in contributors:
                                if isinstance(contributor, dict):
                                    contributor_type = contributor.get('type')
                                    value_resource_id = contributor.get('value_resource_id')

                                    if contributor_type == 'resource:item' and value_resource_id:
                                        # Only process if we haven't seen this contributor before
                                        if value_resource_id not in processed_contributors[property_name]:
                                            page_contributors[property_name].add(value_resource_id)
                                            processed_contributors[property_name].add(value_resource_id)
                                            stats[property_name]['found'] += 1

            # Now add these contributors to their target item sets
            # (items variable is now out of scope and can be garbage collected)
            for property_name, contributor_ids in page_contributors.items():
                if contributor_ids:
                    target_itemset_id = property_to_target_map[property_name]
                    print(f"\n  {property_name}: Adding {len(contributor_ids)} new contributors to item set {target_itemset_id}")

                    # Add this batch
                    batch_stats = self.add_items_to_itemset(contributor_ids, target_itemset_id)
                    stats[property_name]['added'] += batch_stats['added']
                    stats[property_name]['skipped'] += batch_stats['skipped']
                    stats[property_name]['errors'] += batch_stats['errors']

            print(f"  Page {page_num} complete - memory freed for next page")

        print(f"\n{'='*60}")
        print(f"STREAMING PROCESSING COMPLETE")
        print(f"{'='*60}")
        print(f"Total source items processed: {total_items}")
        print(f"\nFinal Statistics:")
        for property_name in property_names:
            print(f"\n{property_name}:")
            print(f"  Unique contributors found: {stats[property_name]['found']}")
            print(f"  Added to target set: {stats[property_name]['added']}")
            print(f"  Already in set (skipped): {stats[property_name]['skipped']}")
            print(f"  Errors: {stats[property_name]['errors']}")

        return stats

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

        if self.verbose:
            print(f"\nDEBUG: Starting to add {len(item_ids)} items to item set {itemset_id}")

        for idx, item_id in enumerate(item_ids, 1):
            # Show progress every 10 items (even in non-verbose mode)
            if not self.verbose and idx % 10 == 0:
                print(f"Progress: {idx}/{len(item_ids)} items processed...")

            try:
                if self.verbose:
                    print(f"\n--- Processing item {idx}/{len(item_ids)}: ID {item_id} ---")

                # Get current item data
                item_url = f"{self.api_url}/items/{item_id}"
                if self.verbose:
                    print(f"DEBUG: GET request to: {item_url}")

                response = self.session.get(item_url)

                if self.verbose:
                    print(f"DEBUG: GET response status: {response.status_code}")

                response.raise_for_status()
                item_data = response.json()

                item_title = item_data.get('o:title', 'Untitled')
                if self.verbose:
                    print(f"DEBUG: Item title: '{item_title}'")

                # Check if item is already in the item set
                current_itemsets = item_data.get('o:item_set', [])
                itemset_ids = [s['o:id'] for s in current_itemsets]

                if self.verbose:
                    print(f"DEBUG: Current item sets for this item: {itemset_ids}")

                if itemset_id in itemset_ids:
                    if self.verbose:
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

                if self.verbose:
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

                if self.verbose:
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

                if self.verbose:
                    print(f"DEBUG: PUT response status: {response.status_code}")

                # Print response details if there's an error
                if response.status_code != 200:
                    print(f"ERROR: Response status: {response.status_code}", file=sys.stderr)
                    if self.verbose:
                        print(f"ERROR: Response headers: {dict(response.headers)}", file=sys.stderr)
                    print(f"ERROR: Response body: {response.text}", file=sys.stderr)

                response.raise_for_status()

                if self.verbose:
                    print(f"SUCCESS: Added item {item_id} ('{item_title}') to item set {itemset_id}")
                success_count += 1

            except requests.exceptions.RequestException as e:
                print(f"ERROR: Failed to add item {item_id} to item set: {e}", file=sys.stderr)
                if hasattr(e, 'response') and e.response is not None:
                    print(f"ERROR: Response status: {e.response.status_code}", file=sys.stderr)
                    if self.verbose:
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

    # TRUE STREAMING: Process page-by-page, adding contributors immediately
    property_to_target_map = {property_name: target_itemset_id}
    all_stats = processor.process_contributors_streaming(
        source_itemset_id,
        property_to_target_map
    )

    return all_stats[property_name]


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

    # Initialize with verbose=False for clean output (set to True for debugging)
    processor = OmekaArtistProcessor(
        api_url,
        key_identity,
        key_credential,
        verbose=False
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

        # Get target item sets for each contributor type
        property_to_target_map = {}
        for key in sorted(CONTRIBUTOR_TYPES.keys()):
            if key == '8':  # Skip "All" option
                continue
            contributor = CONTRIBUTOR_TYPES[key]
            target_id = get_target_itemset(contributor['name'])
            property_to_target_map[contributor['property']] = target_id

            # Verify target exists
            if not processor.verify_itemset_exists(target_id):
                print(f"ERROR: Target item set {target_id} does not exist", file=sys.stderr)
                sys.exit(1)

        # TRUE STREAMING: Process page-by-page, adding contributors immediately
        all_stats = processor.process_contributors_streaming(
            source_itemset_id,
            property_to_target_map
        )

        # Convert stats format for display
        display_stats = {}
        for key in sorted(CONTRIBUTOR_TYPES.keys()):
            if key == '8':
                continue
            contributor = CONTRIBUTOR_TYPES[key]
            property_name = contributor['property']
            display_stats[contributor['name']] = all_stats[property_name]

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
