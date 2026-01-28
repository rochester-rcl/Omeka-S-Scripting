#!/usr/bin/env python3
"""
Omeka S REST API Sample Script
Fetches an item by ID and saves it back unchanged to demonstrate API usage.
"""
import dotenv
import requests
import json
import sys
from typing import Optional, Dict, Any

class OmekaSClient:
    def __init__(self, base_url: str, key_identity: str, key_credential: str):
        """
        Initialize the Omeka S API client.

        Args:
            base_url: Base URL of your Omeka S installation (e.g., 'https://yoursite.com')
            key_identity: API key identity from Omeka S
            key_credential: API key credential from Omeka S
        """
        self.base_url = base_url.rstrip('/')
        self.api_url = f"{self.base_url}/api"
        self.session = requests.Session()

        # Set up authentication headers
        self.session.params.update({
            'key_identity': key_identity,
            'key_credential': key_credential
        })

        # Set content type for API requests
        self.session.headers.update({
            'Content-Type': 'application/json'
        })

    def get_item(self, item_id: int) -> Optional[Dict[Any, Any]]:
        """
        Fetch an item from Omeka S by ID.

        Args:
            item_id: The ID of the item to fetch

        Returns:
            Dictionary containing item data, or None if not found
        """
        try:
            url = f"{self.api_url}/items/{item_id}"
            response = self.session.get(url)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                print(f"Item with ID {item_id} not found.")
                return None
            else:
                print(f"Error fetching item: {response.status_code} - {response.text}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"Network error while fetching item: {e}")
            return None

    def get_item_set_items(self, item_set_id: int) -> Optional[list]:
        """
        Fetch all items from an item set by ID.

        Args:
            item_set_id: The ID of the item set

        Returns:
            List of item dictionaries, or None if error
        """
        try:
            all_items = []
            page = 1
            per_page = 50  # API default, adjust if needed

            while True:
                url = f"{self.api_url}/items"
                params = {
                    'item_set_id': item_set_id,
                    'page': page,
                    'per_page': per_page
                }

                response = self.session.get(url, params=params)

                if response.status_code == 200:
                    items = response.json()
                    if not items:  # No more items
                        break
                    all_items.extend(items)
                    page += 1
                elif response.status_code == 404:
                    print(f"Item set with ID {item_set_id} not found.")
                    return None
                else:
                    print(f"Error fetching items from item set: {response.status_code} - {response.text}")
                    return None

            return all_items

        except requests.exceptions.RequestException as e:
            print(f"Network error while fetching item set items: {e}")
            return None

    def update_item(self, item_id: int, item_data: Dict[Any, Any]) -> bool:
        """
        Update an item in Omeka S.

        Args:
            item_id: The ID of the item to update
            item_data: The item data to save

        Returns:
            True if successful, False otherwise
        """
        try:
            url = f"{self.api_url}/items/{item_id}"

            # Remove read-only fields that shouldn't be sent in updates
            update_data = item_data.copy()
            read_only_fields = ['o:id', 'o:created', 'o:modified', 'o:owner', 'o:resource_class', 'o:resource_template']
            for field in read_only_fields:
                update_data.pop(field, None)

            response = self.session.put(url, data=json.dumps(update_data))

            if response.status_code == 200:
                print(f"  ✓ Successfully updated item {item_id}")
                return True
            else:
                print(f"  ✗ Error updating item {item_id}: {response.status_code} - {response.text}")
                return False

        except requests.exceptions.RequestException as e:
            print(f"  ✗ Network error while updating item {item_id}: {e}")
            return False

    def process_item_set(self, item_set_id: int, silent=False) -> Dict[str, int]:
        """
        Process all items in an item set by saving them back unchanged.

        Args:
            item_set_id: The ID of the item set to process
            silent: If True, don't prompt for confirmation before processing items'
        Returns:
            Dictionary with 'success' and 'failed' counts
        """
        print(f"Fetching all items from item set {item_set_id}...")

        items = self.get_item_set_items(item_set_id)
        if items is None:
            return {'success': 0, 'failed': 0}

        if not items:
            print("No items found in this item set.")
            return {'success': 0, 'failed': 0}

        print(f"Found {len(items)} items in the item set.")
        confirm = ""
        # Confirm before processing
        if not silent:
            confirm = input(f"\nProcess all {len(items)} items (save each back unchanged)? [y/N]: ").strip().lower()
        else:
            confirm = 'y'
        if confirm not in ['y', 'yes']:
            print("Operation cancelled.")
            return {'success': 0, 'failed': 0}

        print(f"\nProcessing {len(items)} items...")

        success_count = 0
        failed_count = 0

        for i, item in enumerate(items, 1):
            item_id = item.get('o:id')
            item_title = item.get('o:title', 'Untitled')

            print(f"[{i}/{len(items)}] Processing item {item_id}: {item_title}")

            if self.update_item(item_id, item):
                success_count += 1
            else:
                failed_count += 1

        return {'success': success_count, 'failed': failed_count}

def process_single_item(client: OmekaSClient):
    """Process a single item by ID."""
    try:
        item_id = input("Enter the item ID to fetch: ").strip()
        item_id = int(item_id)
    except ValueError:
        print("Invalid item ID. Please enter a numeric value.")
        return

    print(f"Fetching item {item_id}...")

    # Fetch the item
    item = client.get_item(item_id)
    if item is None:
        return

    print(f"Successfully fetched item: {item.get('o:title', 'Untitled')}")
    print(f"Item ID: {item.get('o:id')}")
    print(f"Created: {item.get('o:created', {}).get('@value', 'Unknown')}")
    print(f"Modified: {item.get('o:modified', {}).get('@value', 'Unknown')}")

    # Display some basic item information
    if item.get('dcterms:title'):
        titles = item['dcterms:title']
        if titles:
            print(f"Title: {titles[0].get('@value', 'No title')}")

    if item.get('dcterms:description'):
        descriptions = item['dcterms:description']
        if descriptions:
            desc = descriptions[0].get('@value', '')[:100]  # First 100 chars
            print(f"Description: {desc}{'...' if len(desc) == 100 else ''}")

    # Confirm before saving
    confirm = input("\nSave this item back to Omeka S (no changes will be made)? [y/N]: ").strip().lower()
    if confirm not in ['y', 'yes']:
        print("Operation cancelled.")
        return

    print("Saving item back to Omeka S...")

    # Save the item back (unchanged)
    success = client.update_item(item_id, item)

    if success:
        print("✓ Item successfully saved back to Omeka S!")
    else:
        print("✗ Failed to save item.")

def process_item_set_items(client: OmekaSClient):
    """Process all items in an item set."""
    try:
        item_set_id = input("Enter the item set ID to process: ").strip()
        item_set_id = int(item_set_id)
    except ValueError:
        print("Invalid item set ID. Please enter a numeric value.")
        return

    results = client.process_item_set(item_set_id)

    print(f"\n=== Processing Complete ===")
    print(f"Successfully processed: {results['success']} items")
    print(f"Failed to process: {results['failed']} items")
    print(f"Total items: {results['success'] + results['failed']}")

def main():
    """Main function to demonstrate the API workflow."""

    # Configuration - Replace these with your actual Omeka S details

    instance=input("Please enter the name of your instance, in all caps (e.g. EXHIBITS_DEV, EXHIBITS_PROD)")

    OMEKA_BASE_URL=dotenv.get_key(".env", f"{instance}_OMEKA_BASE_URL")
    API_KEY_IDENTITY = dotenv.get_key(".env", f"{instance}_API_KEY_IDENTITY")
    API_KEY_CREDENTIAL = dotenv.get_key(".env", f"{instance}_API_KEY_CREDENTIAL")

    if not (OMEKA_BASE_URL and API_KEY_IDENTITY and API_KEY_CREDENTIAL):
        print("Instance not found. Goodbye")
        sys.exit(1)
    # Initialize the client
    client = OmekaSClient(OMEKA_BASE_URL, API_KEY_IDENTITY, API_KEY_CREDENTIAL)

    try:
        while True:
            print("\n=== Omeka S API Demo ===")
            print("1. Process a single item by ID")
            print("2. Process all items in an item set")
            print("3. Process all items in all item sets")
            print("4. Exit")

            choice = input("\nEnter your choice (1-3): ").strip()

            if choice == '1':
                process_single_item(client)
            elif choice == '2':
                process_item_set_items(client)
            elif choice == '3':
                # First, get all item sets
                try:
                    response = client.session.get(f"{client.api_url}/item_sets")
                    if response.status_code == 200:
                        item_sets = response.json()
                        print(f"Found {len(item_sets)} item sets. Processing all...")

                        for item_set in item_sets:
                            item_set_id = item_set.get('o:id')
                            item_set_title = item_set.get('o:title', 'Untitled')
                            print(f"\n--- Processing item set {item_set_id}: {item_set_title} ---")
                            client.process_item_set(item_set_id, silent=True)
                    else:
                        print(f"Error fetching item sets: {response.status_code} - {response.text}")
                except requests.exceptions.RequestException as e:
                    print(f"Network error while fetching item sets: {e}")
            elif choice == '4':
                print("Goodbye!")
                break
            else:
                print("Invalid choice. Please enter 1, 2, 3, or 4.")

    except KeyboardInterrupt:
        print("\nOperation cancelled. Goodbye!")
        sys.exit(0)

if __name__ == "__main__":
    main()