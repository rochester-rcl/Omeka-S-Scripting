#!/usr/bin/env python3
"""
Omeka S REST API Sample Script
Fetches an item by ID and saves it back unchanged to demonstrate API usage.
"""
import dotenv
import requests
import json
import sys
import logging
from datetime import datetime
from typing import Optional, Dict, Any

# Configure logging
def setup_logging():
    """Set up logging configuration with both file and console handlers."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'omeka_dereferencing_{timestamp}.log'

    # Create logger
    logger = logging.getLogger('omeka_deref')
    logger.setLevel(logging.INFO)

    # Create file handler
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger, log_filename

class OmekaSClient:
    def __init__(self, base_url: str, key_identity: str, key_credential: str, logger: logging.Logger):
        """
        Initialize the Omeka S API client.

        Args:
            base_url: Base URL of your Omeka S installation (e.g., 'https://yoursite.com')
            key_identity: API key identity from Omeka S
            key_credential: API key credential from Omeka S
            logger: Logger instance for logging operations
        """
        self.base_url = base_url.rstrip('/')
        self.api_url = f"{self.base_url}/api"
        self.session = requests.Session()
        self.logger = logger

        # Set up authentication headers
        self.session.params.update({
            'key_identity': key_identity,
            'key_credential': key_credential
        })

        # Set content type for API requests
        self.session.headers.update({
            'Content-Type': 'application/json'
        })

    def get_creator_name(self, item: dict) -> str:
        """
        Extract the creator name from an item.

        Args:
            item: The item data

        Returns:
            Creator name or 'Unknown'
        """
        if 'o:owner' in item and item['o:owner']:
            owner = item['o:owner']
            if 'o:name' in owner:
                return owner['o:name']
            elif 'o:email' in owner:
                return owner['o:email']
        return 'Unknown'

    def get_media(self, media_id: int) -> Optional[Dict[Any, Any]]:
        """
        Fetch a media item by ID.

        Args:
            media_id: The ID of the media to fetch

        Returns:
            Dictionary containing media data, or None if not found
        """
        try:
            url = f"{self.api_url}/media/{media_id}"
            response = self.session.get(url)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                print(f"Media with ID {media_id} not found.")
                return None
            else:
                print(f"Error fetching media: {response.status_code} - {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Network error while fetching media: {e}")
            return None

    def dereference_media(self, item_id: int, item: dict) -> bool:
        """
        Dereference the media from rcl:image property to o:media.
        Fetches the referenced item's media and recreates it on this item.

        Args:
            item_id: The ID of the item to update
            item: The item data containing rcl:image reference

        Returns:
            True if successful, False otherwise
        """
        if "rcl:image" not in item or not item["rcl:image"]:
            print(f"  ! No rcl:image found for item {item_id} - skipping")
            return True  # Return True since no error occurred, just nothing to do

        rcl_image = item["rcl:image"][0]
        referenced_item_id = rcl_image.get("value_resource_id")

        if not referenced_item_id:
            print(f"  ! No value_resource_id found in rcl:image for item {item_id}")
            return False

        print(f"  → Fetching referenced item {referenced_item_id} to get its media...")

        # Fetch the referenced item to get its media
        referenced_item = self.get_item(referenced_item_id)

        if not referenced_item:
            print(f"  ! Referenced item {referenced_item_id} not found")
            return False

        # Get the media from the referenced item
        if "o:media" not in referenced_item or not referenced_item["o:media"]:
            print(f"  ! Referenced item {referenced_item_id} has no media")
            return False

        print(f"  → Found {len(referenced_item['o:media'])} media item(s) on referenced item")

        # Fetch full details for each media item
        new_media = []
        for media_ref in referenced_item["o:media"]:
            media_id = media_ref.get("o:id")
            if not media_id:
                continue

            print(f"  → Fetching media {media_id} details...")
            media_data = self.get_media(media_id)

            if not media_data:
                print(f"  ! Could not fetch media {media_id}")
                continue

            # Determine the ingester type and source
            ingester = media_data.get("o:ingester", "url")

            # Build the new media entry - structure depends on ingester type
            new_media_entry = {
                "o:ingester": ingester,
            }

            # For URL ingester, use ingest_url
            if ingester == "url":
                original_url = media_data.get("o:original_url") or media_data.get("o:source")
                if original_url:
                    new_media_entry["ingest_url"] = original_url
                else:
                    print(f"  ! No URL found for media {media_id}")
                    continue
            # For upload ingester, we need to use the existing file
            elif ingester == "upload":
                # We can't re-upload files via API easily, so we'll use the URL of the existing file
                original_url = media_data.get("o:original_url")
                if original_url:
                    new_media_entry["o:ingester"] = "url"
                    new_media_entry["ingest_url"] = original_url
                else:
                    print(f"  ! No URL found for uploaded media {media_id}")
                    continue
            # For IIIF and other ingesters
            else:
                source = media_data.get("o:source")
                if source:
                    new_media_entry["ingest_url"] = source
                else:
                    print(f"  ! No source found for media {media_id} with ingester {ingester}")
                    continue

            # Copy relevant metadata properties
            for prop in ["dcterms:title", "dcterms:description", "dcterms:creator"]:
                if prop in media_data:
                    new_media_entry[prop] = media_data[prop]

            new_media.append(new_media_entry)

        if not new_media:
            print(f"  ! No valid media to attach")
            return False

        print(f"  → Creating {len(new_media)} new media item(s)")

        # Get a fresh copy of the item to ensure we have the latest data
        fresh_item = self.get_item(item_id)
        if not fresh_item:
            print(f"  ! Could not fetch fresh item data for {item_id}")
            return False

        # Add the new media to the item
        fresh_item["o:media"] = new_media

        return self.update_item(item_id, fresh_item)

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

    def get_all_items(self, per_page: int = 50):
        """
        Generator that fetches all items from the Omeka S instance.

        Args:
            per_page: Number of items to fetch per page

        Yields:
            Individual item dictionaries
        """
        try:
            page = 1

            while True:
                url = f"{self.api_url}/items"
                params = {
                    'page': page,
                    'per_page': per_page
                }

                response = self.session.get(url, params=params)

                if response.status_code == 200:
                    items = response.json()
                    if not items:  # No more items
                        break
                    for item in items:
                        yield item
                    page += 1
                else:
                    print(f"Error fetching items: {response.status_code} - {response.text}")
                    self.logger.error(f"Error fetching items page {page}: {response.status_code}")
                    return

        except requests.exceptions.RequestException as e:
            print(f"Network error while fetching items: {e}")
            self.logger.error(f"Network error while fetching items: {e}")
            return

    def get_item_set_items(self, item_set_id: int):
        """
        Generator that fetches all items from an item set by ID.

        Args:
            item_set_id: The ID of the item set

        Yields:
            Individual item dictionaries
        """
        try:
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
                    for item in items:
                        yield item
                    page += 1
                elif response.status_code == 404:
                    print(f"Item set with ID {item_set_id} not found.")
                    self.logger.error(f"Item set with ID {item_set_id} not found")
                    return
                else:
                    print(f"Error fetching items from item set: {response.status_code} - {response.text}")
                    self.logger.error(f"Error fetching items from item set {item_set_id}: {response.status_code}")
                    return

        except requests.exceptions.RequestException as e:
            print(f"Network error while fetching item set items: {e}")
            self.logger.error(f"Network error while fetching item set items: {e}")
            return

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
            read_only_fields = [
                'o:id',
                'o:created',
                'o:modified',
                'o:owner',
                'o:resource_class',
                'o:resource_template',
                'o:primary_media',  # This is set automatically by Omeka
                'o:thumbnail_display_urls',
                'thumbnail_display_urls',
                '@context',
                '@id',
                '@type'
            ]
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

    def process_items(self, items_generator, silent: bool = False) -> Dict[str, int]:
        """
        Process items from a generator by dereferencing their media.

        Args:
            items_generator: Generator that yields items to process
            silent: If True, don't prompt for confirmation

        Returns:
            Dictionary with 'success', 'failed', and 'skipped' counts
        """
        # First, we need to count items for confirmation
        # We'll process them in the same pass
        print("Counting items...")
        self.logger.info("Starting item processing")

        success_count = 0
        failed_count = 0
        skipped_count = 0
        total_count = 0

        for item in items_generator:
            total_count += 1

            # Ask for confirmation on first item
            if total_count == 1 and not silent:
                # We can't count ahead without loading all items
                # So we ask after seeing the first item
                confirm = input(f"\nProcess items? [y/N]: ").strip().lower()
                if confirm not in ['y', 'yes']:
                    print("Operation cancelled.")
                    self.logger.info("Operation cancelled by user")
                    return {'success': 0, 'failed': 0, 'skipped': 0}
                print("Processing items...\n")

            item_id = item.get('o:id')
            item_title = item.get('o:title', 'Untitled')
            creator = self.get_creator_name(item)

            print(f"[{total_count}] Processing item {item_id}: {item_title}")

            # Check if item has rcl:image
            if "rcl:image" not in item or not item["rcl:image"]:
                print(f"  - No rcl:image, skipping")
                self.logger.info(f"SKIPPED - Item ID: {item_id}, Title: '{item_title}', Creator: {creator}, Reason: No rcl:image property")
                skipped_count += 1
                continue

            if self.dereference_media(item_id, item):
                self.logger.info(f"PROCESSED - Item ID: {item_id}, Title: '{item_title}', Creator: {creator}")
                success_count += 1
            else:
                self.logger.error(f"FAILED - Item ID: {item_id}, Title: '{item_title}', Creator: {creator}")
                failed_count += 1

        if total_count == 0:
            print("No items to process.")
            self.logger.info("No items to process")

        self.logger.info(f"Processing complete - Success: {success_count}, Failed: {failed_count}, Skipped: {skipped_count}, Total: {total_count}")
        return {'success': success_count, 'failed': failed_count, 'skipped': skipped_count}

    def process_item_set(self, item_set_id: int, silent: bool = False) -> Dict[str, int]:
        """
        Process all items in an item set by dereferencing their media.

        Args:
            item_set_id: The ID of the item set to process
            silent: If True, don't prompt for confirmation

        Returns:
            Dictionary with 'success', 'failed', and 'skipped' counts
        """
        print(f"Fetching items from item set {item_set_id}...")
        self.logger.info(f"Fetching items from item set {item_set_id}")

        items_generator = self.get_item_set_items(item_set_id)
        return self.process_items(items_generator, silent)

    def process_all_items(self, silent: bool = False) -> Dict[str, int]:
        """
        Process all items in the entire Omeka S instance.

        Args:
            silent: If True, don't prompt for confirmation

        Returns:
            Dictionary with 'success', 'failed', and 'skipped' counts
        """
        print(f"Fetching items from the instance...")
        self.logger.info("Fetching all items from the instance")

        items_generator = self.get_all_items()
        return self.process_items(items_generator, silent)

def process_single_item(client: OmekaSClient):
    """Process a single item by ID."""
    try:
        item_id = input("Enter the item ID to fetch: ").strip()
        item_id = int(item_id)
    except ValueError:
        print("Invalid item ID. Please enter a numeric value.")
        client.logger.error("Invalid item ID entered")
        return

    print(f"Fetching item {item_id}...")
    client.logger.info(f"Fetching single item {item_id}")

    # Fetch the item
    item = client.get_item(item_id)
    if item is None:
        client.logger.error(f"Failed to fetch item {item_id}")
        return

    item_title = item.get('o:title', 'Untitled')
    creator = client.get_creator_name(item)

    print(f"Successfully fetched item: {item_title}")
    print(f"Item ID: {item.get('o:id')}")
    print(f"Creator: {creator}")
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

    # Check if item has rcl:image
    if "rcl:image" not in item or not item["rcl:image"]:
        print("\n! This item has no rcl:image property - nothing to dereference")
        client.logger.info(f"SKIPPED - Item ID: {item_id}, Title: '{item_title}', Creator: {creator}, Reason: No rcl:image property")
        return

    # Confirm before saving
    confirm = input("\nSave this item back to Omeka S dereferenced? [y/N]: ").strip().lower()
    if confirm not in ['y', 'yes']:
        print("Operation cancelled.")
        client.logger.info(f"Operation cancelled by user for item {item_id}")
        return

    print("Saving item back to Omeka S...")

    # Save the item back (unchanged)
    success = client.dereference_media(item_id, item)

    if success:
        print("✓ Item successfully saved back to Omeka S!")
        client.logger.info(f"PROCESSED - Item ID: {item_id}, Title: '{item_title}', Creator: {creator}")
    else:
        print("✗ Failed to save item.")
        client.logger.error(f"FAILED - Item ID: {item_id}, Title: '{item_title}', Creator: {creator}")

def process_item_set_items(client: OmekaSClient):
    """Process all items in an item set."""
    try:
        item_set_id = input("Enter the item set ID to process: ").strip()
        item_set_id = int(item_set_id)
    except ValueError:
        print("Invalid item set ID. Please enter a numeric value.")
        client.logger.error("Invalid item set ID entered")
        return

    results = client.process_item_set(item_set_id)

    print(f"\n=== Processing Complete ===")
    print(f"Successfully processed: {results['success']} items")
    print(f"Skipped (no rcl:image): {results['skipped']} items")
    print(f"Failed to process: {results['failed']} items")
    print(f"Total items: {results['success'] + results['failed'] + results['skipped']}")

def process_all_instance_items(client: OmekaSClient):
    """Process all items in the entire instance."""
    print("WARNING: This will process ALL items in the entire Omeka S instance.")
    confirm = input("Are you sure you want to continue? [y/N]: ").strip().lower()

    if confirm not in ['y', 'yes']:
        print("Operation cancelled.")
        client.logger.info("Process all items operation cancelled by user")
        return

    results = client.process_all_items()

    print(f"\n=== Processing Complete ===")
    print(f"Successfully processed: {results['success']} items")
    print(f"Skipped (no rcl:image): {results['skipped']} items")
    print(f"Failed to process: {results['failed']} items")
    print(f"Total items: {results['success'] + results['failed'] + results['skipped']}")

def main():
    """Main function to demonstrate the API workflow."""

    # Set up logging first
    logger, log_filename = setup_logging()
    logger.info("=== Omeka S Media Dereferencing Script Started ===")
    print(f"Logging to: {log_filename}\n")

    # Configuration - Replace these with your actual Omeka S details

    instance = input("Please enter the name of your instance, in all caps (e.g. EXHIBITS_DEV, EXHIBITS_PROD): ")
    logger.info(f"Instance selected: {instance}")

    OMEKA_BASE_URL = dotenv.get_key(".env", f"{instance}_OMEKA_BASE_URL")
    API_KEY_IDENTITY = dotenv.get_key(".env", f"{instance}_API_KEY_IDENTITY")
    API_KEY_CREDENTIAL = dotenv.get_key(".env", f"{instance}_API_KEY_CREDENTIAL")

    if not (OMEKA_BASE_URL and API_KEY_IDENTITY and API_KEY_CREDENTIAL):
        print("Instance not found. Goodbye")
        logger.error(f"Instance {instance} not found in .env file")
        sys.exit(1)

    logger.info(f"Connected to Omeka instance at {OMEKA_BASE_URL}")

    # Initialize the client
    client = OmekaSClient(OMEKA_BASE_URL, API_KEY_IDENTITY, API_KEY_CREDENTIAL, logger)

    try:
        while True:
            print("\n=== Omeka S Media Dereferencing ===")
            print("1. Process a single item by ID")
            print("2. Process all items in an item set")
            print("3. Process all items in the entire instance")
            print("4. Exit")

            choice = input("\nEnter your choice (1-4): ").strip()

            if choice == '1':
                process_single_item(client)
            elif choice == '2':
                process_item_set_items(client)
            elif choice == '3':
                process_all_instance_items(client)
            elif choice == '4':
                print("Goodbye!")
                logger.info("=== Script Ended ===")
                break
            else:
                print("Invalid choice. Please enter 1, 2, 3, or 4.")

    except KeyboardInterrupt:
        print("\nOperation cancelled. Goodbye!")
        logger.info("=== Script Interrupted by User ===")
        sys.exit(0)

if __name__ == "__main__":
    main()