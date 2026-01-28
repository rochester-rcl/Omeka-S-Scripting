import dotenv
import requests

def get_input(prompt):
    try:
        return input(prompt)
    except KeyboardInterrupt:
        print("\nExiting.")
        exit(0)

def item_id_generator(base_url, api_key, item_set_id):
    """Generator that yields item IDs from a given Omeka S item set."""
    page = 1
    while True:
        params = {
            "item_set_id[]": item_set_id,
            "page": page,
            "per_page": 100,
            "key_identity": api_key[0],
            "key_credential": api_key[1]
        }
        resp = requests.get(f"{base_url}/api/items", params=params)
        resp.raise_for_status()
        items = resp.json()
        if not items:
            break
        for item in items:
            yield item["o:id"]
        page += 1

def main():
    print("=== Omeka S Title Sync Script ===")
    prod_url = dotenv.get_key(".env", "OMEKA_BASE_URL")
    prod_key_identity = dotenv.get_key(".env", "API_KEY_IDENTITY")
    prod_key_credential = dotenv.get_key(".env", "API_KEY_CREDENTIAL")

    dev_url = dotenv.get_key(".env", "DEV_OMEKA_BASE_URL")
    dev_key_identity = dotenv.get_key(".env", "DEV_API_KEY_IDENTITY")
    dev_key_credential = dotenv.get_key(".env", "DEV_API_KEY_CREDENTIAL")

    item_set_id = 256562

    prod_api_key = (prod_key_identity, prod_key_credential)
    dev_api_key = (dev_key_identity, dev_key_credential)

    for item_id in item_id_generator(prod_url, prod_api_key, item_set_id):
        print(f"Processing item {item_id}...")
        # Fetch from Dev
        dev_params = {
            "key_identity": dev_api_key[0],
            "key_credential": dev_api_key[1]
        }
        dev_resp = requests.get(f"{dev_url}/api/items/{item_id}", params=dev_params)
        if dev_resp.status_code == 404:
            print(f"[WARN] Item {item_id} not found on Dev. Skipping.")
            continue
        dev_resp.raise_for_status()
        dev_item = dev_resp.json()

        # Extract title
        title_value = None
        if "dcterms:title" in dev_item and dev_item["dcterms:title"]:
            title_value = dev_item["dcterms:title"][0]["@value"]

        if title_value is None:
            print(f"[WARN] No title found for item {item_id} on Dev. Skipping.")
            continue

        # Fetch from Prod
        prod_params = {
            "key_identity": prod_api_key[0],
            "key_credential": prod_api_key[1]
        }
        prod_resp = requests.get(f"{prod_url}/api/items/{item_id}", params=prod_params)
        if prod_resp.status_code == 404:
            print(f"[WARN] Item {item_id} not found on Prod. Skipping.")
            continue
        prod_resp.raise_for_status()
        prod_item = prod_resp.json()

        # Replace title
        prod_item["dcterms:title"] = [{
            "type": "literal",
            "property_id": prod_item["dcterms:title"][0]["property_id"] if prod_item.get("dcterms:title") else 1,
            "@value": title_value
        }]

        # Save to Prod
        put_resp = requests.put(
            f"{prod_url}/api/items/{item_id}",
            params=prod_params,
            json=prod_item
        )
        if put_resp.status_code in (200, 201):
            print(f"[OK] Updated title for item {item_id}")
        else:
            print(f"[ERROR] Failed to update item {item_id}: {put_resp.status_code} {put_resp.text}")

if __name__ == "__main__":
    main()
