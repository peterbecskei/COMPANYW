import os
import re
import csv
import aiohttp
import asyncio
import aiofiles
from urllib.parse import urlparse
from typing import List

# === CONSTANTS ===
cwd = os.getcwd()
URL_LIST = "URL_LIST.csv"
FILTERED_URL_LIST = "FILTERED_URL_LIST.csv"
PROXY = "36fda789ac44aa4cc19e:b966e984e5922790@gw.dataimpulse.com:10012"
DATAFOLDER = os.path.join(cwd, "Companies")
CONCURRENT_WORKERS = 10
TIMEOUT = 30
BATCH_SIZE = 10000  # URLs per sub-list

# === PROXY SETUP ===
proxy_url = f"http://{PROXY}"


# === URL PARSING ===
def parse_url_tree(url):
    """Extract URL tree components from companywall.hu URLs"""
    parsed = urlparse(url)
    path_parts = parsed.path.strip('/').split('/')

    if len(path_parts) >= 3 and path_parts[0] == 'vállalat':
        company_part = path_parts[1]  # e.g., "horizontplast-kft"
        return company_part
    return None


# === LEVEL 1 - FILTER URLS ===
def filter_urls():
    """Level 1: Filter URLs based on company type patterns"""
    if not os.path.exists(URL_LIST):
        print(f"❌ {URL_LIST} not found!")
        return

    filtered = []
    with open(URL_LIST, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        urls = [row[0].strip() for row in reader if row]

    print(f"Level 2: Filtering {len(urls)} URLs...")

    pattern_include = re.compile(r'-kft|-bt|-zrt', re.IGNORECASE)
    #pattern_include = re.compile(r'horizon', re.IGNORECASE)

    # pattern_exclude = re.compile(r'-v-a|-f-a', re.IGNORECASE)
    pattern_exclude = re.compile(r'-xxxxxxxxxxxv-a', re.IGNORECASE)
    for url in urls:
        if pattern_include.search(url) and not pattern_exclude.search(url):
            filtered.append(url)

    # Save to FILTERED_URL_LIST.csv
    with open(FILTERED_URL_LIST, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for url in filtered:
            writer.writerow([url])
#    print(f"Level 2 complete. {len(filtered)} URLs saved to {FILTERED_URL_LIST}")

    print(f"✅ Level 1 complete: {len(filtered)} URLs filtered -> {FILTERED_URL_LIST}")



# === LEVEL 2 - CREATE SUB LISTS ===
def create_sub_lists():
    """Level 2: Divide filtered URLs into 10,000 URL parts"""
    if not os.path.exists(FILTERED_URL_LIST):
        print(f"❌ {FILTERED_URL_LIST} not found! Run Level 1 first.")
        return 0

    # Read filtered URLs
    with open(FILTERED_URL_LIST, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        urls = [row[0] for row in reader if row]

    total_urls = len(urls)
    if total_urls == 0:
        print("❌ No URLs found in filtered list!")
        return 0

    # Calculate number of sub-lists needed
    num_sublists = (total_urls + BATCH_SIZE - 1) // BATCH_SIZE  # Ceiling division

    print(f"📊 Total URLs: {total_urls}")
    print(f"📦 Creating {num_sublists} sub-lists with {BATCH_SIZE} URLs each...")

    # Create sub-lists
    sublist_count = 0
    for i in range(0, total_urls, BATCH_SIZE):
        sublist_count += 1
        batch = urls[i:i + BATCH_SIZE]
        sublist_filename = f"URL_LIST{sublist_count}.csv"

        with open(sublist_filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for url in batch:
                writer.writerow([url])

        print(f"✅ Created {sublist_filename} with {len(batch)} URLs")

    print(f"🎉 Level 2 complete: Created {sublist_count} sub-lists")
    return sublist_count


# === LEVEL 3 - ASYNC CONTENT FETCHING ===
async def fetch_single(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore):
    """Fetch single URL with semaphore control"""
    async with semaphore:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Encoding': 'gzip'
        }

        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as response:
                if response.status == 200:
                    html_content = await response.text()
                    return url, html_content, None
                else:
                    return url, None, f"HTTP {response.status}"
        except asyncio.TimeoutError:
            return url, None, "Timeout"
        except Exception as e:
            return url, None, str(e)


async def save_html_content(url_tree: str, html_content: str):
    """Save HTML content to file asynchronously"""
    try:
        folder_code = url_tree[:2].upper()
        filename = f"{url_tree}.html"
        folder_path = os.path.join(DATAFOLDER, folder_code)

        # Create folder if needed
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, filename)

        # Skip if already exists
        if os.path.exists(file_path):
            return "exists"

        async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
            await f.write(html_content)
        return "saved"
    except Exception as e:
        return f"error: {str(e)}"


async def process_url_batch(session: aiohttp.ClientSession, batch: List[str], semaphore: asyncio.Semaphore,
                            batch_num: int):
    """Process a batch of URLs concurrently"""
    tasks = []
    for url in batch:
        task = fetch_single(session, url, semaphore)
        tasks.append(task)

    print(f"🔄 Processing batch {batch_num} with {len(batch)} URLs...")
    results = await asyncio.gather(*tasks, return_exceptions=True)

    success_count = 0
    error_count = 0
    exists_count = 0

    for result in results:
        if isinstance(result, Exception):
            error_count += 1
            continue

        url, html_content, error = result
        url_tree = parse_url_tree(url)

        if html_content and url_tree:
            save_result = await save_html_content(url_tree, html_content)
            if save_result == "saved":
                success_count += 1
            elif save_result == "exists":
                exists_count += 1
            else:
                error_count += 1
        else:
            error_count += 1

    print(f"✅ Batch {batch_num}: {success_count} saved, {exists_count} existed, {error_count} errors")
    return success_count, exists_count, error_count


async def fetch_sublist_async(sublist_number: int):
    """Fetch content from a specific sub-list"""
    sublist_filename = f"URL_LIST{sublist_number}.csv"

    if not os.path.exists(sublist_filename):
        print(f"❌ {sublist_filename} not found!")
        return 0, 0, 0

    # Read URLs from sub-list
    with open(sublist_filename, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        urls = [row[0] for row in reader if row]

    print(f"📥 Starting async fetch for {sublist_filename} ({len(urls)} URLs)...")

    connector = aiohttp.TCPConnector(limit=CONCURRENT_WORKERS, limit_per_host=CONCURRENT_WORKERS)
    semaphore = asyncio.Semaphore(CONCURRENT_WORKERS)

    total_success = 0
    total_exists = 0
    total_errors = 0

    async with aiohttp.ClientSession(connector=connector) as session:
        # Process in smaller batches to avoid memory issues
        processing_batch_size = 500
        batch_num = 1

        for i in range(0, len(urls), processing_batch_size):
            batch = urls[i:i + processing_batch_size]
            success, exists, errors = await process_url_batch(session, batch, semaphore, batch_num)
            total_success += success
            total_exists += exists
            total_errors += errors
            batch_num += 1

            # Small delay between batches
            await asyncio.sleep(1)

    print(f"🎉 {sublist_filename} completed: {total_success} saved, {total_exists} existed, {total_errors} errors")
    return total_success, total_exists, total_errors


def run_async_fetch():
    """Run async fetch for specific sub-lists"""
    print("\nAvailable sub-lists:")
    sublist_files = [f for f in os.listdir('.') if
                     f.startswith('URL_LIST') and f.endswith('.csv') and f != 'URL_LIST.csv']

    if not sublist_files:
        print("❌ No sub-list files found! Run Level 2 first.")
        return

    for file in sorted(sublist_files):
        with open(file, 'r', encoding='utf-8') as f:
            line_count = sum(1 for _ in f)
        print(f"  {file}: {line_count} URLs")

    print("\nOptions:")
    print("1. Fetch specific sub-list")
    print("2. Fetch all sub-lists")

    choice = input("Enter choice (1-2): ").strip()

    if choice == '1':
        sublist_num = input("Enter sub-list number (e.g., 1 for URL_LIST1.csv): ").strip()
        try:
            sublist_num = int(sublist_num)
            print(f"\n🎯 Starting fetch for URL_LIST{sublist_num}.csv...")

            if os.name == 'nt':
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

            success, exists, errors = asyncio.run(fetch_sublist_async(sublist_num))
            print(f"\n📊 Sub-list {sublist_num} Summary: {success} saved, {exists} existed, {errors} errors")

        except ValueError:
            print("❌ Invalid sub-list number!")
        except Exception as e:
            print(f"❌ Error: {e}")

    elif choice == '2':
        print(f"\n🎯 Starting fetch for ALL {len(sublist_files)} sub-lists...")
        total_success = 0
        total_exists = 0
        total_errors = 0

        for i in range(1, len(sublist_files) + 1):
            print(f"\n{'=' * 50}")
            print(f"Processing Sub-list {i}/{len(sublist_files)}")
            print(f"{'=' * 50}")

            try:
                if os.name == 'nt':
                    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

                success, exists, errors = asyncio.run(fetch_sublist_async(i))
                total_success += success
                total_exists += exists
                total_errors += errors

            except Exception as e:
                print(f"❌ Error processing sub-list {i}: {e}")

        print(f"\n🎉 ALL SUB-LISTS COMPLETED!")
        print(f"📊 Grand Total: {total_success} saved, {total_exists} existed, {total_errors} errors")

    else:
        print("❌ Invalid choice!")


# === MAIN EXECUTION ===
def main():
    """Main function with 3-level execution"""
    print("🚀 CWSITEMAPROXY - 3-Level Web Scraping Tool")
    print("=" * 50)
    print(f"⚡ Concurrent workers: {CONCURRENT_WORKERS}")
    print(f"📦 Batch size: {BATCH_SIZE} URLs per sub-list")
    print("=" * 50)

    while True:
        print("\nSelect operation:")
        print("1. Level 1 - Filter URLs")
        print("2. Level 2 - Divide into 10,000 URL parts")
        print("3. Level 3 - Fetch Content (Async)")
        print("4. Exit")

        choice = input("\nEnter choice (1-4): ").strip()

        if choice == '1':
            print("\n🎯 Running Level 1 - URL Filtering...")
            filtered_count = filter_urls()
            if filtered_count:
                estimated_sublists = (filtered_count + BATCH_SIZE - 1) // BATCH_SIZE
                print(f"📈 Estimated sub-lists: {estimated_sublists}")

        elif choice == '2':
            print("\n🎯 Running Level 2 - Creating Sub-lists...")
            sublist_count = create_sub_lists()
            if sublist_count:
                print(f"📈 Ready for Level 3: {sublist_count} sub-lists created")

        elif choice == '3':
            print("\n🎯 Running Level 3 - Async Content Fetching...")
            run_async_fetch()

        elif choice == '4':
            print("👋 Exiting...")
            break

        else:
            print("❌ Invalid choice!")


if __name__ == "__main__":
    main()