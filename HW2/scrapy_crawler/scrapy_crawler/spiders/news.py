import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from urllib.parse import urlparse
import pandas as pd
import os

class latimesSpider(CrawlSpider):
    name = "latimes"
    allowed_domains = ["latimes.com"]
    start_urls = ["https://www.latimes.com/"]
    base_domain = "latimes.com"  # Base domain for checking internal/external links

    # CSV Files
    fetch_file = "fetch_latimes.csv"
    visit_file = "visit_latimes.csv"
    urls_file = "urls_latimes.csv"
    report_file = "CrawlReport_latimes.txt"

    custom_settings = {
        "CLOSESPIDER_PAGECOUNT": 20000,  # Limit crawling to 20,000 pages
        "HTTPERROR_ALLOW_ALL": True,  # Allow all HTTP status codes to be processed
        "REDIRECT_ENABLED": False,  # Disable automatic redirects to capture 3XX responses
        "COOKIES_ENABLED": False,
        'CONCURRENT_REQUESTS': 7,
        'RETRY_ENABLED': True,
        'ROBOTSTXT_OBEY': False,
        'DEPTH_LIMIT': 16,  # Set maximum depth to 16
    }

    # Allowed file types for crawling
    allowed_file_types = (".html", ".htm", ".pdf", ".doc", ".docx",".jpg", ".jpeg", ".png", ".gif", ".tiff", ".webp")

    rules = (
        Rule(
            LinkExtractor(
                allow=allowed_file_types,  # Restrict links to specific formats
                tags=('a', 'img', 'source', 'video', 'audio', 'link', 'script'),
                attrs=('href', 'src'),
            ),
            callback="parse_item",
            follow=True,
        ),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Initialize CSV files with headers
        with open(self.fetch_file, "w") as f:
            f.write("URL,Status\n")
        
        with open(self.visit_file, "w") as f:
            f.write("URL,Size (Bytes),Outlinks,Content-Type\n")
        
        with open(self.urls_file, "w") as f:
            f.write("Encountered URL,Indicator\n")
    

    def parse_item(self, response):
        
        # Capture redirect URLs
        redirect_urls = response.meta.get("redirect_urls", [])  # Get previous URL(s) if redirected

        # Store all attempted URLs
        with open(self.fetch_file, "a") as f:
            f.write(f"{response.url},{response.status}\n")

        # If successful (HTTP 200), process it for visit file
        if response.status == 200:
            content_size = len(response.body) if response.body else 0

            outlinks = response.css("a::attr(href)").getall() or [] # Extract all outlinks
            
            # Extract Content-Type safely
            raw_content_type = response.headers.get("Content-Type", b"").decode(errors="ignore") if response.headers.get("Content-Type") else "unknown"

            # Ensure we always get a valid content type
            content_type = raw_content_type.split(';')[0].strip() if raw_content_type else "unknown"


            with open(self.visit_file, "a") as f:
                f.write(f"{response.url},{content_size:.2f},{len(outlinks)},{content_type}\n")

            # Store all encountered URLs with OK/N_OK indicator
            for link in outlinks:
                absolute_url = response.urljoin(link)
                indicator = "OK" if urlparse(absolute_url).netloc.endswith(self.base_domain) else "N_OK"

                with open(self.urls_file, "a") as f:
                    f.write(f"{absolute_url},{indicator}\n")

        # If this response is a redirected URL, log both the original and the final URL
        if redirect_urls:
            for original_url in redirect_urls:
                # Determine if the original URL is internal or external
                redirect_indicator = "OK" if urlparse(original_url).netloc.endswith(self.base_domain) else "N_OK"
                with open(self.urls_file, "a") as f:
                    f.write(f"{original_url},{redirect_indicator}\n")


    def closed(self, reason):
        """ Generates statistics and saves to CrawlReport_latimes.txt """
        
        # Read CSV files
        fetch_df = pd.read_csv(self.fetch_file, on_bad_lines='skip') if os.path.exists(self.fetch_file) else pd.DataFrame(columns=["URL", "Status"])
        visit_df = pd.read_csv(self.visit_file, on_bad_lines='skip') if os.path.exists(self.visit_file) else pd.DataFrame(columns=["URL", "Size (Bytes)", "Outlinks", "Content-Type"])
        urls_df = pd.read_csv(self.urls_file, on_bad_lines='skip') if os.path.exists(self.urls_file) else pd.DataFrame(columns=["Encountered URL", "Indicator"])

        # Major HTTP Status Code Descriptions
        status_code_descriptions = {
            200: "OK",
            201: "Created",
            204: "No Content",
            301: "Moved Permanently",
            302: "Found (Moved Temporarily)",
            304: "Not Modified",
            400: "Bad Request",
            401: "Unauthorized",
            403: "Forbidden",
            404: "Not Found",
            405: "Method Not Allowed",
            408: "Request Timeout",
            429: "Too Many Requests",
            500: "Internal Server Error",
            502: "Bad Gateway",
            503: "Service Unavailable",
            504: "Gateway Timeout",
        }

        visit_df["Size (Bytes)"] = visit_df["Size (Bytes)"].astype(float).round(2)
        
        # Fetch statistics
        total_fetches = len(fetch_df)
        successful_fetches = len(fetch_df[fetch_df["Status"].between(200, 299)])
        failed_fetches = total_fetches - successful_fetches

        # Outgoing URLs statistics
        total_extracted = len(urls_df)
        unique_extracted = urls_df["Encountered URL"].nunique()
        unique_internal = len(urls_df[urls_df["Indicator"] == "OK"]["Encountered URL"].unique())
        unique_external = len(urls_df[urls_df["Indicator"] == "N_OK"]["Encountered URL"].unique())


        # HTTP Status Code Counts
        status_counts = fetch_df["Status"].value_counts().to_dict()
        # Format status codes with descriptions
        formatted_status_codes = []
        for code, count in sorted(status_counts.items()):  # Sort by status code
            description = status_code_descriptions.get(code, "") 
            formatted_status_codes.append(f"{code} {description}: {count}")

        # File size distribution
        file_sizes = visit_df["Size (Bytes)"].astype(float)
        size_ranges = {
            "< 1 KB": sum(file_sizes < 1024),  # Less than 1 KB (1024 bytes)
            "1 KB - 10 KB": sum((file_sizes >= 1024) & (file_sizes < 10 * 1024)),  # 1 KB to 10 KB
            "10 KB - 100 KB": sum((file_sizes >= 10 * 1024) & (file_sizes < 100 * 1024)),  # 10 KB to 100 KB
            "100 KB - 1 MB": sum((file_sizes >= 100 * 1024) & (file_sizes < 1024 * 1024)),  # 100 KB to 1 MB
            "> 1 MB": sum(file_sizes >= 1024 * 1024),  # Greater than 1 MB
        }


        # Content types encountered
        content_types = visit_df["Content-Type"].value_counts().to_dict()

        # Writing statistics to file
        with open(self.report_file, "w") as f:
            f.write(f"Name: Komali Beeram\n")
            f.write(f"USC ID: 9327372983\n")
            f.write(f"News site crawled: latimes.com\n")
            f.write(f"Number of threads: {self.custom_settings['CONCURRENT_REQUESTS']}\n\n")

            f.write("Fetch Statistics\n================\n")
            f.write(f"# Fetches attempted: {total_fetches}\n")
            f.write(f"# Fetches succeeded: {successful_fetches}\n")
            f.write(f"# Fetches failed or aborted: {failed_fetches}\n\n")

            f.write("Outgoing URLs:\n================\n")
            f.write(f"Total URLs extracted: {total_extracted}\n")
            f.write(f"# Unique URLs extracted: {unique_extracted}\n")
            f.write(f"# Unique URLs within News Site: {unique_internal}\n")
            f.write(f"# Unique URLs outside News Site: {unique_external}\n\n")

            f.write("Status Codes:\n================\n")
            f.write("\n".join(formatted_status_codes) + "\n\n")

            f.write("File Sizes:\n================\n")
            for size_range, count in size_ranges.items():
                f.write(f"{size_range}: {count}\n")
            f.write("\n")

            f.write("Content Types:\n================\n")
            for ctype, count in content_types.items():
                f.write(f"{ctype}: {count}\n")
            f.write("\n")

        print(f"\nCrawl Report saved as {self.report_file}")