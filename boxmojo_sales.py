import scrapy
import json
import re
from urllib.parse import quote_plus
from boxmojo.items import SalesItem


def money_to_int(text: str):
    if not text:
        return None
    text = text.strip()
    m = re.search(r"\$([\d,]+)", text)
    if not m:
        return None
    return int(m.group(1).replace(",", ""))


def extract_gross(page_text: str, label: str):
    """
    Extracts money values from the 'All Releases' summary box text.
    Handles formats like:
      DOMESTIC (33.3%) $393,242,207
      INTERNATIONAL (66.7%) $1,313,300,000
      WORLDWIDE $1,706,542,207
    Works even if there are line breaks / extra spaces.
    """
    if not page_text:
        return None

    # With percent (allow decimals like 33.3%)
    m = re.search(
        rf"{label}\s*\(\s*[\d.]+\s*%\s*\)\s*\$([\d,]+)",
        page_text,
        re.IGNORECASE,
    )
    if m:
        return int(m.group(1).replace(",", ""))

    # Without percent (some pages show Worldwide without percent)
    m = re.search(
        rf"{label}\s*\$([\d,]+)",
        page_text,
        re.IGNORECASE,
    )
    if m:
        return int(m.group(1).replace(",", ""))

    return None


class BoxMojoSalesSpider(scrapy.Spider):
    name = "boxmojo_sales"
    allowed_domains = ["www.boxofficemojo.com", "boxofficemojo.com"]

    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "AUTOTHROTTLE_ENABLED": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "ROBOTSTXT_OBEY": False,
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.8",
        },
    }

    def __init__(self, targets_path="targets.jsonl", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.targets_path = targets_path

    def start_requests(self):
        # Read JSONL: one JSON object per line
        with open(self.targets_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                row = json.loads(line)
                title = row.get("title")
                year = row.get("year")

                if not title:
                    continue

                q = quote_plus(title)
                search_url = f"https://www.boxofficemojo.com/search/?q={q}"

                yield scrapy.Request(
                    url=search_url,
                    callback=self.parse_search,
                    meta={"input_title": title, "input_year": year},
                    dont_filter=True,
                )

    # Ahmed ----------------------------------------------
    def parse_search(self, response):
        input_title = response.meta["input_title"]
        input_year = response.meta.get("input_year")

        # collect candidate title links from search results
        candidates = []
        for a in response.css('a[href^="/title/tt"]'):
            href = a.attrib.get("href")
            if not href:
                continue

            anchor_title = " ".join(
                t.strip() for t in a.css("::text").getall() if t.strip()
            )

            block = a.xpath("ancestor::div[1]")
            block_text = " ".join(
                t.strip() for t in block.css("::text").getall() if t.strip()
            )

            candidates.append(
                {
                    "href": href,
                    "anchor_title": anchor_title,
                    "block_text": block_text,
                }
            )

        if not candidates:
            yield SalesItem(
                input_title=input_title,
                input_year=input_year,
                bom_title=None,
                budget=None,
                opening_weekend=None,
                gross_worldwide=None,
                gross_domestic=None,
                gross_international=None,
                release_date=None,
                genres=None,
                runtime_minutes=None,
                filmmakers=None,
                cast=None,
                source_url=None,
            )
            return

        # pick best candidate: year match first, otherwise first result
        best = None
        if input_year:
            y = str(input_year)
            for c in candidates:
                if f"({y})" in c["block_text"]:
                    best = c
                    break
        if not best:
            best = candidates[0]

        title_url = response.urljoin(best["href"])

        yield scrapy.Request(
            url=title_url,
            callback=self.parse_title,
            meta={"input_title": input_title, "input_year": input_year},
            dont_filter=True,
        )

    def parse_title(self, response):
        input_title = response.meta["input_title"]
        input_year = response.meta.get("input_year")

        bom_title = response.css("h1::text").get()
        bom_title = bom_title.strip() if bom_title else None

        # Full visible text (robust)
        page_text = " ".join(
            t.strip() for t in response.css("body *::text").getall() if t and t.strip()
        )

        # ------------------------------------------------------------
        # A) Parse the summary/info (table if exists, else flex rows)
        # ------------------------------------------------------------
        info = {}

        # 1) Try HTML table rows first
        for tr in response.css("table tr"):
            label = tr.css("th:nth-child(1)::text, td:nth-child(1)::text").get()
            if not label:
                continue

            value_parts = tr.css("td:nth-child(2)::text, td:nth-child(2) a::text").getall()
            value = " ".join(v.strip() for v in value_parts if v and v.strip())

            label = label.strip().rstrip(":").strip()
            if value:
                info[label] = value

        # 2) Fallback: flex rows with 2 spans (what you saw in DevTools)
        if not info:
            for row in response.css("div.a-section.a-spacing-none"):
                spans = [s.strip() for s in row.css("span::text").getall() if s and s.strip()]
                if len(spans) >= 2:
                    label = spans[0].strip().rstrip(":").strip()
                    value = spans[1].strip()
                    if label and value:
                        info[label] = value

        # ------------------------------------------------------------
        # B) Extract required fields (sales + core metadata)
        # ------------------------------------------------------------

        # Sales: use robust extractor that supports decimal % and line breaks
        gross_domestic = extract_gross(page_text, "Domestic")
        gross_international = extract_gross(page_text, "International")
        gross_worldwide = extract_gross(page_text, "Worldwide")

        genres = info.get("Genres")
        if genres:
            # normalize: collapse any weird whitespace/newlines
            genres = " ".join(genres.split())

        # Release date (key name varies)
        release_date = (
            info.get("Release Date")
            or info.get("Earliest Release Date")
            or info.get("Domestic Release Date")
        )

        # Runtime
        runtime_minutes = None
        rt = info.get("Running Time") or info.get("Runtime")
        if rt:
            hrs = re.search(r"(\d+)\s*hr", rt, re.IGNORECASE)
            mins = re.search(r"(\d+)\s*min", rt, re.IGNORECASE)
            h = int(hrs.group(1)) if hrs else 0
            mi = int(mins.group(1)) if mins else 0
            runtime_minutes = h * 60 + mi if (h or mi) else None
        else:
            m = re.search(r"Running\s*Time\s*(\d+)\s*hr\s*(\d+)\s*min", page_text, re.IGNORECASE)
            if m:
                runtime_minutes = int(m.group(1)) * 60 + int(m.group(2))

        # Opening weekend (BOM label is usually "Domestic Opening")
        opening_weekend = None
        dom_open = info.get("Domestic Opening")
        if dom_open:
            opening_weekend = money_to_int(dom_open)
        else:
            m = re.search(r"Domestic\s*Opening\s*\$([\d,]+)", page_text, re.IGNORECASE)
            if m:
                opening_weekend = int(m.group(1).replace(",", ""))

        # Budget usually not present
        budget = None

        # ------------------------------------------------------------
        # C) Follow to credits page to fetch filmmakers + cast
        # ------------------------------------------------------------
        item = SalesItem(
            input_title=input_title,
            input_year=input_year,
            bom_title=bom_title,
            budget=budget,
            opening_weekend=opening_weekend,
            gross_worldwide=gross_worldwide,
            gross_domestic=gross_domestic,
            gross_international=gross_international,
            release_date=release_date,
            genres=genres,
            runtime_minutes=runtime_minutes,
            filmmakers=None,
            cast=None,
            source_url=response.url,
        )

        credits_url = response.urljoin("credits/")
        yield scrapy.Request(
            url=credits_url,
            callback=self.parse_credits,
            meta={"item": item},
            dont_filter=True,
        )

    def parse_credits(self, response):
        item = response.meta["item"]

        filmmakers = []
        cast = []

        # Filmmakers table: name in first col, role in second col
        for tr in response.css("table tr"):
            tds = tr.css("td")
            if len(tds) >= 2:
                name = " ".join(tds[0].css("::text").getall()).strip()
                role = " ".join(tds[1].css("::text").getall()).strip()
                if name and role:
                    filmmakers.append({"name": " ".join(name.split()), "role": " ".join(role.split())})

        # Cast table: name in first col, role/character in second col
        # On BOM credits pages, cast can also appear in similar tables.
        # We'll store anything we see; you can filter later.
        for tr in response.css("table tr"):
            tds = tr.css("td")
            if len(tds) >= 2:
                actor = " ".join(tds[0].css("::text").getall()).strip()
                role = " ".join(tds[1].css("::text").getall()).strip()
                if actor and role:
                    cast.append({"actor": " ".join(actor.split()), "role": " ".join(role.split())})

        # De-duplicate (simple repeats)
        def dedupe_list_of_dicts(lst, keys):
            seen = set()
            out = []
            for d in lst:
                k = tuple(d.get(x) for x in keys)
                if k in seen:
                    continue
                seen.add(k)
                out.append(d)
            return out

        filmmakers = dedupe_list_of_dicts(filmmakers, ("name", "role"))
        cast = dedupe_list_of_dicts(cast, ("actor", "role"))

        item["filmmakers"] = filmmakers if filmmakers else None
        item["cast"] = cast if cast else None

        yield item


#---------------------------------------------- old as a backup --------- Ahmed

# import scrapy
# import json
# import re
# from urllib.parse import quote_plus

# from boxmojo.items import SalesItem


# def money_to_int(text: str):
#     """Convert '$1,657,599,388' -> 1657599388. Return None if missing/invalid."""
#     if not text:
#         return None
#     text = text.strip()
#     if text in {"-", "—", "N/A", "na"}:
#         return None
#     m = re.search(r"\$([\d,]+)", text)
#     if not m:
#         return None
#     return int(m.group(1).replace(",", ""))


# def parse_runtime_minutes(rt: str):
#     """Convert '1 hr 48 min' -> 108."""
#     if not rt:
#         return None
#     rt = " ".join(rt.split())
#     hrs = re.search(r"(\d+)\s*hr", rt, re.IGNORECASE)
#     mins = re.search(r"(\d+)\s*min", rt, re.IGNORECASE)
#     h = int(hrs.group(1)) if hrs else 0
#     mi = int(mins.group(1)) if mins else 0
#     total = h * 60 + mi
#     return total if total > 0 else None


# def normalize_whitespace(s: str):
#     return " ".join(s.split()).strip() if s else s


# class BoxMojoSalesSpider(scrapy.Spider):
#     """
#     BoxOfficeMojo spider:
#     - Input: JSONL file (targets.jsonl) with {"title": "...", "year": 2016}
#     - Flow: search -> pick best /title/tt... -> parse title summary
#     - Outputs: domestic, international, worldwide, domestic opening, release date, genres, runtime
#     - Optional: fetch credits page to get filmmakers + cast
#     """
#     name = "boxmojo_sales"
#     allowed_domains = ["www.boxofficemojo.com", "boxofficemojo.com"]

#     custom_settings = {
#         "DOWNLOAD_DELAY": 2,
#         "AUTOTHROTTLE_ENABLED": True,
#         "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
#         "ROBOTSTXT_OBEY": False,
#         "USER_AGENT": (
#             "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#             "AppleWebKit/537.36 (KHTML, like Gecko) "
#             "Chrome/120.0.0.0 Safari/537.36"
#         ),
#         "DEFAULT_REQUEST_HEADERS": {
#             "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
#             "Accept-Language": "en-US,en;q=0.8",
#         },
#     }

#     def __init__(self, targets_path="targets.jsonl", *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.targets_path = targets_path

#     # -------------------------
#     # START REQUESTS (targets)
#     # -------------------------
#     def start_requests(self):
#         with open(self.targets_path, "r", encoding="utf-8") as f:
#             for line in f:
#                 line = line.strip()
#                 if not line:
#                     continue
#                 row = json.loads(line)

#                 title = row.get("title")
#                 year = row.get("year")

#                 if not title:
#                     continue

#                 q = quote_plus(title)
#                 search_url = f"https://www.boxofficemojo.com/search/?q={q}"

#                 yield scrapy.Request(
#                     url=search_url,
#                     callback=self.parse_search,
#                     meta={"input_title": title, "input_year": year},
#                     dont_filter=True,
#                 )

#     # -------------------------
#     # SEARCH RESULTS -> PICK BEST TITLE PAGE
#     # -------------------------
#     def parse_search(self, response):
#         input_title = response.meta["input_title"]
#         input_year = response.meta.get("input_year")

#         # Collect candidate /title/tt... links
#         candidates = []
#         for a in response.css('a[href^="/title/tt"]'):
#             href = a.attrib.get("href")
#             if not href:
#                 continue

#             # Title text on anchor
#             anchor_title = normalize_whitespace(" ".join(a.css("::text").getall()))

#             # Try to capture year from the closest container (heuristic)
#             block = a.xpath("ancestor::div[1]")
#             block_text = normalize_whitespace(" ".join(block.css("::text").getall()))

#             candidates.append(
#                 {"href": href, "anchor_title": anchor_title, "block_text": block_text}
#             )

#         if not candidates:
#             yield SalesItem(
#                 input_title=input_title,
#                 input_year=input_year,
#                 bom_title=None,
#                 budget=None,
#                 opening_weekend=None,
#                 gross_worldwide=None,
#                 gross_domestic=None,
#                 gross_international=None,
#                 release_date=None,
#                 genres=None,
#                 runtime_minutes=None,
#                 filmmakers=None,
#                 cast=None,
#                 source_url=None,
#             )
#             return

#         # Year match first, else first candidate
#         best = None
#         if input_year:
#             y = str(input_year)
#             for c in candidates:
#                 if f"({y})" in c["block_text"]:
#                     best = c
#                     break
#         if not best:
#             best = candidates[0]

#         title_url = response.urljoin(best["href"])
#         yield scrapy.Request(
#             url=title_url,
#             callback=self.parse_title,
#             meta={"input_title": input_title, "input_year": input_year},
#             dont_filter=True,
#         )

#     # -------------------------
#     # TITLE PAGE -> PARSE SUMMARY TABLE + SALES
#     # -------------------------
#     def parse_title(self, response):
#         input_title = response.meta["input_title"]
#         input_year = response.meta.get("input_year")

#         bom_title = response.css("h1::text").get()
#         bom_title = bom_title.strip() if bom_title else None

#         # Full text fallback (robust)
#         page_text = " ".join(
#             t.strip()
#             for t in response.css("body *::text").getall()
#             if t and t.strip()
#         )

#         # ------------------------------------------
#         # A) Build info dict from table OR flex rows
#         # (NO comma selectors; Scrapy-safe)
#         # ------------------------------------------
#         info = {}

#         # 1) Preferred: parse table rows
#         for tr in response.css("table tr"):
#             label = tr.css("th:nth-child(1)::text").get()
#             if not label:
#                 label = tr.css("td:nth-child(1)::text").get()
#             if not label:
#                 continue

#             label = label.strip().rstrip(":").strip()

#             # second column: combine plain text + link text (Scrapy-safe)
#             value_parts = tr.css("td:nth-child(2)::text").getall()
#             value_parts += tr.css("td:nth-child(2) a::text").getall()
#             value = " ".join(v.strip() for v in value_parts if v and v.strip())
#             value = normalize_whitespace(value)

#             if value:
#                 info[label] = value

#         # 2) Fallback: flex rows with <div class="a-section a-spacing-none"><span>Label</span><span>Value</span></div>
#         if not info:
#             for row in response.css("div.a-section.a-spacing-none"):
#                 spans = [s.strip() for s in row.css("span::text").getall() if s and s.strip()]
#                 if len(spans) >= 2:
#                     label = spans[0].strip().rstrip(":").strip()
#                     value = spans[1].strip()
#                     if label and value:
#                         info[label] = normalize_whitespace(value)

#         # ------------------------------------------
#         # B) Extract the fields you want
#         # ------------------------------------------
#         # Sales (most important): Domestic / International / Worldwide
#         gross_domestic = None
#         gross_international = None
#         gross_worldwide = None

#         # Best: sometimes shown as "DOMESTIC (33.3%) $341,268,248"
#         m = re.search(r"DOMESTIC\s*\(\d+(\.\d+)?%\)\s*\$([\d,]+)", page_text, re.IGNORECASE)
#         if m:
#             gross_domestic = int(m.group(2).replace(",", ""))
#         else:
#             m = re.search(r"Domestic\s*\$([\d,]+)", page_text, re.IGNORECASE)
#             if m:
#                 gross_domestic = int(m.group(1).replace(",", ""))

#         m = re.search(r"INTERNATIONAL\s*\(\d+(\.\d+)?%\)\s*\$([\d,]+)", page_text, re.IGNORECASE)
#         if m:
#             gross_international = int(m.group(2).replace(",", ""))
#         else:
#             m = re.search(r"International\s*\$([\d,]+)", page_text, re.IGNORECASE)
#             if m:
#                 gross_international = int(m.group(1).replace(",", ""))

#         m = re.search(r"WORLDWIDE\s*\$([\d,]+)", page_text, re.IGNORECASE)
#         if m:
#             gross_worldwide = int(m.group(1).replace(",", ""))
#         else:
#             m = re.search(r"Worldwide\s*\$([\d,]+)", page_text, re.IGNORECASE)
#             if m:
#                 gross_worldwide = int(m.group(1).replace(",", ""))

#         # Domestic Opening (your "opening_weekend" proxy)
#         opening_weekend = None
#         dom_open = info.get("Domestic Opening") or info.get("Opening") or info.get("Opening Weekend")
#         if dom_open:
#             opening_weekend = money_to_int(dom_open)
#         else:
#             m = re.search(r"Domestic\s*Opening\s*\$([\d,]+)", page_text, re.IGNORECASE)
#             if m:
#                 opening_weekend = int(m.group(1).replace(",", ""))

#         # Release date
#         release_date = info.get("Earliest Release Date") or info.get("Release Date")

#         # Genres + runtime
#         genres = info.get("Genres")
#         runtime_minutes = parse_runtime_minutes(info.get("Running Time") or info.get("Runtime"))

#         # Budget typically not on BOM title summary
#         budget = None

#         # Prepare base item; then optionally enrich with credits (cast + filmmakers)
#         item = SalesItem(
#             input_title=input_title,
#             input_year=input_year,
#             bom_title=bom_title,
#             budget=budget,
#             opening_weekend=opening_weekend,
#             gross_worldwide=gross_worldwide,
#             gross_domestic=gross_domestic,
#             gross_international=gross_international,
#             release_date=release_date,
#             genres=genres,
#             runtime_minutes=runtime_minutes,
#             filmmakers=None,
#             cast=None,
#             source_url=response.url,
#         )

#         # ------------------------------------------
#         # C) Follow credits page to get filmmakers + cast
#         # ------------------------------------------
#         # Your sample URL already has /title/tt.../?ref_=...
#         # Credits is: /title/tt.../credits
#         credits_url = response.url.split("?")[0].rstrip("/") + "/credits"

#         yield scrapy.Request(
#             url=credits_url,
#             callback=self.parse_credits,
#             meta={"item": item},
#             dont_filter=True,
#         )

#     # -------------------------
#     # CREDITS PAGE -> FILMMAKERS + CAST
#     # -------------------------
#     def parse_credits(self, response):
#         item = response.meta["item"]

#         # Filmmakers table: header "Filmmakers" with a 2-col table (name, role)
#         filmmakers = []
#         for row in response.css("div.a-section.a-spacing-none"):
#             # This page often renders rows similarly; we take anchor text + role cell text
#             name = row.css("a::text").get()
#             role = row.css("::text").getall()

#             # Heuristic: find "Role" next to the name in that row
#             # Safer approach: parse actual table rows if present
#         # Prefer actual table parsing if present
#         for tr in response.css("table tr"):
#             name = tr.css("td:nth-child(1) a::text").get()
#             role = tr.css("td:nth-child(2)::text").get()
#             if name and role:
#                 filmmakers.append({"name": name.strip(), "role": role.strip()})

#         # Cast table: often has "Cast" header and rows (actor, role)
#         cast = []
#         # Parse any table rows that look like cast (actor link + character)
#         for tr in response.css("table tr"):
#             actor = tr.css("td:nth-child(1) a::text").get()
#             character = tr.css("td:nth-child(2)::text").get()
#             if actor and character:
#                 # If the role column looks like "Director" etc, it’s filmmakers; if it looks like character names, it’s cast
#                 # We'll store everything in cast too, then you can filter later if needed.
#                 cast.append({"actor": actor.strip(), "role": character.strip()})

#         # De-duplicate simple repeats
#         def dedupe_list_of_dicts(lst, keys):
#             seen = set()
#             out = []
#             for d in lst:
#                 k = tuple(d.get(x) for x in keys)
#                 if k in seen:
#                     continue
#                 seen.add(k)
#                 out.append(d)
#             return out

#         filmmakers = dedupe_list_of_dicts(filmmakers, ("name", "role"))
#         cast = dedupe_list_of_dicts(cast, ("actor", "role"))

#         # Attach to item
#         item["filmmakers"] = filmmakers if filmmakers else None
#         item["cast"] = cast if cast else None

#         yield item

