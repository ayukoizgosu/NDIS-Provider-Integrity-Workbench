from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

try:
    from src.matching_utils import clean_abn, clean_acn, normalize_text
except ImportError:
    from matching_utils import clean_abn, clean_acn, normalize_text

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "raw" / "abn"

JSON_MATCHING_NAMES_URL = "https://abr.business.gov.au/json/MatchingNames.aspx"
JSON_ABN_DETAILS_URL = "https://abr.business.gov.au/json/AbnDetails.aspx"
HTML_SEARCH_URL = "https://abr.business.gov.au/Search/ResultsActive"
HTML_ABN_VIEW_URL = "https://abr.business.gov.au/ABN/View"


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def save_raw_payload(prefix: str, query: str, suffix: str, payload: str) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    safe_query = re.sub(r"[^a-zA-Z0-9]+", "_", query).strip("_")[:50] or "query"
    path = RAW_DIR / f"{prefix}_{safe_query}_{utc_stamp()}.{suffix}"
    path.write_text(payload, encoding="utf-8")
    return path


def unwrap_jsonp(payload: str) -> dict[str, Any]:
    text = payload.strip()
    if text.startswith("callback(") and text.endswith(")"):
        text = text[len("callback(") : -1]
    return json.loads(text)


class ABNLookupClient:
    def __init__(self, timeout: int = 30) -> None:
        self.guid = os.getenv("ABN_LOOKUP_GUID", "").strip()
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": os.getenv(
                    "NDIS_USER_AGENT",
                    "Antigravity-NDIS-Enforcement-Intelligence/0.1 (+public-data MVP)",
                )
            }
        )

    def search_name(self, name: str, max_results: int = 10) -> dict[str, Any]:
        if self.guid:
            try:
                return self._search_name_via_guid(name=name, max_results=max_results)
            except Exception as exc:  # pragma: no cover - live API behavior varies
                logging.warning("GUID-backed ABN search failed, falling back to HTML: %s", exc)
        return self._search_name_via_html(name=name, max_results=max_results)

    def get_abn_details(self, abn: str) -> dict[str, Any]:
        abn_digits = clean_abn(abn)
        if self.guid:
            try:
                return self._get_abn_details_via_guid(abn_digits)
            except Exception as exc:  # pragma: no cover - live API behavior varies
                logging.warning(
                    "GUID-backed ABN details failed, falling back to HTML: %s",
                    exc,
                )
        return self._get_abn_details_via_html(abn_digits)

    def _search_name_via_guid(self, name: str, max_results: int) -> dict[str, Any]:
        response = self.session.get(
            JSON_MATCHING_NAMES_URL,
            params={
                "name": name,
                "maxResults": max_results,
                "callback": "callback",
                "guid": self.guid,
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        raw_path = save_raw_payload("matching_names", name, "json", response.text)
        payload = unwrap_jsonp(response.text)
        names = payload.get("Names", [])

        results = []
        for item in names:
            results.append(
                {
                    "abn": clean_abn(item.get("Abn") or item.get("ABN") or item.get("abn")),
                    "name": normalize_text(
                        item.get("Name")
                        or item.get("EntityName")
                        or item.get("OrganisationName")
                    ),
                    "entity_type": normalize_text(
                        item.get("EntityTypeName") or item.get("EntityType")
                    ),
                    "state": normalize_text(item.get("AddressState") or item.get("State")),
                    "postcode": normalize_text(
                        item.get("AddressPostcode") or item.get("Postcode")
                    ),
                    "raw": item,
                }
            )

        return {
            "mode": "guid_json",
            "query": name,
            "raw_path": str(raw_path),
            "results": results,
            "message": normalize_text(payload.get("Message")),
        }

    def _search_name_via_html(self, name: str, max_results: int) -> dict[str, Any]:
        response = self.session.get(
            HTML_SEARCH_URL,
            params={"SearchText": name},
            timeout=self.timeout,
        )
        response.raise_for_status()
        raw_path = save_raw_payload("matching_names", name, "html", response.text)
        soup = BeautifulSoup(response.text, "lxml")

        rows = []
        for row in soup.select("table tr"):
            cells = row.find_all("td")
            if len(cells) != 4:
                continue

            abn_and_status = normalize_text(cells[0].get_text(" ", strip=True))
            match = re.match(r"(?P<abn>[\d ]+)\s+(?P<status>\w+)$", abn_and_status)
            abn_value = clean_abn(match.group("abn")) if match else clean_abn(abn_and_status)
            abn_status = normalize_text(match.group("status")) if match else ""

            location = normalize_text(cells[3].get_text(" ", strip=True))
            location_match = re.match(r"(?P<postcode>\d{4})\s+(?P<state>[A-Z]{2,3})$", location)

            link = cells[0].find("a")
            rows.append(
                {
                    "abn": abn_value,
                    "abn_status": abn_status,
                    "name": normalize_text(cells[1].get_text(" ", strip=True)),
                    "entity_type": normalize_text(cells[2].get_text(" ", strip=True)),
                    "postcode": location_match.group("postcode") if location_match else "",
                    "state": location_match.group("state") if location_match else location,
                    "detail_url": (
                        f"https://abr.business.gov.au{link.get('href')}"
                        if link and link.get("href")
                        else ""
                    ),
                }
            )
            if len(rows) >= max_results:
                break

        return {
            "mode": "html_fallback",
            "query": name,
            "raw_path": str(raw_path),
            "results": rows,
            "message": "",
        }

    def _get_abn_details_via_guid(self, abn: str) -> dict[str, Any]:
        response = self.session.get(
            JSON_ABN_DETAILS_URL,
            params={"abn": abn, "callback": "callback", "guid": self.guid},
            timeout=self.timeout,
        )
        response.raise_for_status()
        raw_path = save_raw_payload("abn_details", abn, "json", response.text)
        payload = unwrap_jsonp(response.text)
        gst_text = normalize_text(payload.get("Gst"))
        return {
            "mode": "guid_json",
            "abn": clean_abn(payload.get("Abn") or abn),
            "entity_name": normalize_text(payload.get("EntityName")),
            "abn_status": normalize_text(payload.get("AbnStatus")),
            "abn_status_effective_from": normalize_text(
                payload.get("AbnStatusEffectiveFrom")
            ),
            "acn": clean_acn(payload.get("Acn")),
            "entity_type_code": normalize_text(payload.get("EntityTypeCode")),
            "entity_type_name": normalize_text(payload.get("EntityTypeName")),
            "address_state": normalize_text(payload.get("AddressState")),
            "address_postcode": normalize_text(payload.get("AddressPostcode")),
            "gst_raw": gst_text,
            "gst_registered": "registered" in gst_text.lower(),
            "message": normalize_text(payload.get("Message")),
            "raw_path": str(raw_path),
            "raw": payload,
        }

    def _get_abn_details_via_html(self, abn: str) -> dict[str, Any]:
        response = self.session.get(
            HTML_ABN_VIEW_URL,
            params={"abn": abn},
            timeout=self.timeout,
        )
        response.raise_for_status()
        raw_path = save_raw_payload("abn_details", abn, "html", response.text)
        soup = BeautifulSoup(response.text, "lxml")

        abn_details: dict[str, str] = {}
        acn_value = ""

        for table in soup.select("table"):
            caption = normalize_text(table.caption.get_text(" ", strip=True)) if table.caption else ""
            if caption.lower().startswith("abn details"):
                for row in table.select("tr"):
                    header = row.find("th")
                    value = row.find("td")
                    if not header or not value:
                        continue
                    key = normalize_text(header.get_text(" ", strip=True)).rstrip(":")
                    abn_details[key] = normalize_text(value.get_text(" ", strip=True))
            elif caption.lower().startswith("asic registration"):
                acn_value = clean_acn(table.get_text(" ", strip=True))

        status_text = abn_details.get("ABN status", "")
        gst_text = abn_details.get("Goods & Services Tax (GST)", "")
        location_text = abn_details.get("Main business location", "")
        location_match = re.match(
            r"(?P<state>[A-Z]{2,3})\s+(?P<postcode>\d{4})$",
            location_text,
        )

        return {
            "mode": "html_fallback",
            "abn": clean_abn(abn),
            "entity_name": abn_details.get("Entity name", ""),
            "abn_status": status_text.split(" from ")[0] if status_text else "",
            "abn_status_effective_from": (
                status_text.split(" from ", 1)[1] if " from " in status_text else ""
            ),
            "acn": clean_acn(acn_value),
            "entity_type_code": "",
            "entity_type_name": abn_details.get("Entity type", ""),
            "address_state": location_match.group("state") if location_match else "",
            "address_postcode": location_match.group("postcode") if location_match else "",
            "gst_raw": gst_text,
            "gst_registered": "registered" in gst_text.lower(),
            "message": "",
            "raw_path": str(raw_path),
            "raw": abn_details,
        }
