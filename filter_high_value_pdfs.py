#!/usr/bin/env python3
import argparse
import json
import re
import subprocess
from collections import Counter
from pathlib import Path


POSITIVE_RULES = [
    ("brief_motion", 7, re.compile(r"\b(OPPOSITION|REPLY|DEMURRER|MOTION TO|MOTION FOR|MEMORANDUM OF POINTS AND AUTHORITIES|TRIAL BRIEF)\b", re.I)),
    ("declaration", 4, re.compile(r"\b(DECLARATION|AFFIDAVIT)\b", re.I)),
    ("answer", 4, re.compile(r"\bANSWER TO COMPLAINT\b", re.I)),
    ("complaint_petition", 3, re.compile(r"\bPETITION\b|\bCOMPLAINT\b", re.I)),
]

NEGATIVE_RULES = [
    ("proof_service", -7, re.compile(r"\bPROOF OF SERVICE\b", re.I)),
    ("summons", -6, re.compile(r"\bSUMMONS\b", re.I)),
    ("case_management", -5, re.compile(r"\b(CASE MANAGEMENT|CMC|STATUS CONFERENCE|CASE CONFERENCE)\b", re.I)),
    ("notice_admin", -4, re.compile(r"\bNOTICE\b.*\b(OFF CALENDAR|SENT BY COURT|CASE MANAGEMENT|HEARING)\b", re.I)),
    ("dismissal", -5, re.compile(r"\b(DISMISSAL|REQUEST FOR DISMISSAL)\b", re.I)),
    ("default_form", -7, re.compile(r"\b(REQUEST FOR ENTRY OF DEFAULT|CLERK'S JUDGMENT|DEFAULT ENTERED|NON-MILITARY STATUS|COST BILL)\b", re.I)),
    ("minute_order", -5, re.compile(r"\b(MINUTE ORDER|OFF CALENDAR)\b", re.I)),
    ("fee_admin", -4, re.compile(r"\b(FEE WAIVER|FEE PAID|JURY FEES|APPLICATION OF WAIVER)\b", re.I)),
    ("collections_form", -5, re.compile(r"\b(EXEMPT COLLECTIONS|JUDICIAL COUNCIL|CIVIL CASE COVER SHEET)\b", re.I)),
    ("cover_sheet", -8, re.compile(r"\b(CASE COVER SHEET|COVERSHEET)\b", re.I)),
    ("summons_on_complaint", -7, re.compile(r"\bSUMMONS ON COMPLAINT\b", re.I)),
]


def extract_text_preview(pdf_path: Path, pages: int, timeout: int):
    try:
        text = subprocess.check_output(
            ["pdftotext", "-f", "1", "-l", str(pages), str(pdf_path), "-"],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=timeout,
        )
    except Exception:
        return None
    return text


def text_metrics(text: str):
    compact = re.sub(r"\s+", "", text)
    words = re.findall(r"[A-Za-z][A-Za-z'-]+", text)
    unique_words = {word.lower() for word in words}
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return {
        "char_count": len(compact),
        "word_count": len(words),
        "unique_word_count": len(unique_words),
        "line_count": len(lines),
        "preview": " ".join(lines[:12])[:500],
    }


def score_document(proceedings: str, metrics: dict):
    score = 0
    reasons = []
    positive_hits = set()
    text_preview = (metrics.get("preview") or "").upper()

    for label, value, pattern in POSITIVE_RULES:
        if pattern.search(proceedings):
            if label == "complaint_petition" and "ANSWER TO COMPLAINT" in proceedings.upper():
                continue
            score += value
            reasons.append(f"+{value} {label}")
            positive_hits.add(label)

    for label, value, pattern in NEGATIVE_RULES:
        if pattern.search(proceedings):
            score += value
            reasons.append(f"{value} {label}")

    if metrics["char_count"] >= 5000:
        score += 6
        reasons.append("+6 rich_text_5000")
    elif metrics["char_count"] >= 3000:
        score += 4
        reasons.append("+4 rich_text_3000")
    elif metrics["char_count"] >= 1800:
        score += 2
        reasons.append("+2 rich_text_1800")
    else:
        score -= 5
        reasons.append("-5 low_text")

    if metrics["unique_word_count"] >= 400:
        score += 3
        reasons.append("+3 diverse_text_400")
    elif metrics["unique_word_count"] >= 250:
        score += 2
        reasons.append("+2 diverse_text_250")
    elif metrics["unique_word_count"] < 120:
        score -= 3
        reasons.append("-3 low_diversity")

    if metrics["word_count"] < 250:
        score -= 4
        reasons.append("-4 short_doc")

    if "ATTORNEY OR PARTY WITHOUT ATTORNEY" in text_preview and "CASE COVER" in text_preview:
        score -= 8
        reasons.append("-8 form_cover_sheet_text")

    if not positive_hits:
        score -= 6
        reasons.append("-6 no_substantive_signal")

    return score, reasons


def iter_case_jsons(data_roots):
    for root in data_roots:
        if not root.exists():
            continue
        yield from root.rglob("register_of_actions.json")


def main():
    parser = argparse.ArgumentParser(
        description="Identify text-heavy, high-value PDFs for generative AI usage."
    )
    parser.add_argument(
        "--data-root",
        action="append",
        type=Path,
        default=[],
        help="Data root(s) to scan. Can be passed multiple times.",
    )
    parser.add_argument("--pages", type=int, default=3, help="Pages to extract for scoring.")
    parser.add_argument("--timeout-seconds", type=int, default=8, help="pdftotext timeout per file.")
    parser.add_argument("--min-score", type=int, default=6, help="Minimum score to keep a PDF.")
    parser.add_argument("--limit", type=int, default=50, help="Maximum kept PDFs to print.")
    parser.add_argument(
        "--max-linked-pdfs",
        type=int,
        default=None,
        help="Stop after examining this many linked PDFs.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional path to write full kept-document metadata as JSON.",
    )
    args = parser.parse_args()

    data_roots = args.data_root or [
        Path("data_2024"),
        Path("data"),
    ]

    kept = []
    category_counts = Counter()
    scanned_docs = 0
    extracted_docs = 0

    for json_path in iter_case_jsons(data_roots):
        try:
            payload = json.loads(json_path.read_text())
        except Exception:
            continue
        metadata = payload.get("metadata", {})
        case_number = metadata.get("case_number", json_path.parent.name)
        filing_date = metadata.get("filing_date", json_path.parent.parent.name)

        for action in payload.get("actions", []):
            doc_filename = action.get("doc_filename")
            if not doc_filename:
                continue
            scanned_docs += 1
            if args.max_linked_pdfs is not None and scanned_docs > args.max_linked_pdfs:
                break
            pdf_path = json_path.parent / doc_filename
            if not pdf_path.exists():
                continue
            text = extract_text_preview(pdf_path, args.pages, args.timeout_seconds)
            if not text:
                continue
            extracted_docs += 1
            metrics = text_metrics(text)
            proceedings = action.get("proceedings") or ""
            score, reasons = score_document(proceedings, metrics)
            if score < args.min_score:
                continue

            entry = {
                "score": score,
                "reasons": reasons,
                "pdf_path": str(pdf_path.resolve()),
                "case_number": case_number,
                "filing_date": filing_date,
                "doc_filename": doc_filename,
                "proceedings": proceedings,
                "char_count": metrics["char_count"],
                "word_count": metrics["word_count"],
                "unique_word_count": metrics["unique_word_count"],
                "preview": metrics["preview"],
            }
            kept.append(entry)

            for label, _, pattern in POSITIVE_RULES:
                if pattern.search(proceedings):
                    category_counts[label] += 1
                    break
            else:
                category_counts["uncategorized"] += 1
        if args.max_linked_pdfs is not None and scanned_docs > args.max_linked_pdfs:
            break

    kept.sort(key=lambda item: (-item["score"], -item["char_count"], item["pdf_path"]))

    print(
        f"Scanned {scanned_docs} linked PDFs, extracted text from {extracted_docs}, "
        f"kept {len(kept)} with score >= {args.min_score}."
    )
    print("Top categories:")
    for name, count in category_counts.most_common():
        print(f"  {name}: {count}")

    for item in kept[: args.limit]:
        print("")
        print(f"SCORE {item['score']}  CASE {item['case_number']}  DATE {item['filing_date']}")
        print(item["pdf_path"])
        print(item["proceedings"][:220])
        print(", ".join(item["reasons"]))
        print(item["preview"][:350])

    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(kept, indent=2) + "\n")
        print("")
        print(f"Wrote {len(kept)} kept documents to {args.output_json}")


if __name__ == "__main__":
    main()
