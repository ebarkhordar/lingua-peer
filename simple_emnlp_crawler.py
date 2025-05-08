#!/usr/bin/env python
"""
crawl_emnlp2023.py – improved OpenReview crawler for EMNLP 2023 main-track papers
Requires: openreview-py, python-dotenv, SQLAlchemy ≥2.0
"""

import os
import logging
from datetime import datetime
from uuid import uuid4
from dotenv import load_dotenv
import openreview
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, Paper, Review, PaperReviewMapping   # <- your models

###############################################################################
# 0. Config
###############################################################################

VENUE_ID = "EMNLP/2023/Conference"
SUB_INV = f"{VENUE_ID}/-/Submission"
REV_SUFFIXES = ["/-/Official_Review", "/-/Paper_Review", "/-/Commitment_Review", "/-/Public_Comment"]
PDF_DIR    = "data/pdfs"
DB_PATH    = "sqlite:///data/nlpeer.db"

os.makedirs(PDF_DIR, exist_ok=True)
load_dotenv()
USERNAME, PASSWORD = os.getenv("OPENREVIEW_USERNAME"), os.getenv("OPENREVIEW_PASSWORD")

# Set up logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("openreview_fetch.log", mode="a"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

###############################################################################
# 1. DB + OpenReview client
###############################################################################

engine  = create_engine(DB_PATH, echo=False, future=True)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine, future=True)

client = openreview.api.OpenReviewClient(
    baseurl="https://api2.openreview.net",
    username=USERNAME,
    password=PASSWORD,
)

###############################################################################
# 2. Helpers
###############################################################################

def download_pdf(note, dest_dir=PDF_DIR) -> str:
    """Return local path of saved PDF or None on failure."""
    pdf_link = note.content.get("pdf")
    if not pdf_link:
        logger.warning(f"No PDF attachment found for paper ID: {note.id}")
        return None
    try:
        pdf_bytes = client.get_attachment(id=note.id, field_name="pdf")
        path = os.path.join(dest_dir, f"{note.number}.pdf")
        with open(path, "wb") as fh:
            fh.write(pdf_bytes)
        return path
    except Exception as e:
        logger.error(f"Failed to download PDF for paper ID {note.id}: {e}")
        return None

def get_reviews(forum_id: str) -> list[openreview.api.Note]:
    """Return all official reviews attached to a forum."""
    try:
        # Retrieve all notes that are replies to the forum
        replies = client.get_all_notes(forum=forum_id)
        # Filter for notes that are reviews based on their invitation
        reviews = [note for note in replies if 'Official_Review' in note.invitation]
        return reviews
    except Exception as e:
        logger.error(f"Failed to fetch reviews for forum ID {forum_id}: {e}")
        return []


###############################################################################
# 3. Main crawl
###############################################################################

def crawl():
    sess = Session()
    papers_added = reviews_added = 0

    # --- fetch all submissions in one call
    submissions = client.get_all_notes(invitation=SUB_INV)
    logger.info(f"🔍 Found {len(submissions)} candidate submissions")

    for sub in submissions:
        forum_id = sub.forum
        reviews  = get_reviews(forum_id)

        # skip papers with no reviews (rare for main track, but per spec)
        if not reviews:
            logger.info(f"⏩ Skipping paper without reviews: {forum_id}")
            continue

        # skip if paper already in DB
        if sess.get(Paper, forum_id):
            logger.info(f"⚠️ Skipping already existing paper: {forum_id}")
            continue

        # -------------------- Paper --------------------
        title = sub.content.get("title", {}).get("value", "")
        abstract = sub.content.get("abstract", {}).get("value", "")
        authors = ", ".join(sub.content.get("authors", {}).get("value", []))
        pdf = download_pdf(sub)

        paper_row = Paper(
            paper_id        = forum_id,
            title           = title,
            abstract        = abstract,
            authors         = authors,
            venue           = VENUE_ID,
            year            = 2023,
            submission_text = pdf,
            acceptance_status = None,   # decision notes aren’t public for 2023
            license         = "CC-BY",
        )
        sess.add(paper_row)
        papers_added += 1
        logger.info(f"📄 Added paper: {title} (ID: {forum_id})")

        # -------------------- Reviews --------------------
        for rev in reviews:
            review_row = Review(
                review_id        = rev.id,
                paper_id         = forum_id,
                reviewer_id      = rev.signatures[0] if rev.signatures else None,
                review_text      = rev.content.get("review", {}).get("value", ""),
                review_date      = datetime.fromtimestamp(rev.tcdate/1000).date(),
                overall_score    = rev.content.get("overall assessment", {}).get("value", ""),
                confidence_score = rev.content.get("confidence", {}).get("value", ""),
                review_structure = "structured" if len(rev.content) > 5 else "unstructured",
            )
            mapping_row = PaperReviewMapping(
                paper_id     = forum_id,
                review_id    = rev.id,
                reviewer_role= "reviewer",
                review_round = 1,
            )
            sess.add_all([review_row, mapping_row])
            reviews_added += 1
            logger.info(f"📝 Added review for paper ID: {forum_id} (Review ID: {rev.id})")

        sess.commit()

    sess.close()
    logger.info(f"✅ Done – inserted {papers_added} papers and {reviews_added} reviews")

###############################################################################
# 4. Entrypoint
###############################################################################

if __name__ == "__main__":
    run_id = uuid4()
    logger.info(f"🚀 EMNLP 2023 crawl – run {run_id}")
    crawl()
