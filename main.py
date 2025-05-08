import os
import requests
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import openreview

from models import Base, Paper, Review, PaperReviewMapping

DB_PATH = 'sqlite:///data/nlpeer.db'
VENUE_ID = "EMNLP.cc/2023/Conference"
PDF_DIR = "data/pdfs"

os.makedirs(PDF_DIR, exist_ok=True)

# DB setup
engine = create_engine(DB_PATH, echo=False)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

client = openreview.api.OpenReviewClient(baseurl="https://api2.openreview.net")


def download_pdf(pdf_path, paper_id):
    if not pdf_path:
        return None
    try:
        url = f"https://openreview.net{pdf_path}"
        response = requests.get(url)
        filepath = os.path.join(PDF_DIR, f"{paper_id}.pdf")
        with open(filepath, 'wb') as f:
            f.write(response.content)
        return filepath
    except Exception as e:
        print(f"[ERROR] Failed to download PDF for {paper_id}: {e}")
        return None


def fetch_papers():
    print("Fetching papers...")
    submissions = client.get_all_notes(invitation=f"{VENUE_ID}/-/Submission")
    for note in submissions:
        paper_id = note.id
        paper = Paper(
            paper_id=paper_id,
            title=note.content.get("title", {}).get("value", ""),
            abstract=note.content.get("abstract", {}).get("value", ""),
            authors=", ".join(note.content.get("authors", {}).get("value", [])),
            venue=VENUE_ID,
            year=2023,
            submission_text=download_pdf(note.content.get("pdf", {}).get("value", ""), paper_id),
            acceptance_status=None,
            license="CC-BY"
        )
        session.merge(paper)
    session.commit()
    print("Papers inserted.")


def fetch_reviews():
    print("Fetching reviews...")
    reviews = client.get_all_notes(invitation=f"{VENUE_ID}/-/Official_Review")
    for rev in reviews:
        review_id = rev.id
        review = Review(
            review_id=review_id,
            paper_id=rev.forum,
            reviewer_id=rev.signatures[0],
            review_text=rev.content.get("review", {}).get("value", ""),
            review_date=datetime.fromtimestamp(rev.tcdate / 1000).date(),
            overall_score=rev.content.get("overall assessment", {}).get("value", ""),
            confidence_score=rev.content.get("confidence", {}).get("value", ""),
            review_structure="structured" if len(rev.content) > 5 else "unstructured"
        )
        session.merge(review)

        mapping = PaperReviewMapping(
            paper_id=rev.forum,
            review_id=review_id,
            reviewer_role="reviewer",
            review_round=1
        )
        session.merge(mapping)
    session.commit()
    print("Reviews inserted.")


def fetch_decisions():
    print("Fetching decisions...")
    decisions = client.get_all_notes(invitation=f"{VENUE_ID}/-/Decision")
    for d in decisions:
        decision = d.content.get("decision", {}).get("value", "")
        paper = session.get(Paper, d.forum)
        if paper:
            paper.acceptance_status = decision
    session.commit()
    print("Decisions updated.")


if __name__ == "__main__":
    fetch_papers()
    fetch_reviews()
    fetch_decisions()
    print("✅ Done.")
