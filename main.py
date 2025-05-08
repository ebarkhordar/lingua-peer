import os
from datetime import datetime
from dotenv import load_dotenv
import openreview
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, Paper, Review, PaperReviewMapping

# Load environment variables
load_dotenv()
USERNAME = os.getenv("OPENREVIEW_USERNAME")
PASSWORD = os.getenv("OPENREVIEW_PASSWORD")

# Constants
DB_PATH = 'sqlite:///data/nlpeer.db'
VENUE_ID = "EMNLP.cc/2023/Conference"
PDF_DIR = "data/pdfs"
os.makedirs(PDF_DIR, exist_ok=True)

# Database setup
engine = create_engine(DB_PATH, echo=False)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# OpenReview client
client = openreview.api.OpenReviewClient(
    baseurl="https://api2.openreview.net",
    username=USERNAME,
    password=PASSWORD
)

def download_pdf(note_id, paper_number):
    try:
        pdf_content = client.get_attachment(id=note_id, field_name='pdf')
        filepath = os.path.join(PDF_DIR, f"{paper_number}.pdf")
        with open(filepath, 'wb') as f:
            f.write(pdf_content)
        return filepath
    except Exception as e:
        print(f"[ERROR] Failed to download PDF for {note_id}: {e}")
        return None

def fetch_papers():
    print("Fetching papers...")
    submissions = client.get_all_notes(invitation=f"{VENUE_ID}/-/Submission")
    for note in submissions:
        paper_id = note.id
        title = note.content.get("title", {}).get("value", "")
        abstract = note.content.get("abstract", {}).get("value", "")
        authors = ", ".join(note.content.get("authors", {}).get("value", []))
        pdf_path = download_pdf(note.id, note.number)
        paper = Paper(
            paper_id=paper_id,
            title=title,
            abstract=abstract,
            authors=authors,
            venue=VENUE_ID,
            year=2023,
            submission_text=pdf_path,
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
        paper_id = rev.forum
        reviewer_id = rev.signatures[0] if rev.signatures else None
        review_text = rev.content.get("review", {}).get("value", "")
        review_date = datetime.fromtimestamp(rev.tcdate / 1000).date()
        overall_score = rev.content.get("overall assessment", {}).get("value", "")
        confidence_score = rev.content.get("confidence", {}).get("value", "")
        review_structure = "structured" if len(rev.content) > 5 else "unstructured"
        review = Review(
            review_id=review_id,
            paper_id=paper_id,
            reviewer_id=reviewer_id,
            review_text=review_text,
            review_date=review_date,
            overall_score=overall_score,
            confidence_score=confidence_score,
            review_structure=review_structure
        )
        session.merge(review)
        mapping = PaperReviewMapping(
            paper_id=paper_id,
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
