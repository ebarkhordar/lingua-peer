import os
import logging
from datetime import datetime
from uuid import uuid4
from dotenv import load_dotenv
import openreview
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, Paper, Review, PaperReviewMapping

# Configure logging for both file and console
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# File handler
file_handler = logging.FileHandler('openreview_fetch.log', mode='a')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

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
try:
    engine = create_engine(DB_PATH, echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Database initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize database: {str(e)}")
    raise

# OpenReview client
try:
    client = openreview.api.OpenReviewClient(
        baseurl="https://api2.openreview.net",
        username=USERNAME,
        password=PASSWORD
    )
    logger.info("OpenReview client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize OpenReview client: {str(e)}")
    raise


def download_pdf(note_id, paper_number):
    logger.debug(f"Downloading PDF for note_id: {note_id}, paper_number: {paper_number}")
    try:
        pdf_content = client.get_attachment(id=note_id, field_name='pdf')
        filepath = os.path.join(PDF_DIR, f"{paper_number}.pdf")
        with open(filepath, 'wb') as f:
            f.write(pdf_content)
        logger.info(f"Successfully downloaded PDF to {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Failed to download PDF for note_id {note_id}: {str(e)}")
        return None


def fetch_papers():
    logger.info("Starting to fetch papers")
    try:
        submissions = client.get_all_notes(invitation=f"{VENUE_ID}/-/Submission")
        logger.info(f"Retrieved {len(submissions)} submissions")

        for note in submissions:
            try:
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
                logger.debug(f"Processed paper: {title} (ID: {paper_id})")
            except Exception as e:
                logger.error(f"Error processing paper {note.id}: {str(e)}")
                continue

        session.commit()
        logger.info("All papers inserted into database")
    except Exception as e:
        logger.error(f"Failed to fetch papers: {str(e)}")
        session.rollback()


def fetch_reviews():
    logger.info("Starting to fetch reviews")
    try:
        reviews = client.get_all_notes(invitation=f"{VENUE_ID}/-/Official_Review")
        logger.info(f"Retrieved {len(reviews)} reviews")

        for rev in reviews:
            try:
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
                logger.debug(f"Processed review: {review_id} for paper: {paper_id}")
            except Exception as e:
                logger.error(f"Error processing review {rev.id}: {str(e)}")
                continue

        session.commit()
        logger.info("All reviews inserted into database")
    except Exception as e:
        logger.error(f"Failed to fetch reviews: {str(e)}")
        session.rollback()


def fetch_decisions():
    logger.info("Starting to fetch decisions")
    try:
        decisions = client.get_all_notes(invitation=f"{VENUE_ID}/-/Decision")
        logger.info(f"Retrieved {len(decisions)} decisions")

        for d in decisions:
            try:
                decision = d.content.get("decision", {}).get("value", "")
                paper = session.get(Paper, d.forum)
                if paper:
                    paper.acceptance_status = decision
                    logger.debug(f"Updated decision for paper {d.forum}: {decision}")
                else:
                    logger.warning(f"No paper found for decision {d.forum}")
            except Exception as e:
                logger.error(f"Error processing decision {d.id}: {str(e)}")
                continue

        session.commit()
        logger.info("All decisions updated in database")
    except Exception as e:
        logger.error(f"Failed to fetch decisions: {str(e)}")
        session.rollback()


if __name__ == "__main__":
    try:
        execution_id = str(uuid4())
        logger.info(f"Starting execution with ID: {execution_id}")

        fetch_papers()
        fetch_reviews()
        fetch_decisions()

        logger.info(f"Execution {execution_id} completed successfully")
        print("✅ Done.")
    except Exception as e:
        logger.error(f"Execution failed: {str(e)}")
        print(f"❌ Failed: {str(e)}")