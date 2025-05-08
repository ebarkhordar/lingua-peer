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
logger.setLevel(logging.DEBUG)

# File handler
file_handler = logging.FileHandler('openreview_fetch.log', mode='a')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
console_handler.setLevel(logging.DEBUG)
logger.addHandler(console_handler)

# Load environment variables
load_dotenv()
USERNAME = os.getenv("OPENREVIEW_USERNAME")
PASSWORD = os.getenv("OPENREVIEW_PASSWORD")

# Constants
DB_PATH = 'sqlite:///data/nlpeer.db'
VENUE_ID = "EMNLP/2023/Conference"
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

def check_profile():
    logger.info("Checking user profile to verify API authentication")
    try:
        profile = client.get_profile()
        logger.info(f"Successfully retrieved profile: ID={profile.id}, Email={profile.content.get('preferredEmail', 'N/A')}")
        return True
    except Exception as e:
        logger.error(f"Failed to retrieve profile: {str(e)}")
        return False

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
    submissions = []
    paper_count = 0
    try:
        # Try multiple submission invitations
        for invitation in [
            f"{VENUE_ID}/-/Submission",
            f"{VENUE_ID}/-/Blind_Submission",
            f"{VENUE_ID}/-/ARR_Commitment",
            f"{VENUE_ID}/-/Direct_Submission"
        ]:
            try:
                subs = client.get_all_notes(invitation=invitation)
                submissions.extend(subs)
                logger.info(f"Retrieved {len(subs)} submissions using invitation {invitation}")
            except Exception as e:
                logger.warning(f"No submissions for {invitation}: {str(e)}")

        # Try venueid as fallback
        if not submissions:
            logger.warning("No submissions found with invitations. Trying venueid.")
            submissions = client.get_all_notes(content={'venueid': VENUE_ID})
            logger.info(f"Retrieved {len(submissions)} submissions using venueid {VENUE_ID}")

        # Try alternative venueid
        if not submissions:
            alt_venue_id = "aclweb.org/EMNLP/2023/Conference"
            logger.warning(f"No submissions found with {VENUE_ID}. Trying alternative venueid {alt_venue_id}.")
            submissions = client.get_all_notes(content={'venueid': alt_venue_id})
            logger.info(f"Retrieved {len(submissions)} submissions using venueid {alt_venue_id}")

        # Debug: Log invitation details
        try:
            invitation = client.get_invitation(f"{VENUE_ID}/-/Submission")
            logger.debug(f"Submission invitation details: {invitation.id}, due_date={invitation.duedate}")
        except Exception as e:
            logger.warning(f"Could not retrieve Submission invitation: {str(e)}")

        # Deduplicate submissions by ID
        unique_submissions = {note.id: note for note in submissions}.values()
        total_submissions = len(unique_submissions)
        logger.info(f"Total unique submissions: {total_submissions}")

        # Process papers in batches to reduce database locking
        batch_size = 50
        for i, note in enumerate(unique_submissions, 1):
            try:
                paper_id = note.id
                # Skip if paper already exists
                if session.query(Paper).filter_by(paper_id=paper_id).first():
                    logger.debug(f"Skipping paper ID {paper_id}: already in database")
                    paper_count += 1
                    continue

                title = note.content.get("title", {}).get("value", "") or "Unknown"
                abstract = note.content.get("abstract", {}).get("value", "") or ""
                authors = ", ".join(note.content.get("authors", {}).get("value", [])) or "Unknown"
                pdf_path = download_pdf(note.id, note.number) if note.content.get("pdf") else None

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
                paper_count += 1
                logger.info(f"Inserted paper {i}/{total_submissions}: {title} (ID: {paper_id}, PDF: {pdf_path})")

                # Commit batch every batch_size papers
                if i % batch_size == 0:
                    logger.debug(f"Committing batch at paper {i}")
                    session.commit()
            except Exception as e:
                logger.error(f"Error processing paper {note.id}: {str(e)}")
                session.rollback()
                continue

        session.commit()  # Final commit for any remaining papers
        logger.info(f"All papers inserted into database. Total: {paper_count}")
        return paper_count
    except Exception as e:
        logger.error(f"Failed to fetch papers: {str(e)}")
        session.rollback()
        return paper_count

def fetch_reviews():
    logger.info("Starting to fetch reviews")
    reviews = []
    try:
        # Try multiple review invitations
        for invitation in [
            f"{VENUE_ID}/-/Official_Review",
            f"{VENUE_ID}/-/ARR_Review",
            f"{VENUE_ID}/-/Review",
            f"{VENUE_ID}/-/Public_Review",
            f"{VENUE_ID}/-/Paper_Review",
            f"{VENUE_ID}/-/Anonymous_Review"
        ]:
            try:
                revs = client.get_all_notes(invitation=invitation)
                reviews.extend(revs)
                logger.info(f"Retrieved {len(revs)} reviews using invitation {invitation}")
                # Log sample review content for debugging
                if revs:
                    logger.debug(f"Sample review content: {revs[0].content}")
            except Exception as e:
                logger.warning(f"No reviews for {invitation}: {str(e)}")

        # Deduplicate reviews by ID
        unique_reviews = {rev.id: rev for rev in reviews}.values()
        total_reviews = len(unique_reviews)
        logger.info(f"Total unique reviews: {total_reviews}")

        for i, rev in enumerate(unique_reviews, 1):
            try:
                review_id = rev.id
                # Skip if review already exists
                if session.query(Review).filter_by(review_id=review_id).first():
                    logger.debug(f"Skipping review ID {review_id}: already in database")
                    continue

                paper_id = rev.forum
                reviewer_id = rev.signatures[0] if rev.signatures else None
                review_text = rev.content.get("review", {}).get("value", "") or ""
                review_date = datetime.fromtimestamp(rev.tcdate / 1000).date() if rev.tcdate else datetime.now().date()
                overall_score = (
                    rev.content.get("overall assessment", {}).get("value", "") or
                    rev.content.get("recommendation", {}).get("value", "") or
                    rev.content.get("overall_evaluation", {}).get("value", "") or ""
                )
                confidence_score = rev.content.get("confidence", {}).get("value", "") or ""
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
                session.commit()  # Commit per review to ensure data is saved
                logger.info(f"Inserted review {i}/{total_reviews}: {review_id} for paper: {paper_id}")
            except Exception as e:
                logger.error(f"Error processing review {rev.id}: {str(e)}")
                session.rollback()
                continue

        session.commit()
        logger.info("All reviews inserted into database")
        return total_reviews
    except Exception as e:
        logger.error(f"Failed to fetch reviews: {str(e)}")
        session.rollback()
        return 0

def fetch_decisions():
    logger.info("Starting to fetch decisions")
    decisions = []
    try:
        # Try multiple decision invitations
        for invitation in [
            f"{VENUE_ID}/-/Decision",
            f"{VENUE_ID}/-/ARR_Decision",
            f"{VENUE_ID}/-/Acceptance_Decision",
            f"{VENUE_ID}/-/Program_Committee_Decision"
        ]:
            try:
                decs = client.get_all_notes(invitation=invitation)
                decisions.extend(decs)
                logger.info(f"Retrieved {len(decs)} decisions using invitation {invitation}")
                # Log sample decision content for debugging
                if decs:
                    logger.debug(f"Sample decision content: {decs[0].content}")
            except Exception as e:
                logger.warning(f"No decisions for {invitation}: {str(e)}")

        # Deduplicate decisions by ID
        unique_decisions = {d.id: d for d in decisions}.values()
        total_decisions = len(unique_decisions)
        logger.info(f"Total unique decisions: {total_decisions}")

        for i, d in enumerate(unique_decisions, 1):
            try:
                decision = d.content.get("decision", {}).get("value", "") or ""
                paper = session.get(Paper, d.forum)
                if paper:
                    paper.acceptance_status = decision
                    session.commit()
                    logger.info(f"Updated decision {i}/{total_decisions} for paper {d.forum}: {decision}")
                else:
                    logger.warning(f"No paper found for decision {d.forum}")
            except Exception as e:
                logger.error(f"Error processing decision {d.id}: {str(e)}")
                session.rollback()
                continue

        session.commit()
        logger.info("All decisions updated in database")
        return total_decisions
    except Exception as e:
        logger.error(f"Failed to fetch decisions: {str(e)}")
        session.rollback()
        return 0

if __name__ == "__main__":
    try:
        execution_id = str(uuid4())
        logger.info(f"Starting execution with ID: {execution_id}")

        # Check profile to verify API authentication
        if not check_profile():
            logger.error("Aborting execution due to profile retrieval failure")
            print("❌ Failed: Unable to retrieve profile. Check credentials or API connectivity.")
            raise Exception("Profile retrieval failed")

        # Fetch data and check results
        paper_count = fetch_papers()
        review_count = fetch_reviews()
        decision_count = fetch_decisions()

        # Verify database state
        db_paper_count = session.query(Paper).count()
        db_review_count = session.query(Review).count()
        logger.info(f"Database verification: {db_paper_count} papers, {db_review_count} reviews")

        if paper_count == 0:
            logger.warning(
                "No papers retrieved. Possible issues: incorrect VENUE_ID, no public data, or API restrictions. "
                "Try 'aclweb.org/EMNLP/2023/Conference' or contact OpenReview support."
            )
            print(
                "⚠️ Warning: No papers retrieved. Check VENUE_ID, API access, or try manual data download from OpenReview."
            )
        elif review_count == 0 or decision_count == 0:
            logger.warning(
                "Partial data retrieved. Reviews or decisions may not be public. Check OpenReview for availability."
            )
            print("⚠️ Warning: Retrieved papers but missing reviews or decisions. Verify public data availability.")

        logger.info(f"Execution {execution_id} completed successfully")
        print(f"✅ Done. Retrieved {paper_count} papers, {review_count} reviews, {decision_count} decisions.")
    except Exception as e:
        logger.error(f"Execution failed: {str(e)}")
        print(f"❌ Failed: {str(e)}")
    finally:
        session.close()