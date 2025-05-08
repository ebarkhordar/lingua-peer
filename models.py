from sqlalchemy import Column, String, Integer, Text, ForeignKey, Date
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Paper(Base):
    __tablename__ = 'papers'

    paper_id = Column(String, primary_key=True)
    title = Column(Text)
    abstract = Column(Text)
    authors = Column(Text)
    venue = Column(String)
    year = Column(Integer)
    submission_text = Column(Text)
    acceptance_status = Column(String)
    license = Column(String)

    reviews = relationship("Review", back_populates="paper")
    mappings = relationship("PaperReviewMapping", back_populates="paper")


class Review(Base):
    __tablename__ = 'reviews'

    review_id = Column(String, primary_key=True)
    paper_id = Column(String, ForeignKey('papers.paper_id'))
    reviewer_id = Column(String)
    review_text = Column(Text)
    review_date = Column(Date)
    overall_score = Column(String)
    confidence_score = Column(String)
    review_structure = Column(String)

    paper = relationship("Paper", back_populates="reviews")
    mapping = relationship("PaperReviewMapping", back_populates="review", uselist=False)


class PaperReviewMapping(Base):
    __tablename__ = 'paper_review_mapping'

    paper_id = Column(String, ForeignKey('papers.paper_id'), primary_key=True)
    review_id = Column(String, ForeignKey('reviews.review_id'), primary_key=True)
    reviewer_role = Column(String)
    review_round = Column(Integer)

    paper = relationship("Paper", back_populates="mappings")
    review = relationship("Review", back_populates="mapping")
