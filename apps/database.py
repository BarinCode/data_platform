from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session, Session

from apps.config import settings


# create a database engine depends on database protocol
engine = create_engine(settings.mysql_url, pool_size=50, max_overflow=50)


SessionLocal = sessionmaker(autocommit=False, autoflush=True, bind=engine)


# TODO: Base class, what for?
Base = declarative_base()


# database session generator
def get_db_session():
    db_session = SessionLocal()

    try:
        yield db_session
    except:
        db_session.rollback()
        raise 
    finally:
        db_session.close()
