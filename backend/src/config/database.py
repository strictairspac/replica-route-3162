from sqlalchemy import create_engine,text
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from .settings import DATABASE_URL
class Base(DeclarativeBase): pass
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
Session = sessionmaker(engine, expire_on_commit=False)
def db_session():
    with Session() as db: yield db
def ensure_columns():
    """无在线迁移引擎下的幂等补列：create_all 不会给已存在的表加列，这里显式补齐。"""
    extras={'trials':[('snapshot_retention','INT NOT NULL DEFAULT 8'),('scan_serving','VARCHAR(20) NOT NULL DEFAULT "pinned"')]}
    with engine.begin() as conn:
        for table,columns in extras.items():
            existing={row[0] for row in conn.execute(text('SHOW COLUMNS FROM %s'%table))}
            for name,ddl in columns:
                if name not in existing:
                    conn.execute(text('ALTER TABLE %s ADD COLUMN %s %s'%(table,name,ddl)))
