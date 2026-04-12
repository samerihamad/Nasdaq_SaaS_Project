import sqlite3
import os

BEHAVIORAL_DB_PATH = os.path.join(os.path.dirname(__file__), "behavioral_memory.db")

def init_behavioral_db():
    conn = sqlite3.connect(BEHAVIORAL_DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS negative_habits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            direction TEXT,
            regime TEXT,
            adx_band TEXT,
            time_of_day TEXT,
            sector_sentiment TEXT,
            loss_count INTEGER DEFAULT 1,
            UNIQUE(symbol, direction, regime, adx_band, time_of_day, sector_sentiment)
        )
    ''')
    conn.commit()
    conn.close()

def record_negative_habit(symbol, direction, regime, adx_band, time_of_day, sector_sentiment):
    conn = sqlite3.connect(BEHAVIORAL_DB_PATH)
    c = conn.cursor()
    
    # Safely handle missing/null values
    symbol = symbol or "UNKNOWN"
    direction = direction or "UNKNOWN"
    regime = regime or "UNKNOWN"
    adx_band = adx_band or "UNKNOWN"
    time_of_day = time_of_day or "UNKNOWN"
    sector_sentiment = sector_sentiment or "UNKNOWN"

    c.execute('''
        INSERT INTO negative_habits (symbol, direction, regime, adx_band, time_of_day, sector_sentiment, loss_count)
        VALUES (?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(symbol, direction, regime, adx_band, time_of_day, sector_sentiment)
        DO UPDATE SET loss_count = loss_count + 1
    ''', (symbol, direction, regime, adx_band, time_of_day, sector_sentiment))
    conn.commit()
    conn.close()


def is_toxic_habit(symbol: str, regime: str, adx_band: str, time_of_day: str) -> bool:
    """
    Check if a behavioral fingerprint exists with loss_count >= 1.
    """
    conn = sqlite3.connect(BEHAVIORAL_DB_PATH)
    c = conn.cursor()
    c.execute('''
        SELECT SUM(loss_count) FROM negative_habits 
        WHERE symbol=? AND regime=? AND adx_band=? AND time_of_day=?
    ''', (symbol, regime, adx_band, time_of_day))
    row = c.fetchone()
    conn.close()
    
    if row and row[0] and int(row[0]) >= 1:
        return True
    return False