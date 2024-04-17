import gandiva_pandas as gdv_pd
import gandiva_pandas.llvm_gandiva_visitor
from alive_progress import alive_bar
from functools import wraps
import pytest

def section(title):
    title = title.replace("test_", "").replace("_", " ").title()
    print("\n" + title)

@pytest.fixture
def df():
    records_per_batch = 5_000_000
    return gdv_pd.create_df(records_per_batch)

def iteration(batches=100):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            section(func.__name__)
            with alive_bar(batches) as bar:
                for _ in range(batches):
                    result = func(*args, **kwargs)
                    bar()
            return result
        return wrapper
    return decorator
    

def test_init():
    df = gdv_pd.create_df(10)
    filtered_df = df.gandiva.query("c1 > 100")
    assert len(filtered_df) < 10


@iteration(batches=100)
def test_pandas(df):
    filtered_df = df.query("c1 > 100 and c2 > 200")
    assert len(filtered_df) > 0


@iteration(batches=100)
def test_gandiva(df):
    filtered_df = df.gandiva.query("(c1 > 100) & (c2 > 200)")
    assert len(filtered_df) > 0