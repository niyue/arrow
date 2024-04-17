import gandiva_pandas as gdv_pd
import gandiva_pandas.llvm_gandiva_visitor

def test_dump_ir():
    df = gdv_pd.create_df(10)
    filtered_df = df.gandiva.query("c1 > 100", dump_ir=True)
    assert len(filtered_df) < 10