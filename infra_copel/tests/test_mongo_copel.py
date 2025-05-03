# -*- coding: utf-8 -*-
"""Tests for mongo_copel."""
import pytest

import pandas as pd

from infra_copel import MongoHistoricoOficial

@pytest.fixture(scope="module")
def historico_oficial():
    db = MongoHistoricoOficial()
    return db

def test_df_ena_mlt(historico_oficial):
    df = historico_oficial.df_ena_mlt
    assert isinstance(df, pd.DataFrame)
