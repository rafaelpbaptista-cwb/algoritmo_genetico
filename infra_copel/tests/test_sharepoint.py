# -*- coding: utf-8 -*-
"""Tests for sharepoint."""
import pytest

import pandas as pd

from infra_copel import SharepointSiteCopel
from infra_copel.sharepoint.shareplum.folder import _Folder

FOLDER = 'Base de Dados/test_sharepoint'

@pytest.fixture(scope="module")
def obj_sharepoint():
    sp = SharepointSiteCopel('PowerBIInsightsComercializacao')
    return sp

def test_get_folder(obj_sharepoint):
    sp = obj_sharepoint
    folder = sp.get_folder(FOLDER)
    assert isinstance(folder, _Folder)

@pytest.mark.filterwarnings('ignore::urllib3.exceptions.InsecureRequestWarning')
def test_write_df_to_excel(obj_sharepoint):
    sp = obj_sharepoint
    df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    sp.write_df_to_excel(df, FOLDER, 'test_write_df_to_excel.xlsx')

@pytest.mark.filterwarnings('ignore::urllib3.exceptions.InsecureRequestWarning')
def test_read_df_from_excel(obj_sharepoint):
    sp = obj_sharepoint
    df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    df_read = sp.read_df_from_excel(FOLDER, 'test_write_df_to_excel.xlsx')
    assert df.equals(df_read)

@pytest.mark.filterwarnings('ignore::urllib3.exceptions.InsecureRequestWarning')
def test_write_df_to_pickle(obj_sharepoint):
    sp = obj_sharepoint
    df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    sp.write_df_to_pickle(df, FOLDER, 'test_write_df_to_pickle.pkl')

@pytest.mark.filterwarnings('ignore::urllib3.exceptions.InsecureRequestWarning')
def test_read_df_from_pickle(obj_sharepoint):
    sp = obj_sharepoint
    df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    df_read = sp.read_df_from_pickle(FOLDER, 'test_write_df_to_pickle.pkl')
    assert df.equals(df_read)
