from cmdlineutil.tieredconfig import load_tiered_config
import sys
from os.path import abspath, dirname, join, pardir
import pandas as pd
from nose.tools import with_setup
from contextlib import contextmanager
import mock
from fido.testing.testingutil import temp_sys_args


sys.path = [abspath(join(dirname(__file__), pardir, 'mgo'))] + sys.path
from mgoutils.catalog import catalog
from transform import CronGDWTransform

CONFIG_FILE = abspath(join(dirname(__file__), pardir,
                           'tieredconf', 'secrets.properties'))


def load_config_for_eravana_tst(config_file=CONFIG_FILE):
    config = load_tiered_config(config_file)
    return config


@contextmanager
def mock_eravana_config():
    with mock.patch('cmdlineutil.tieredconfig.load_tiered_config') as mock_tieredconf:
        mock_tieredconf.side_effect = load_config_for_eravana_tst
        yield mock_tieredconf


def setup_func():
    config = load_tiered_config(CONFIG_FILE)
    catalog.configure(config)
    engine = catalog.engines['bloodmoondb']
    engine.execute("CREATE SCHEMA IF NOT EXISTS testing;")

def teardown_func():
    engine = catalog.engines['bloodmoondb']
    engine.execute("DROP SCHEMA IF EXISTS testing CASCADE;")

@with_setup(setup_func, teardown_func)
def test_simple_split():
    args = 'transform.py --props "{}" -t test/account_sales_rep -s "2018-03-01" -e "2018-03-02"'.format(CONFIG_FILE)
    args = args.split()
    with temp_sys_args(*args), mock_eravana_config():
        app = CronGDWTransform()
    from nose.tools import set_trace; set_trace()
    print(app.props)
    app.run()
    chase_ract0010 = pd.DataFrame({
        'merchant_order_num': ['1', '2', '3', '4']
        })
    assert chase_ract0010['merchant_order_num'].equals(pd.Series(['1', '2', '3', '4']))
