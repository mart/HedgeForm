import strategy.cusip as cusip


def test_parse_fail_filing_three():
    with open('./data/cusip1.txt', encoding="ASCII", errors="ignore") as f:
        file_map = cusip.parse_fail_filing(f)
        assert file_map == {'D1668R123': 'DDAIF', 'D18190822': 'ABC', 'G01767110': 'DEF'}


def test_parse_fail_filing_another_file():
    with open('./data/cusip2.txt', encoding="ASCII", errors="ignore") as f:
        file_map = cusip.parse_fail_filing(f)
        assert file_map == {'D1668R123': 'DDAIF', 'D18190898': 'ABC', 'G01767105': 'ALKS'}


def test_create_cusip_map():
    cusip_map = cusip.create_cusip_map('./data')
    full_map = {'D1668R123': 'DDAIF', 'D18190822': 'ABC', 'G01767110': 'DEF',
                'D18190898': 'ABC', 'G01767105': 'ALKS'}
    assert cusip_map == full_map
