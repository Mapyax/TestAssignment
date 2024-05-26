import unittest
from unittest.mock import patch, MagicMock
from EGRUL_Parser import Company, insert_data_to_db, process_companies, chunk_generation, parse_json_file, main

class TestCompany(unittest.TestCase):

    def setUp(self):
        self.company_data = {
            "name": "Test Company",
            "full_name": "Test Company Full Name",
            "inn": "1234567890",
            "kpp": "987654321",
            "data": {
                "СвОКВЭД": {
                    "СвОКВЭДОсн": {
                        "КодОКВЭД": "62.01"
                    }
                },
                "СвАдресЮЛ": {
                    "АдресРФ": {
                        "Регион": {
                            "НаимРегион": "МОСКВА",
                            "ТипРегион": "г."
                        },
                        "КодРегион": "77",
                        "Дом": "Д.1",
                        "Город": {
                            "НаимГород": "Москва",
                            "ТипГород": "г."
                        },
                        "Улица": {
                            "НаимУлица": "Тверская",
                            "ТипУлица": "ул."
                        },
                        "Индекс": "123456"
                    }
                }
            }
        }
        self.company = Company(**self.company_data)

    def test_has_okved_62(self):
        self.assertTrue(self.company.has_okved_62())

    def test_is_district(self):
        self.assertTrue(self.company.is_district())

    def test_generate_adres(self):
        expected_address = "123456, г. МОСКВА, г.Москва, ул.Тверская, Д.1"
        self.assertEqual(self.company.generate_adres(), expected_address)

class TestFunctions(unittest.TestCase):

    def setUp(self):
        self.company_data = {
            "name": "Test Company",
            "full_name": "Test Company Full Name",
            "inn": "1234567890",
            "kpp": "987654321",
            "data": {
                "СвОКВЭД": {
                    "СвОКВЭДОсн": {
                        "КодОКВЭД": "62.01"
                    }
                },
                "СвАдресЮЛ": {
                    "АдресРФ": {
                        "Регион": {
                            "НаимРегион": "МОСКВА",
                            "ТипРегион": "г."
                        },
                        "КодРегион": "77",
                        "Дом": "Д.1",
                        "Город": {
                            "НаимГород": "Москва",
                            "ТипГород": "г."
                        },
                        "Улица": {
                            "НаимУлица": "Тверская",
                            "ТипУлица": "ул."
                        },
                        "Индекс": "123456"
                    }
                }
            }
        }

    @patch('EGRUL_Parser.psycopg2.connect')
    def test_insert_data_to_db(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        data = ["name", "full_name", "62.01", "1234567890", "987654321", "address"]
        insert_data_to_db(data)

        mock_cursor.execute.assert_called_once_with(
            "INSERT INTO EGRUL62 (name, full_name, okved, inn, kpp, adres) VALUES (%s, %s, %s, %s, %s, %s)", data)
        mock_conn.commit.assert_called_once()

    @patch('EGRUL_Parser.process_companies')
    @patch('EGRUL_Parser.orjson.loads')
    @patch('EGRUL_Parser.zipfile.ZipFile')
    def test_parse_json_file(self, mock_zipfile, mock_orjson_loads, mock_process_companies):
        mock_zip = MagicMock()
        mock_zipfile.return_value = mock_zip

        mock_file = MagicMock()
        mock_zip.open.return_value = mock_file

        mock_orjson_loads.return_value = [self.company_data]

        parse_json_file(['test.json'])

        mock_process_companies.assert_called_once_with([self.company_data])

    @patch('EGRUL_Parser.zipfile.ZipFile')
    def test_chunk_generation(self, mock_zipfile):
        mock_zip = MagicMock()
        mock_zip.namelist.return_value = [f'file_{i}.json' for i in range(98)]
        mock_zipfile.return_value.__enter__.return_value = mock_zip

        chunks = chunk_generation('test.zip', 20)
        expected_chunks = [
            [f'file_{i}.json' for i in range(20)],
            [f'file_{i}.json' for i in range(20, 40)],
            [f'file_{i}.json' for i in range(40, 60)],
            [f'file_{i}.json' for i in range(60, 80)],
            [f'file_{i}.json' for i in range(80, 98)]
        ]

        self.assertEqual(chunks, expected_chunks)

    @patch('EGRUL_Parser.chunk_generation')
    @patch('EGRUL_Parser.ProcessPoolExecutor')
    def test_main(self, mock_executor, mock_chunk_generation):
        mock_chunk_generation.return_value = [['chunk1'], ['chunk2']]
        mock_executor.return_value.__enter__.return_value.map = MagicMock()

        main()

        mock_chunk_generation.assert_called_once_with('egrul.json.zip', 20)
        mock_executor.return_value.__enter__.return_value.map.assert_called_once()

if __name__ == "__main__":
    unittest.main()