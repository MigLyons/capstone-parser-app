import unittest
import unittest
from unittest.mock import patch, MagicMock
from function_app import _get_file_type, _parse_profile, _read_pdf_with_metadata, _extract_contact_information, _experience_section_helper, _experienceHeaderHelper

class TestParser(unittest.TestCase):

    def test_get_file_type_pptx_success(self):
        # Test with a .pptx file to ensure it returns success
        file_path = "test_files/test.pptx"
        result = _get_file_type(file_path)
        self.assertEqual(result, 'Powerpoint')

    def test_get_file_type_altFileType_failure(self):
        # Test with an unsupported file type to ensure it returns failure
        file_path = "test_files/test.txt"
        result = _get_file_type(file_path)
        self.assertEqual(result, 'unsupported')

    @patch('function_app.pymupdf.open')
    def test_read_pdf_with_metadata_success(self, mock_open):
        # Mock PyMuPDF's open function to simulate reading a PDF file
        mock_doc = MagicMock()
        mock_page = MagicMock()
        mock_page.get_text.return_value = {
            "blocks": [
                {"lines": [{"spans": [{"text": "Executive Summary"}]}]},
                {"lines": [{"spans": [{"text": "This is a test summary."}]}]}
            ]
        }
        mock_doc.__iter__.return_value = [mock_page]
        mock_open.return_value = mock_doc

        file_path = "test_assets/test.pdf"
        result = _read_pdf_with_metadata(file_path)
        self.assertIsNotNone(result)
        self.assertTrue(len(result) > 0)
        self.assertEqual(result[0]["section"], "Executive Summary")
        self.assertEqual(result[0]["text"], "This is a test summary.")

    @patch('function_app.pymupdf.open')
    def test_read_pdf_with_metadata_no_sections(self, mock_open):
        # Test when no sections are found in the PDF
        mock_doc = MagicMock()
        mock_page = MagicMock()
        mock_page.get_text.return_value = {
            "blocks": [
                {"lines": [{"spans": [{"text": "Random Text"}]}]}
            ]
        }
        mock_doc.__iter__.return_value = [mock_page]
        mock_open.return_value = mock_doc

        file_path = "test_assets/test_no_sections.pdf"
        result = _read_pdf_with_metadata(file_path)
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0]["section"])
        self.assertEqual(result[0]["text"], "Random Text")

    @patch('function_app.pymupdf.open')
    def test_read_pdf_with_metadata_empty_file(self, mock_open):
        # Test reading an empty PDF file to ensure it handles empty content gracefully
        mock_doc = MagicMock()
        mock_doc.__iter__.return_value = []
        mock_open.return_value = mock_doc

        file_path = "test_assets/empty.pdf"
        result = _read_pdf_with_metadata(file_path)
        self.assertEqual(result, [])

    @patch('function_app.pymupdf.open')
    def test_read_pdf_with_metadata_failure(self, mock_open):
        # Test reading a non-existent PDF file to ensure it handles errors gracefully
        mock_open.side_effect = Exception("File not found")
        file_path = "test_assets/non_existent_file.pdf"
        result = _read_pdf_with_metadata(file_path)
        self.assertIsNone(result)

    def test_extract_contact_information_success(self):
        content = [
            {'section': None, 'text': 'M. Gallegos Lyons - "Software Engineer"'},
            {'section': 'Executive Summary', 'text': 'This is an executive summary.'},
            {'section': 'Experience', 'text': 'Email madeleinel@cognizant.com'}
        ]
        result = _extract_contact_information(content)
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "M. Gallegos Lyons")
        self.assertEqual(result["email"], "madeleinel@cognizant.com")
        self.assertEqual(result["job_title"], "Software Engineer")

    def test_extract_contact_information_failure(self):
        # Test with empty content to ensure it handles no data gracefully
        content = []
        result = _extract_contact_information(content)
        self.assertIsNone(result)

    def test_parse_profile_success(self):
        content = [
            {'section': None, 'text': 'M. Gallegos Lyons - "Software Engineer"'},
            {'section': 'Executive Summary', 'text': 'This is an executive summary.'},
            {'section': 'Experience', 'text': 'Marketing - Copy writer - Technology'}
        ]
        url = "https://example.com"
        result = _extract_contact_information(content)
        result_keys = list(result.keys())
        self.assertIsNotNone(result)
        self.assertEqual(len(result_keys), 3)        

    def test_get_file_type_pptx_success(self):
        # Test with a .pdf file to ensure it returns success
        file_path = "test_files/test.pptx"
        result = _get_file_type(file_path)
        self.assertEqual(result, 'Powerpoint')

    def test_get_file_type_docx_success(self):
        # Test with a .docx file to ensure it returns success
        file_path = "test_files/test.docx"
        result = _get_file_type(file_path)
        self.assertEqual(result, 'unsupported')

    def test_read_pdf_with_metadata_multiple_sections(self):
        # Test reading a PDF with multiple sections
        content = [
            {"section": "Executive Summary", "text": "This is a summary."},
            {"section": "Experience", "text": "Worked as a developer."}
        ]
        mock_doc = MagicMock()
        mock_page = MagicMock()
        mock_page.get_text.return_value = {
            "blocks": [
                {"lines": [{"spans": [{"text": "Executive Summary"}]}]},
                {"lines": [{"spans": [{"text": "This is a summary."}]}]},
                {"lines": [{"spans": [{"text": "Experience"}]}]},
                {"lines": [{"spans": [{"text": "Worked as a developer."}]}]}
            ]
        }
        mock_doc.__iter__.return_value = [mock_page]
        with patch('function_app.pymupdf.open', return_value=mock_doc):
            file_path = "test_assets/test_multiple_sections.pdf"
            result = _read_pdf_with_metadata(file_path)
            self.assertIsNotNone(result)
            self.assertEqual(len(result), 2)
            self.assertEqual(result[0]["section"], "Executive Summary")
            self.assertEqual(result[1]["section"], "Experience")

    def test_extract_contact_information_partial_data(self):
        # Test extracting contact information with partial data
        content = [
            {'section': None, 'text': 'M. Gallegos Lyons'},
            {'section': 'Experience', 'text': 'Email madeleinel@cognizant.com'}
        ]
        result = _extract_contact_information(content)
        self.assertIsNone(result)

    def test_experience_section_helper_valid_content(self):
        # Test experience section helper with valid content
        content = "Project Alpha - Lead Developer - Technology"
        result = _experienceHeaderHelper(content)
        self.assertIsNotNone(result)
        self.assertEqual(result["project_title"], "Project Alpha")
        self.assertEqual(result["project_position"], "Lead Developer")
        self.assertEqual(result["project_industry"], "Technology")

    def test_extract_contact_information_failure(self):
        #Test with empty content to ensure it handles no data gracefully
        content = None
        result = _extract_contact_information(content)
        self.assertIsNone(result)

    def test_parse_profile_success(self):
        url = "https://example.com"
        content = [{'section': None, 'text': 'M. Gallegos Lyons - "Software Engineer"'},
                    {'section': 'Executive Summary', 'text': 'This is an executive summary. M Gallegos Lyons engineers software,'},
                    {'section': 'Executive Summary', 'text': 'develops solutions, units the tests, and qualities the assurance.'},
                    {'section': 'Experience', 'text': 'Marketing - Copy writer - Technology'},
                    {'section': 'Experience', 'text': 'wrote copy for social media messages'},
                    {'section': 'Experience', 'text': 'promoted webinars and whitepapers'},
                    {'section': 'Experience', 'text': 'Email madeleinel@cognizant.com'},
                    {'section': 'Experience', 'text': 'Montana (Mountain Time)'},
                    {'section': 'Functional Expertise', 'text': 'Minor car repair'},
                    {'section': 'Technical Expertise', 'text': 'Salesforce Lightning'},
                    {'section': 'Certifications', 'text': 'National Latin Exam silver medal'},
                    {'section': 'Industry Sectors', 'text': 'Academia'},
                    {'section': 'Methodologies', 'text': 'Agile'},
                    {'section': 'Languages Spoken', 'text': 'English'},
                    {'section': 'Mobility', 'text': 'No'}]
        result = _parse_profile(content, url)
        result_keys = list(result.keys())
        self.assertIsNotNone(result)
        self.assertTrue(len(result) > 0)
        self.assertIn('sharePointRef', result_keys)
        
    def test_experience_header_helper(self):
        #Test that the experience header is correctly identified in the content
        content = "Apex Refactoring - Developer - Business"
        result = _experienceHeaderHelper(content)
        self.assertIsNotNone(result)

